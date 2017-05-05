package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	cache "github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	amqp_driver "github.com/streadway/amqp"

	inmem_client "github.com/linkit360/go-inmem/rpcclient"
	inmem_service "github.com/linkit360/go-inmem/service"
	"github.com/linkit360/go-utils/amqp"
	queue_config "github.com/linkit360/go-utils/config"
	m "github.com/linkit360/go-utils/metrics"
	rec "github.com/linkit360/go-utils/rec"
)

// Mobilink telco handlers.
// rejected rule configured in cofig (24h usually)

type mobilink struct {
	conf              MobilinkConfig
	m                 *MobilinkMetrics
	periodicSync      *sync.WaitGroup
	prevCache         *cache.Cache
	NewCh             <-chan amqp_driver.Delivery
	NewConsumer       *amqp.Consumer
	MOCh              <-chan amqp_driver.Delivery
	MOConsumer        *amqp.Consumer
	ResponsesCh       <-chan amqp_driver.Delivery
	ResponsesConsumer *amqp.Consumer
}

type MobilinkConfig struct {
	Enabled         bool                            `yaml:"enabled" default:"false"`
	OperatorName    string                          `yaml:"operator_name" default:"mobilink"`
	OperatorCode    int64                           `yaml:"operator_code" default:"41001"`
	RejectedHours   int                             `yaml:"rejected_hours" default:"24"`
	Channel         ChannelNotifyConfig             `yaml:"channel"`
	Content         ContentConfig                   `yaml:"content"`
	Periodic        PeriodicConfig                  `yaml:"periodic"`
	NewSubscription queue_config.ConsumeQueueConfig `yaml:"new"`
	MO              queue_config.ConsumeQueueConfig `yaml:"mo"`
	SMSRequests     string                          `yaml:"sms"`
	Requests        string                          `yaml:"charge_req"`
	Responses       queue_config.ConsumeQueueConfig `yaml:"charge_resp"`
}

type ChannelNotifyConfig struct {
	Enabled bool   `yaml:"enabled"`
	Url     string `yaml:"url"`
}

func initMobilink(mbConfig MobilinkConfig, consumerConfig amqp.ConsumerConfig) *mobilink {
	if !mbConfig.Enabled {
		return nil
	}
	mb := &mobilink{
		conf: mbConfig,
	}
	mb.initActiveSubscriptionsCache()
	mb.initMetrics()
	if mbConfig.Requests == "" {
		log.Fatal("empty queue name requests")
	}
	if mbConfig.SMSRequests == "" {
		log.Fatal("empty queue name sms requests")
	}
	mb.NewConsumer = amqp.InitConsumer(
		consumerConfig,
		mbConfig.NewSubscription,
		mb.NewCh,
		mb.processNewMobilinkSubscription,
	)
	mb.MOConsumer = amqp.InitConsumer(
		consumerConfig,
		mbConfig.MO,
		mb.MOCh,
		mb.processMO,
	)
	mb.ResponsesConsumer = amqp.InitConsumer(
		consumerConfig,
		mbConfig.Responses,
		mb.ResponsesCh,
		mb.processResponses,
	)

	if mb.conf.Content.Enabled {
		go func() {
			for range time.Tick(time.Duration(mb.conf.Content.FetchPeriodSeconds) * time.Second) {
				mb.sendContent()
			}
		}()

	} else {
		log.Info("send content disabled")
	}

	if mb.conf.Periodic.Enabled {
		mb.periodicSync = &sync.WaitGroup{}
		go func() {
			for range time.Tick(time.Duration(mb.conf.Periodic.Period) * time.Second) {
				mb.processPeriodic()
			}
		}()
	} else {
		log.Info("periodic disabled")
	}

	return mb
}

// ============================================================
// periodics: get subscriptions in database and send charge request
func (mb *mobilink) processPeriodic() {
	if !mb.conf.Periodic.Enabled {
		return
	}
	begin := time.Now()
	subscriptions, err := rec.GetPeriodicsOnceADay(mb.conf.Periodic.FetchLimit)
	if err != nil {
		return
	}
	mb.m.GetPeriodicsDuration.Observe(time.Since(begin).Seconds())

	begin = time.Now()
	for _, subscr := range subscriptions {
		mb.periodicSync.Add(1)

		logCtx := log.WithFields(log.Fields{
			"tid":    subscr.Tid,
			"action": "periodic",
		})

		s, err := inmem_client.GetServiceById(subscr.ServiceId)
		if err != nil {
			Errors.Inc()
			err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error":      err.Error(),
				"service_id": subscr.ServiceId,
			}).Error("cannot get service by id")
			continue
		}
		mb.setServiceFields(&subscr, s)

		// send charge req
		if err := publish(mb.conf.Requests, "charge", subscr, 1); err != nil {

			err = fmt.Errorf("publish: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot send charge request")
			continue
		}
		begin := time.Now()
		if err = rec.SetSubscriptionStatus("pending", subscr.SubscriptionId); err != nil {
			return
		}
		SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())

	}
	mb.periodicSync.Wait()
	mb.m.ProcessPeriodicsDuration.Observe(time.Since(begin).Seconds())
}

type EventNotifyNewSubscription struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

// ============================================================
// new subscriptions came from dispatcherd
// one thing - drop / insert in db and send in MO queue
func (mb *mobilink) processNewMobilinkSubscription(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var ns EventNotifyNewSubscription
		var r rec.Record
		var err error
		var s inmem_service.Service

		logCtx := log.WithFields(log.Fields{
			"action": "send content",
		})

		if err := json.Unmarshal(msg.Body, &ns); err != nil {
			mb.m.Dropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
				"q":     mb.conf.NewSubscription.Name,
			}).Error("failed")
			goto ack
		}

		r = ns.EventData
		logCtx = logCtx.WithFields(log.Fields{
			"tid": r.Tid,
		})

		// first checks
		if r.Msisdn == "" || r.CampaignId == 0 {
			mb.m.Dropped.Inc()
			mb.m.Empty.Inc()

			log.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"body":  string(msg.Body),
				"q":     mb.conf.NewSubscription.Name,
			}).Error("failed")
			goto ack
		}

		s, err = inmem_client.GetServiceById(r.ServiceId)
		if err != nil {
			Errors.Inc()
			time.Sleep(1)

			err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error":      err.Error(),
				"service_id": r.ServiceId,
			}).Error("cannot get service by id")
			msg.Nack(false, true)
			continue
		}
		mb.setServiceFields(&r, s)

		// add to database
		if err = rec.AddNewSubscriptionToDB(&r); err != nil {
			Errors.Inc()
			mb.m.AddToDBErrors.Inc()

			msg.Nack(false, true)
			continue
		} else {
			mb.m.AddToDbSuccess.Inc()
		}

		if err := publish(mb.conf.MO.Name, "mo", r); err != nil {
			Errors.Inc()

			log.WithFields(log.Fields{
				"body": string(msg.Body),
			}).Error("failed to publish")
			msg.Nack(false, true)
			continue
		}

		// acknowledge
	ack:
		if err := msg.Ack(false); err != nil {
			Errors.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}

// ============================================================
// mo from rabbitMQ buffer
// checks MO: set/get rejected cache, publish to telco
func (mb *mobilink) processMO(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {

		var ns EventNotifyNewSubscription
		var r rec.Record
		var err error

		logCtx := log.WithFields(log.Fields{
			"action": "send content",
		})

		if err := json.Unmarshal(msg.Body, &ns); err != nil {
			Errors.Inc()
			mb.m.Dropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
				"q":     mb.conf.MO.Name,
			}).Error("failed")
			goto ack
		}

		// setup rec
		r = ns.EventData
		logCtx = logCtx.WithFields(log.Fields{
			"tid": r.Tid,
		})
		// check
		if err = checkMO(&r, mb.isRejectedFn, mb.setActiveSubscriptionCache); err != nil {
			Errors.Inc()
			msg.Nack(false, true)
			continue
		}

		if r.SubscriptionStatus == "" {
			if err := publish(mb.conf.Requests, "charge", r, 1); err != nil {
				Errors.Inc()

				logCtx.WithFields(log.Fields{
					"tid":   r.Tid,
					"error": err.Error(),
				}).Error("send to charge")
				msg.Nack(false, true)
				continue
			}
			if r.SMSText == "" {
				Errors.Inc()
				logCtx.WithFields(log.Fields{}).Info("empty text for sms subscribe")
			} else {
				publish(mb.conf.SMSRequests, "send_sms", r)
			}
		}

	ack:
		if err := msg.Ack(false); err != nil {
			Errors.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}

func (mb *mobilink) setServiceFields(r *rec.Record, s inmem_service.Service) {
	r.Periodic = true
	r.DelayHours = s.DelayHours
	r.PaidHours = s.PaidHours
	r.RetryDays = s.RetryDays
	r.PeriodicDays = s.PeriodicDays
	r.Price = 100 * int(s.Price)
	r.SMSText = s.SMSOnSubscribe // for mo func
}

// ============================================================
// handle responses
func (mb *mobilink) processResponses(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var e EventNotifyTarifficate
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			Errors.Inc()
			mb.m.ResponseDropped.Inc()

			log.WithFields(log.Fields{
				"error":    err.Error(),
				"msg":      "dropped",
				"response": string(msg.Body),
			}).Error("failed")
			goto ack
		}
		if err := mb.handleResponse(e.EventData); err != nil {
			Errors.Inc()
			mb.m.ResponseErrors.Inc()
			msg.Nack(false, true)
			continue
		}
		mb.sendChannelNotify("renewal", e.EventData)

		mb.m.ResponseSuccess.Inc()
	ack:
		if err := msg.Ack(false); err != nil {
			Errors.Inc()
			log.WithFields(log.Fields{
				"tid":   e.EventData.Tid,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}

func (mb *mobilink) handleResponse(r rec.Record) error {
	if err := processResponse(&r, false); err != nil {
		return err
	}

	flagUnsubscribe := false

	s, err := inmem_client.GetServiceById(r.ServiceId)
	if err != nil {
		err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
		return err
	}
	gracePeriod := time.Duration(24*(s.RetryDays-s.GraceDays)) * time.Hour
	allowedSubscriptionPeriod := time.Duration(24*s.RetryDays) * time.Hour
	timePassedSinsceSubscribe := time.Now().UTC().Sub(mb.getSubscriptionStartTime(r))
	var downloadedContentCount int

	count, err := rec.GetCountOfFailedChargesFor(r.Msisdn, r.ServiceId, s.InactiveDays)
	if err != nil {
		return err
	}
	if r.Paid == false {
		count = count + 1
	}
	if count > s.InactiveDays {
		log.WithField("reason", "inactive days").Info("deactivate subscription")
		flagUnsubscribe = true
		goto unsubscribe
	}

	if r.Paid == false && timePassedSinsceSubscribe > gracePeriod {
		log.WithField("reason", "failed charge in grace period").Info("deactivate subscription")
		flagUnsubscribe = true
		goto unsubscribe
	}

	if timePassedSinsceSubscribe > allowedSubscriptionPeriod {
		log.WithField("reason", "passed the subscription period").Info("deactivate subscription")
		flagUnsubscribe = true
		goto unsubscribe
	}

	downloadedContentCount, err = rec.GetCountOfDownloadedContent(r.SubscriptionId)
	if err != nil {
		log.WithField("error", err.Error()).Error("cannot get count of downloaded content")
		downloadedContentCount = s.MinimalTouchTimes
	}
	if downloadedContentCount < s.MinimalTouchTimes && timePassedSinsceSubscribe > gracePeriod {
		log.WithField("reason", "too view content downloaded").Info("deactivate subscription")
		flagUnsubscribe = true
		goto unsubscribe
	}

unsubscribe:
	if flagUnsubscribe {
		r.SubscriptionStatus = "inactive"
		if err := writeSubscriptionStatus(r); err != nil {
			return err
		}
		mb.sendChannelNotify("unsub", r)

		if s.SMSOnUnsubscribe == "" {
			log.WithField("sms_text", "empty").Info("skip user notify")
			return nil
		}
		r.SMSText = s.SMSOnUnsubscribe
		publish(mb.conf.SMSRequests, "send_sms", r)

	}

	// send everything, pixels module will decide to send pixel, or not to send
	if r.Pixel != "" && r.AttemptsCount == 0 && r.SubscriptionStatus != "postpaid" {
		if err := notifyPixel(r); err != nil {
			return err
		}
	}

	return nil
}

func (mb *mobilink) sendChannelNotify(event string, r rec.Record) {
	if !mb.conf.Channel.Enabled {
		return
	}
	if r.Channel == "" {
		return
	}
	if r.AttemptsCount == 0 {
		event = "mo"
	}
	if (event == "mo" || event == "renewal") && r.Paid == false {
		return
	}

	v := url.Values{}
	v.Set("msisdn", r.Msisdn)
	if r.Paid {
		v.Set("status", "1")
	} else {
		v.Set("status", "0")
	}
	eventNum := "0"
	if event == "mo" {
		eventNum = "1"
	} else if event == "renewal" {
		eventNum = "2"
	} else if event == "unsub" {
		eventNum = "3"
	} else {
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"event":  event,
		}).Error("wrong event type")
		return
	}
	v.Set("event", eventNum)
	v.Set("timestamp", time.Now().UTC().Format("20060102 15:04:05"))
	channelUrl := mb.conf.Channel.Url + "?" + v.Encode()
	resp, err := http.DefaultClient.Get(channelUrl)
	if err != nil {
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"event":  event,
			"url":    channelUrl,
			"error":  err.Error(),
		}).Warn("error while channel notify")
	}
	if resp.StatusCode != 200 {
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"event":  event,
			"url":    channelUrl,
			"error":  resp.Status,
		}).Warn("channel notify status code is suspisious")
	} else {
		log.WithFields(log.Fields{
			"tid":  r.Tid,
			"url":  channelUrl,
			"code": resp.StatusCode,
		}).Info("channel notify")
	}
}

func (mb *mobilink) sendContent() error {
	if !mb.conf.Content.Enabled {
		return nil
	}

	subscriptions, err := rec.GetLiveTodayPeriodicsForContent(mb.conf.Content.FetchLimit)
	if err != nil {
		return err
	}
	for _, subscr := range subscriptions {
		logCtx := log.WithFields(log.Fields{
			"tid":    subscr.Tid,
			"action": "send content",
		})

		s, err := inmem_client.GetServiceById(subscr.ServiceId)
		if err != nil {
			Errors.Inc()
			err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error":      err.Error(),
				"service_id": subscr.ServiceId,
			}).Error("cannot get service by id")
			return err
		}
		if s.SMSOnContent == "" {
			logCtx.WithFields(log.Fields{
				"service_id": subscr.ServiceId,
			}).Debug("sms on content is empty, skip content sending")
			return nil
		}

		mb.setServiceFields(&subscr, s)

		contentHash, err := getContentUniqueHash(subscr)
		if err != nil {
			Errors.Inc()
			err = fmt.Errorf("getContentUniqueHash: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("getContentUniqueHash")
			return err
		}

		subscr.SMSText = fmt.Sprintf(s.SMSOnContent, mb.conf.Content.Url+contentHash)
		if err := publish(mb.conf.SMSRequests, "send_sms", subscr); err != nil {

			err = fmt.Errorf("publish: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot enqueue")
			return err
		}
	}
	return nil
}

// ============================================================
// active subscriptions cache
func (mb *mobilink) initActiveSubscriptionsCache() {
	prev, err := rec.LoadActiveSubscriptions()
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load previous subscriptions")
	}
	mb.prevCache = cache.New(time.Duration(4)*time.Hour, time.Minute)
	for _, v := range prev {
		storeDuration := time.Duration(24*v.RetryDays)*time.Hour - time.Now().UTC().Sub(v.CreatedAt) + time.Hour
		if storeDuration > 0 {
			key := v.Msisdn + strconv.FormatInt(v.ServiceId, 10)
			mb.prevCache.Set(key, v.CreatedAt, storeDuration)
		}
	}
}

func (mb *mobilink) getSubscriptionStartTime(r rec.Record) time.Time {
	key := r.Msisdn + strconv.FormatInt(r.ServiceId, 10)
	createdAtI, found := mb.prevCache.Get(key)
	if !found {
		return time.Now().UTC()
	}

	createdAt, ok := createdAtI.(time.Time)
	if !ok {
		log.WithField("error", fmt.Sprintf("Unknown field type: %T", createdAtI)).
			Error("cannot pass time cache for active subscription")
		return time.Now().UTC()
	}
	return createdAt
}

func (mb *mobilink) isRejectedFn(r rec.Record) bool {
	key := r.Msisdn + strconv.FormatInt(r.ServiceId, 10)
	createdAtI, found := mb.prevCache.Get(key)
	if !found {
		return false
	}

	createdAt, ok := createdAtI.(time.Time)
	if !ok {
		log.WithField("error", fmt.Sprintf("Unknown field type: %T", createdAtI)).
			Error("cannot pass time cache for active subscription")
		return false
	}

	if time.Now().Sub(createdAt).Hours() < float64(mb.conf.RejectedHours) {
		return true
	}
	return false
}

func (mb *mobilink) setActiveSubscriptionCache(r rec.Record) {
	key := r.Msisdn + strconv.FormatInt(r.ServiceId, 10)
	_, found := mb.prevCache.Get(key)
	if !found {
		mb.prevCache.Set(key, time.Now().UTC(), time.Duration(24*r.RetryDays)*time.Hour)
		log.WithFields(log.Fields{
			"tid": r.Tid,
			"key": key,
		}).Debug("set previous subscription cache")
	}
}

// ============================================================
// metrics
type MobilinkMetrics struct {
	Dropped                  m.Gauge
	Empty                    m.Gauge
	AddToDBErrors            m.Gauge
	AddToDbSuccess           m.Gauge
	NotifyErrors             m.Gauge
	ResponseDropped          m.Gauge
	ResponseErrors           m.Gauge
	ResponseSuccess          m.Gauge
	SMSResponseSuccess       m.Gauge
	SinceRetryStartProcessed prometheus.Gauge
	GetPeriodicsDuration     prometheus.Summary
	ProcessPeriodicsDuration prometheus.Summary
}

func (mb *mobilink) initMetrics() {
	mbm := &MobilinkMetrics{
		Dropped:                  m.NewGauge(appName, mb.conf.OperatorName, "dropped", "mobilink dropped"),
		Empty:                    m.NewGauge(appName, mb.conf.OperatorName, "empty", "mobilink queue empty"),
		AddToDBErrors:            m.NewGauge(appName, mb.conf.OperatorName, "add_to_db_errors", "subscription add to db errors"),
		AddToDbSuccess:           m.NewGauge(appName, mb.conf.OperatorName, "add_to_db_success", "subscription add to db success"),
		NotifyErrors:             m.NewGauge(appName, mb.conf.OperatorName, "notify_errors", "notify errors"),
		ResponseDropped:          m.NewGauge(appName, mb.conf.OperatorName, "response_dropped", "dropped"),
		ResponseErrors:           m.NewGauge(appName, mb.conf.OperatorName, "response_errors", "errors"),
		ResponseSuccess:          m.NewGauge(appName, mb.conf.OperatorName, "response_success", "success"),
		SMSResponseSuccess:       m.NewGauge(appName, mb.conf.OperatorName, "sms_response_success", "success"),
		SinceRetryStartProcessed: m.PrometheusGauge(appName, mb.conf.OperatorName, "since_last_retries_fetch_seconds", "seconds since last retries processing"),
		GetPeriodicsDuration:     m.NewSummary("get_periodics_duration_seconds", "get periodics duration seconds"),
		ProcessPeriodicsDuration: m.NewSummary("process_periodics_duration_seconds", "process periodics duration seconds"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			mbm.Dropped.Update()
			mbm.Empty.Update()
			mbm.AddToDBErrors.Update()
			mbm.AddToDbSuccess.Update()
			mbm.NotifyErrors.Update()
			mbm.ResponseDropped.Update()
			mbm.ResponseErrors.Update()
			mbm.ResponseSuccess.Update()
			mbm.SMSResponseSuccess.Update()
		}
	}()

	go func() {
		for range time.Tick(time.Second) {
			mbm.SinceRetryStartProcessed.Inc()
		}
	}()
	mb.m = mbm
}
