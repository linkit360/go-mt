package service

import (
	"encoding/json"
	"fmt"
	"strconv"
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
	Content         ContentConfig                   `yaml:"content"`
	NewSubscription queue_config.ConsumeQueueConfig `yaml:"new"`
	MO              queue_config.ConsumeQueueConfig `yaml:"mo"`
	SMSRequests     string                          `yaml:"sms"`
	Requests        string                          `yaml:"charge_req"`
	Responses       queue_config.ConsumeQueueConfig `yaml:"charge_resp"`
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

	if !mb.conf.Content.Enabled {
		log.Info("send content disabled")
		return mb
	}

	go func() {
		for range time.Tick(time.Duration(mb.conf.Content.FetchPeriodSeconds) * time.Second) {
			mb.sendContent()
		}
	}()

	return mb
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
		s, err := inmem_client.GetServiceById(r.ServiceId)
		if err != nil {
			Errors.Inc()
			time.Sleep(1)

			err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
			log.WithFields(log.Fields{
				"error":      err.Error(),
				"service_id": r.ServiceId,
			}).Error("cannot get service by id")
			msg.Nack(false, true)
			continue
		}
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
		mb.setServiceFields(&r, s)

		// check
		if err = checkMO(&r, mb.isRejectedFn, mb.setActiveSubscriptionCache); err != nil {
			Errors.Inc()
			msg.Nack(false, true)
			continue
		}
		if r.SubscriptionStatus == "" {
			if err := publish(mb.conf.Requests, "charge", r, 1); err != nil {
				Errors.Inc()

				log.WithFields(log.Fields{
					"tid":   r.Tid,
					"error": err.Error(),
				}).Error("send to charge")
				msg.Nack(false, true)
				continue
			}
			r.SMSText = s.SMSOnSubscribe
			publish(mb.conf.SMSRequests, "send_sms", r)
		}

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

func (mb *mobilink) setServiceFields(r *rec.Record, s inmem_service.Service) {
	r.DelayHours = s.DelayHours
	r.PaidHours = s.PaidHours
	r.RetryDays = s.RetryDays
	r.Price = 100 * int(s.Price)
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

	downloadedContentCount, err := rec.GetCountOfDownloadedContent(r.SubscriptionId)
	if err != nil {
		log.WithField("error", err.Error()).Error("cannot get count of downloaded content")
		downloadedContentCount = 2
	}
	if downloadedContentCount <= 1 && timePassedSinsceSubscribe > gracePeriod {
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

func (mb *mobilink) sendContent() error {
	if !mb.conf.Content.Enabled {
		return nil
	}
	subscriptions, err := rec.GetLiveTodayPeriodics(mb.conf.Content.FetchLimit)
	if err != nil {
		return err
	}
	for _, subscr := range subscriptions {
		logCtx := log.WithFields(log.Fields{
			"tid": subscr.Tid,
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
		mb.setServiceFields(&subscr, s)

		contentHash, err := getContentUniqueHash(subscr)
		if err != nil {
			Errors.Inc()
			err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error":      err.Error(),
				"service_id": subscr.ServiceId,
			}).Error("cannot get service by id")
			return err
		}
		url := mb.conf.Content.Url + contentHash
		subscr.SMSText = fmt.Sprintf(s.SMSOnContent, url)

		if err := publish(mb.conf.SMSRequests, "send_sms", subscr); err != nil {

			err = fmt.Errorf("publish: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error":      err.Error(),
				"service_id": subscr.ServiceId,
			}).Error("cannot get service by id")
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
	log.WithField("count", len(prev)).Debug("loaded previous subscriptions")
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
