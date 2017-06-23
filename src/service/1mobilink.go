package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	cache "github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	content_client "github.com/linkit360/go-contentd/rpcclient"
	mid_client "github.com/linkit360/go-mid/rpcclient"
	"github.com/linkit360/go-utils/amqp"
	queue_config "github.com/linkit360/go-utils/config"
	m "github.com/linkit360/go-utils/metrics"
	rec "github.com/linkit360/go-utils/rec"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
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
	Channel         ChannelNotifyConfig             `yaml:"channel"`
	Content         ContentConfig                   `yaml:"content"`
	Periodic        PeriodicConfig                  `yaml:"periodic"`
	ReCharge        PeriodicConfig                  `yaml:"recharge"`
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

func initMobilink(
	mbConfig MobilinkConfig,
	consumerConfig amqp.ConsumerConfig,
	contentConf content_client.ClientConfig,
) *mobilink {
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
		content_client.Init(contentConf)
		go func() {
			for range time.Tick(time.Duration(mb.conf.Content.FetchPeriodSeconds) * time.Second) {
				mb.sendContent()
			}
		}()

	} else {
		log.Info("send content disabled")
	}

	if mb.conf.Periodic.Enabled {
		go func() {
			for range time.Tick(time.Duration(mb.conf.Periodic.Period) * time.Second) {
				mb.processPeriodic()
			}
		}()
	} else {
		log.Info("periodic disabled")
	}

	if mb.conf.ReCharge.Enabled {
		go func() {
			for range time.Tick(time.Duration(mb.conf.ReCharge.Period) * time.Second) {
				mb.processReChargePeriodic()
			}
		}()
	} else {
		log.Info("periodic recharge disabled")
	}

	return mb
}

// ============================================================
// periodics: some of them aren't paid. Do try to charge them again in the time.
func (mb *mobilink) processReChargePeriodic() {
	mb.m.SinceReChargePeriodicStartProcess.Set(0)
	if !mb.conf.ReCharge.Enabled {
		return
	}
	begin := time.Now()
	rechargers, err := rec.GetNotPaidPeriodics(mb.conf.ReCharge.FetchLimit)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("cannot get recharge periodics")
		return
	}
	mb.m.GetReChargePeriodicsDuration.Observe(time.Since(begin).Seconds())

	begin = time.Now()
	for _, subscr := range rechargers {

		logCtx := log.WithFields(log.Fields{
			"tid":    subscr.Tid,
			"action": "recharge periodic",
		})

		s, err := mid_client.GetServiceByCode(subscr.ServiceCode)
		if err != nil {
			Errors.Inc()
			err = fmt.Errorf("mid_client.GetServiceByCode: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error":        err.Error(),
				"service_code": subscr.ServiceCode,
			}).Error("cannot get service by id")
			continue
		}
		mb.setServiceFields(&subscr, s)

		// send charge req
		if err := publish(mb.conf.Requests, "charge", subscr, 0); err != nil {

			err = fmt.Errorf("publish: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot send charge request")
			continue
		} else {
			logCtx.WithFields(log.Fields{
				"attempts_count":  subscr.AttemptsCount,
				"days":            subscr.PeriodicDays,
				"price":           subscr.Price,
				"subscription_id": subscr.SubscriptionId,
			}).Info("sent charge request")
		}
		begin := time.Now()
		if err = rec.SetSubscriptionStatus("pending", subscr.SubscriptionId); err != nil {
			SetPendingStatusErrors.Inc()
			return
		}
		SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())
	}

	mb.m.ProcessReChargePeriodicsDuration.Observe(time.Since(begin).Seconds())
}

// ============================================================
// periodics: get subscriptions in database and send charge request
func (mb *mobilink) processPeriodic() {
	mb.m.SincePeriodicStartProcess.Set(0)
	if !mb.conf.Periodic.Enabled {
		return
	}
	begin := time.Now()
	subscriptions, err := rec.GetPeriodicsOnceADay(mb.conf.Periodic.FetchLimit)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("cannot get periodics")
		return
	}
	mb.m.GetPeriodicsDuration.Observe(time.Since(begin).Seconds())

	begin = time.Now()
	for _, subscr := range subscriptions {

		logCtx := log.WithFields(log.Fields{
			"tid":    subscr.Tid,
			"action": "periodic",
		})

		s, err := mid_client.GetServiceByCode(subscr.ServiceCode)
		if err != nil {
			Errors.Inc()
			err = fmt.Errorf("mid_client.GetServiceById: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error":      err.Error(),
				"service_id": subscr.ServiceCode,
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
		} else {
			logCtx.WithFields(log.Fields{
				"days":            subscr.PeriodicDays,
				"price":           subscr.Price,
				"subscription_id": subscr.SubscriptionId,
			}).Info("sent charge request")
		}
		begin := time.Now()
		if err = rec.SetSubscriptionStatus("pending", subscr.SubscriptionId); err != nil {
			SetPendingStatusErrors.Inc()
			return
		}
		SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())
	}

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
		var s xmp_api_structs.Service

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
		logCtx.Debug("new")

		// first checks
		if r.Msisdn == "" || r.CampaignId == "" {
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

		s, err = mid_client.GetServiceByCode(r.ServiceCode)
		if err != nil {
			Errors.Inc()
			time.Sleep(1)

			err = fmt.Errorf("mid_client.GetServiceById: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error":      err.Error(),
				"service_id": r.ServiceCode,
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
			"sid": r.ServiceCode,
		})
		logCtx.Debug("mo")
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
				logCtx.WithFields(log.Fields{}).Info("empty text for sms subscribe")
			} else {
				logCtx.WithFields(log.Fields{"text": r.SMSText}).Info("send sms text for mo")
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

func (mb *mobilink) setServiceFields(r *rec.Record, s xmp_api_structs.Service) {
	r.Periodic = true
	r.DelayHours = s.DelayHours
	r.PaidHours = s.PaidHours
	r.RetryDays = s.RetryDays
	r.PeriodicDays = s.PeriodicDays
	r.Price = s.PriceCents
	r.TrialDays = s.TrialDays
	r.SMSText = s.SMSOnSubscribe // for mo func
}

// ============================================================
// handle responses
func (mb *mobilink) processResponses(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var e EventNotifyTarifficate
		var logCtx *log.Entry
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
		logCtx = log.WithFields(log.Fields{
			"tid": e.EventData.Tid,
			"sid": e.EventData.ServiceCode,
		})
		logCtx.Debug("response")
		if err := mb.handleResponse(e.EventName, e.EventData); err != nil {
			Errors.Inc()
			mb.m.ResponseErrors.Inc()
			log.WithFields(log.Fields{
				"error":    err.Error(),
				"msg":      "requeue",
				"response": string(msg.Body),
			}).Error("response")
			time.Sleep(time.Second)
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

func (mb *mobilink) handleResponse(eventName string, r rec.Record) error {
	var err error
	var downloadedContentCount int
	var count int
	purgePolicyActive := true
	var s xmp_api_structs.Service
	var gracePeriod time.Duration
	var allowedSubscriptionPeriod time.Duration
	var timePassedSinsceSubscribe time.Duration

	logCtx := log.WithFields(log.Fields{
		"action": "handle response",
		"tid":    r.Tid,
	})

	if r.ServiceCode == "" {
		mb.m.ResponseErrors.Inc()
		logCtx.WithFields(log.Fields{}).Error("service code is empty")
		return nil
	}

	flagUnsubscribe := false

	s, err = mid_client.GetServiceByCode(r.ServiceCode)
	if err != nil {
		err = fmt.Errorf("mid_client.GetServiceByCode: %s", err.Error())
		return err
	}

	// do not process as usual response, need only unsub
	if eventName == "unreg" || eventName == "purge" {
		mb.removeActiveSubscription(r)

		logCtx.WithField("reason", "api unsubscribe").Info("unsubscribe subscription")

		if eventName == "unreg" {
			if err := unsubscribe(r); err != nil {
				return err
			}
		}
		if eventName == "purge" {
			if err := unsubscribeAll(r); err != nil {
				return err
			}
		}

		mb.sendChannelNotify("unreg", r)

		if s.SMSOnUnsubscribe == "" {
			logCtx.WithField("sms_text", "empty").Info("skip user notify")
			return nil
		}
		r.SMSText = s.SMSOnUnsubscribe

		logCtx.WithFields(log.Fields{"text": r.SMSText}).Info("send sms text for mo")
		publish(mb.conf.SMSRequests, "send_sms", r)

		return nil
	}

	if err := processResponse(&r, false); err != nil {
		return err
	}
	if r.Type == "injection" || r.Type == "expired" {
		logCtx.WithFields(log.Fields{
			"msisdn":             r.Msisdn,
			"type":               r.Type,
			"transaction_result": r.Result,
		}).Info("done")
		return nil
	}
	if r.Paid == true && r.Type == "" {
		mb.sendChannelNotify("renewal", r)
	}

	gracePeriod = time.Duration(24*(s.RetryDays-s.GraceDays)) * time.Hour
	allowedSubscriptionPeriod = time.Duration(24*s.RetryDays) * time.Hour
	timePassedSinsceSubscribe = time.Now().UTC().Sub(mb.getSubscriptionStartTime(r))

	if r.SubscriptionId == 0 {
		mb.m.ResponseErrors.Inc()
		logCtx.WithFields(log.Fields{}).Error("subscriptionId is empty")
		return nil
	}

	// purge policy starts from certain period
	if s.PurgeAfterDays > 0 && timePassedSinsceSubscribe < time.Duration(24*s.PurgeAfterDays)*time.Hour {
		purgePolicyActive = false
	}

	if purgePolicyActive {
		count, err = rec.GetCountOfFailedChargesFor(r.Msisdn, r.Tid, r.SubscriptionId, s.InactiveDays)
		if err != nil {
			return err
		}
		if r.Paid == false {
			count = count + 1
		}
		if count > s.InactiveDays {
			logCtx.WithFields(log.Fields{
				"reason":               "inactive days",
				"failed_charges_count": count,
				"inactive_days":        s.InactiveDays,
			}).Info("deactivate subscription")
			flagUnsubscribe = true
			goto unsubscribe
		}

		if r.Paid == false && timePassedSinsceSubscribe > gracePeriod {
			logCtx.WithField("reason", "failed charge in grace period").Info("deactivate subscription")
			flagUnsubscribe = true
			goto unsubscribe
		}

		if timePassedSinsceSubscribe > allowedSubscriptionPeriod {
			logCtx.WithField("reason", "passed the subscription period").Info("deactivate subscription")
			flagUnsubscribe = true
			goto unsubscribe
		}

		downloadedContentCount, err = rec.GetCountOfDownloadedContent(r.SubscriptionId)
		if err != nil {
			logCtx.WithField("error", err.Error()).Error("cannot get count of downloaded content")
			downloadedContentCount = s.MinimalTouchTimes
		}
		if downloadedContentCount < s.MinimalTouchTimes && timePassedSinsceSubscribe > gracePeriod {
			logCtx.WithField("reason", "too view content downloaded").Info("deactivate subscription")
			flagUnsubscribe = true
			goto unsubscribe
		}

	unsubscribe:
		if flagUnsubscribe {
			mb.removeActiveSubscription(r)
			r.SubscriptionStatus = "inactive"
			if err := writeSubscriptionStatus(r); err != nil {
				return err
			}
			mb.sendChannelNotify("unreg", r)

			if s.SMSOnUnsubscribe == "" {
				log.WithField("sms_text", "empty").Info("skip user notify")
				return nil
			}
			r.SMSText = s.SMSOnUnsubscribe

			logCtx.WithFields(log.Fields{"text": r.SMSText}).Info("send sms text for unsubscribe")
			publish(mb.conf.SMSRequests, "send_sms", r)

		}
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
	if r.Channel == "" {
		return
	}
	mb.m.ChannelTotal.Inc()
	if !mb.conf.Channel.Enabled {
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
	} else if event == "unreg" {
		eventNum = "3"
	} else {
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"event":  event,
		}).Error("wrong event type")
		mb.m.ChannelErrors.Inc()
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
		mb.m.ChannelErrors.Inc()
		return
	}
	if resp.StatusCode == 200 ||
		resp.StatusCode == 201 ||
		resp.StatusCode == 202 {

		mb.m.ChannelSuccess.Inc()
		log.WithFields(log.Fields{
			"tid":  r.Tid,
			"url":  channelUrl,
			"code": resp.StatusCode,
		}).Info("channel notify")
		return
	}
	log.WithFields(log.Fields{
		"tid":    r.Tid,
		"msisdn": r.Msisdn,
		"event":  event,
		"url":    channelUrl,
		"error":  resp.Status,
	}).Warn("channel notify status code is suspisious")
	mb.m.ChannelErrors.Inc()
}

func (mb *mobilink) sendContent() error {
	if !mb.conf.Content.Enabled {
		return nil
	}

	begin := time.Now().UTC()
	subscriptions, err := rec.GetLiveTodayPeriodicsForContent(mb.conf.Content.FetchLimit)
	if err != nil {
		return err
	}
	mb.m.GetContentDuration.Observe(time.Since(begin).Seconds())

	begin = time.Now().UTC()
	for _, subscr := range subscriptions {
		logCtx := log.WithFields(log.Fields{
			"tid":    subscr.Tid,
			"action": "send content",
		})

		s, err := mid_client.GetServiceByCode(subscr.ServiceCode)
		if err != nil {
			Errors.Inc()
			err = fmt.Errorf("mid_client.GetServiceById: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error":      err.Error(),
				"service_id": subscr.ServiceCode,
			}).Error("cannot get service by id")
			return err
		}
		if s.SMSOnContent == "" {
			logCtx.WithFields(log.Fields{
				"service_id": subscr.ServiceCode,
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

		logCtx.WithFields(log.Fields{"text": subscr.SMSText}).Info("send sms text for send content")
		if err := publish(mb.conf.SMSRequests, "send_sms", subscr); err != nil {

			err = fmt.Errorf("publish: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot enqueue")
			return err
		}
	}
	mb.m.ProcessContentDuration.Observe(time.Since(begin).Seconds())
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
			key := v.Msisdn + "-" + v.ServiceCode
			mb.prevCache.Set(key, v.CreatedAt, storeDuration)
		}
	}
}

func (mb *mobilink) removeActiveSubscription(r rec.Record) {
	key := r.Msisdn + r.ServiceCode
	mb.prevCache.Delete(key)
}

func (mb *mobilink) getSubscriptionStartTime(r rec.Record) time.Time {
	key := r.Msisdn + "-" + r.ServiceCode
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
	key := r.Msisdn + "-" + r.ServiceCode
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
	key := r.Msisdn + "-" + r.ServiceCode
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
	Dropped                           m.Gauge
	Empty                             m.Gauge
	AddToDBErrors                     m.Gauge
	AddToDbSuccess                    m.Gauge
	NotifyErrors                      m.Gauge
	SMSSent                           m.Gauge
	ResponseDropped                   m.Gauge
	ResponseErrors                    m.Gauge
	ResponseSuccess                   m.Gauge
	ChannelTotal                      m.Gauge
	ChannelSuccess                    m.Gauge
	ChannelErrors                     m.Gauge
	SincePeriodicStartProcess         prometheus.Gauge
	GetPeriodicsDuration              prometheus.Summary
	ProcessPeriodicsDuration          prometheus.Summary
	SinceReChargePeriodicStartProcess prometheus.Gauge
	GetReChargePeriodicsDuration      prometheus.Summary
	ProcessReChargePeriodicsDuration  prometheus.Summary
	GetContentDuration                prometheus.Summary
	ProcessContentDuration            prometheus.Summary
}

func (mb *mobilink) initMetrics() {
	mbm := &MobilinkMetrics{
		Dropped:                           m.NewGauge(appName, mb.conf.OperatorName, "dropped", "mobilink dropped"),
		Empty:                             m.NewGauge(appName, mb.conf.OperatorName, "empty", "mobilink queue empty"),
		AddToDBErrors:                     m.NewGauge(appName, mb.conf.OperatorName, "add_to_db_errors", "subscription add to db errors"),
		AddToDbSuccess:                    m.NewGauge(appName, mb.conf.OperatorName, "add_to_db_success", "subscription add to db success"),
		NotifyErrors:                      m.NewGauge(appName, mb.conf.OperatorName, "notify_errors", "notify errors"),
		SMSSent:                           m.NewGauge(appName, mb.conf.OperatorName, "sms_sent", "sms sent"),
		ResponseDropped:                   m.NewGauge(appName, mb.conf.OperatorName, "response_dropped", "dropped"),
		ResponseErrors:                    m.NewGauge(appName, mb.conf.OperatorName, "response_errors", "errors"),
		ResponseSuccess:                   m.NewGauge(appName, mb.conf.OperatorName, "response_success", "success"),
		ChannelTotal:                      m.NewGauge(appName, mb.conf.OperatorName, "channel_total", "channel total"),
		ChannelSuccess:                    m.NewGauge(appName, mb.conf.OperatorName, "channel_success", "channel success"),
		ChannelErrors:                     m.NewGauge(appName, mb.conf.OperatorName, "channel_errors", "channel errors"),
		SincePeriodicStartProcess:         m.PrometheusGauge(appName, mb.conf.OperatorName, "since_last_periodic_fetch_seconds", "seconds since last periodic processing"),
		GetPeriodicsDuration:              m.NewSummary("get_periodics_duration_seconds", "get periodics duration seconds"),
		ProcessPeriodicsDuration:          m.NewSummary("process_periodics_duration_seconds", "process periodics duration seconds"),
		SinceReChargePeriodicStartProcess: m.PrometheusGauge(appName, mb.conf.OperatorName, "since_last_recharge_periodic_fetch_seconds", "seconds since last recharge periodic processing"),
		GetReChargePeriodicsDuration:      m.NewSummary("get_recharge_periodics_duration_seconds", "get recharge periodics duration seconds"),
		ProcessReChargePeriodicsDuration:  m.NewSummary("process_recharge_periodics_duration_seconds", "process recharge periodics duration seconds"),
		GetContentDuration:                m.NewSummary("get_content_duration_seconds", "get content duration seconds"),
		ProcessContentDuration:            m.NewSummary("process_content_duration_seconds", "process content duration seconds"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			mbm.Dropped.Update()
			mbm.Empty.Update()
			mbm.AddToDBErrors.Update()
			mbm.AddToDbSuccess.Update()
			mbm.NotifyErrors.Update()
			mbm.SMSSent.Update()
			mbm.ResponseDropped.Update()
			mbm.ResponseErrors.Update()
			mbm.ResponseSuccess.Update()
			mbm.ChannelTotal.Update()
			mbm.ChannelSuccess.Update()
			mbm.ChannelErrors.Update()
		}
	}()

	go func() {
		for range time.Tick(time.Second) {
			mbm.SincePeriodicStartProcess.Inc()
			mbm.SinceReChargePeriodicStartProcess.Inc()
		}
	}()
	mb.m = mbm
}
