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

	"github.com/vostrok/utils/amqp"
	queue_config "github.com/vostrok/utils/config"
	m "github.com/vostrok/utils/metrics"
	rec "github.com/vostrok/utils/rec"
)

type mobilink struct {
	conf                 MobilinkConfig
	m                    *MobilinkMetrics
	prevCache            *cache.Cache
	NewCh                <-chan amqp_driver.Delivery
	NewConsumer          *amqp.Consumer
	MOCh                 <-chan amqp_driver.Delivery
	MOConsumer           *amqp.Consumer
	ResponsesCh          <-chan amqp_driver.Delivery
	ResponsesConsumer    *amqp.Consumer
	SMSResponsesCh       <-chan amqp_driver.Delivery
	SMSResponsesConsumer *amqp.Consumer
}

type MobilinkConfig struct {
	Enabled         bool                            `yaml:"enabled" default:"false"`
	OperatorName    string                          `yaml:"operator_name" default:"mobilink"`
	OperatorCode    int64                           `yaml:"operator_code" default:"41001"`
	Retries         RetriesConfig                   `yaml:"retries"`
	Periodic        PeriodicConfig                  `yaml:"periodic" `
	Requests        string                          `yaml:"requests"`
	SMSRequests     string                          `yaml:"sms_requests"`
	NewSubscription queue_config.ConsumeQueueConfig `yaml:"new"`
	MO              queue_config.ConsumeQueueConfig `yaml:"mo"`
	Responses       queue_config.ConsumeQueueConfig `yaml:"responses"`
	SMSResponses    queue_config.ConsumeQueueConfig `yaml:"sms_responses"`
}

func initMobilink(mbConfig MobilinkConfig, consumerConfig amqp.ConsumerConfig) *mobilink {
	if !mbConfig.Enabled {
		return nil
	}
	mb := &mobilink{
		conf: mbConfig,
	}
	mb.initPrevSubscriptionsCache()
	mb.initMetrics()
	if mbConfig.Requests == "" {
		log.Fatal("empty queue name requests")
	}
	if mbConfig.SMSRequests == "" {
		log.Fatal("empty queue name sms requests")
	}
	if mbConfig.NewSubscription.Enabled {
		if mbConfig.NewSubscription.Name == "" {
			log.Fatal("empty queue name new subscription")
		}
		mb.NewConsumer = amqp.NewConsumer(
			consumerConfig,
			mbConfig.NewSubscription.Name,
			mbConfig.NewSubscription.PrefetchCount,
		)
		if err := mb.NewConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			mb.NewConsumer,
			mb.NewCh,
			mb.processNewMobilinkSubscription,
			mbConfig.NewSubscription.ThreadsCount,
			mbConfig.NewSubscription.Name,
			mbConfig.NewSubscription.Name,
		)
	}
	if mbConfig.MO.Enabled {
		if mbConfig.MO.Name == "" {
			log.Fatal("empty queue name mo")
		}
		mb.MOConsumer = amqp.NewConsumer(
			consumerConfig,
			mbConfig.MO.Name,
			mbConfig.MO.PrefetchCount,
		)
		if err := mb.MOConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			mb.MOConsumer,
			mb.MOCh,
			mb.processMO,
			mbConfig.MO.ThreadsCount,
			mbConfig.MO.Name,
			mbConfig.MO.Name,
		)
	}
	if mbConfig.Responses.Enabled {
		if mbConfig.Responses.Name == "" {
			log.Fatal("empty queue name responses")
		}
		mb.ResponsesConsumer = amqp.NewConsumer(
			consumerConfig,
			mbConfig.Responses.Name,
			mbConfig.Responses.PrefetchCount,
		)
		if err := mb.ResponsesConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			mb.ResponsesConsumer,
			mb.ResponsesCh,
			mb.processResponses,
			mbConfig.Responses.ThreadsCount,
			mbConfig.Responses.Name,
			mbConfig.Responses.Name,
		)
	}
	if mbConfig.SMSResponses.Enabled {
		if mbConfig.SMSResponses.Name == "" {
			log.Fatal("empty queue name sms responses")
		}
		mb.SMSResponsesConsumer = amqp.NewConsumer(
			consumerConfig,
			mbConfig.SMSResponses.Name,
			mbConfig.SMSResponses.PrefetchCount,
		)
		if err := mb.SMSResponsesConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			mb.SMSResponsesConsumer,
			mb.SMSResponsesCh,
			mb.processSMSResponses,
			mbConfig.SMSResponses.ThreadsCount,
			mbConfig.SMSResponses.Name,
			mbConfig.SMSResponses.Name,
		)
	}
	log.WithFields(log.Fields{
		"seconds": mbConfig.Retries.Period,
	}).Debug("init retries")

	go func() {
	retries:
		for range time.Tick(time.Duration(mbConfig.Retries.Period) * time.Second) {
			//for true {
			for _, queue := range mbConfig.Retries.CheckQueuesFree {
				queueSize, err := svc.notifier.GetQueueSize(queue)
				if err != nil {
					log.WithFields(log.Fields{
						"operator": mbConfig.OperatorName,
						"queue":    queue,
						"error":    err.Error(),
					}).Error("cannot get queue size")
					continue retries
				}
				log.WithFields(log.Fields{
					"queue":     queue,
					"queueSize": queueSize,
					"waitFor":   mbConfig.Retries.QueueFreeSize,
				}).Debug("")
				if queueSize > mbConfig.Retries.QueueFreeSize {
					continue retries
				}
			}
			log.WithFields(log.Fields{}).Debug("achieve free queues")
			if mbConfig.Retries.Enabled {
				mb.m.SinceRetryStartProcessed.Set(.0)
				ProcessRetries(mbConfig.OperatorCode, mbConfig.Retries.FetchLimit, mb.publishToTelcoAPI)
			} else {
				log.Debug("retries disabled")
			}
		}
	}()

	return mb
}

// ============================================================
// new subscriptions came from dispatcherd
// one thing - insert in db
// and send in mo queue
type EventNotifyNewSubscription struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

func (mb *mobilink) processNewMobilinkSubscription(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var ns EventNotifyNewSubscription
		var r rec.Record

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
		if err := rec.AddNewSubscriptionToDB(&r); err != nil {
			mb.m.AddToDBErrors.Inc()
			msg.Nack(false, true)
			continue
		} else {
			mb.m.AddToDbSuccess.Inc()
		}

		if err := mb.sendToMO(r); err != nil {
			log.WithFields(log.Fields{
				"tid":   r.Tid,
				"error": err.Error(),
			}).Error("send to mo")
			msg.Nack(false, true)
			continue
		}

	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
func (mb *mobilink) sendToMO(r rec.Record) error {
	event := amqp.EventNotify{
		EventName: "mo",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return err
	}
	svc.notifier.Publish(amqp.AMQPMessage{QueueName: mb.conf.MO.Name, Body: body})
	return nil
}

// ============================================================
// new subscriptions came from dispatcherd
// checks for MO
// set/get rejected cache
// publish to telco
func (mb *mobilink) processMO(deliveries <-chan amqp_driver.Delivery) {
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
				"q":     mb.conf.MO.Name,
			}).Error("failed")
			goto ack
		}

		r = ns.EventData
		if err = checkMO(&r, mb.getPrevSubscriptionCache, mb.setPrevSubscriptionCache); err != nil {
			msg.Nack(false, true)
			continue
		}
		if r.SubscriptionStatus == "" {
			if err := mb.publishToTelcoAPI(1, r); err != nil {
				log.WithFields(log.Fields{
					"tid":   r.Tid,
					"error": err.Error(),
				}).Error("send to charge")
				msg.Nack(false, true)
				continue
			}
		}

	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
func (mb *mobilink) initPrevSubscriptionsCache() {
	prev, err := rec.LoadPreviousSubscriptions(mb.conf.OperatorCode)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load previous subscriptions")
	}
	log.WithField("count", len(prev)).Debug("loaded previous subscriptions")
	mb.prevCache = cache.New(24*time.Hour, time.Minute)
	for _, v := range prev {
		key := v.Msisdn + strconv.FormatInt(v.CampaignId, 10)
		mb.prevCache.Set(key, struct{}{}, time.Now().Sub(v.CreatedAt))
	}
}
func (mb *mobilink) getPrevSubscriptionCache(r rec.Record) bool {
	key := r.Msisdn + strconv.FormatInt(r.CampaignId, 10)
	_, found := mb.prevCache.Get(key)
	return found
}
func (mb *mobilink) setPrevSubscriptionCache(r rec.Record) {
	key := r.Msisdn + strconv.FormatInt(r.CampaignId, 10)
	_, found := mb.prevCache.Get(key)
	if !found {
		mb.prevCache.Set(key, struct{}{}, 24*time.Hour)
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
	ResponseSMSDropped       m.Gauge
	ResponseSMSErrors        m.Gauge
	ResponseSMSSuccess       m.Gauge
	SinceRetryStartProcessed prometheus.Gauge
	SinceLastSuccessPay      prometheus.Gauge
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
		ResponseSMSDropped:       m.NewGauge(appName, mb.conf.OperatorName, "response_sms_dropped", "sms response dropped"),
		ResponseSMSErrors:        m.NewGauge(appName, mb.conf.OperatorName, "response_sms_errors", "errors"),
		ResponseSMSSuccess:       m.NewGauge(appName, mb.conf.OperatorName, "response_sms_success", "success"),
		SinceRetryStartProcessed: m.PrometheusGauge(appName, mb.conf.OperatorName, "since_last_retries_fetch_seconds", "seconds since last retries processing"),
		SinceLastSuccessPay:      m.PrometheusGauge(appName, mb.conf.OperatorName, "since_last_success_pay_seconds", "seconds since success pay"),
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
			mbm.ResponseSMSDropped.Update()
			mbm.ResponseSMSErrors.Update()
			mbm.ResponseSMSSuccess.Update()
		}
	}()
	go func() {
		for range time.Tick(time.Second) {
			mbm.SinceRetryStartProcessed.Inc()
			mbm.SinceLastSuccessPay.Inc()
		}
	}()
	mb.m = mbm
}

// ============================================================
// notifier functions
func (mb *mobilink) publishToTelcoAPI(priority uint8, r rec.Record) (err error) {
	event := amqp.EventNotify{
		EventName: "charge",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return
	}
	svc.notifier.Publish(amqp.AMQPMessage{QueueName: mb.conf.Requests, Priority: priority, Body: body})
	return nil
}
func (mb *mobilink) publishToTelcoAPISMS(r rec.Record) (err error) {
	event := amqp.EventNotify{
		EventName: "send_sms",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return
	}
	svc.notifier.Publish(amqp.AMQPMessage{QueueName: mb.conf.SMSRequests, Priority: 0, Body: body})
	return nil
}

// ============================================================
// responses
func (mb *mobilink) processResponses(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var e EventNotifyTarifficate
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			mb.m.ResponseDropped.Inc()

			log.WithFields(log.Fields{
				"error":    err.Error(),
				"msg":      "dropped",
				"response": string(msg.Body),
			}).Error("failed")
			goto ack
		}
		if err := mb.handleResponse(e.EventData); err != nil {
			mb.m.ResponseErrors.Inc()
			msg.Nack(false, true)
			continue
		}
		mb.m.ResponseSuccess.Inc()
	ack:
		if err := msg.Ack(false); err != nil {
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
	if err := processResponse(&r); err != nil {
		return err
	}
	if r.SubscriptionStatus == "paid" {
		mb.m.SinceLastSuccessPay.Set(.0)
	}
	logCtx := log.WithFields(log.Fields{
		"tid":    r.Tid,
		"msisdn": r.Msisdn,
	})
	if r.AttemptsCount == 0 && r.SubscriptionStatus == "failed" {
		if r.SMSSend {
			logCtx.WithField("sms", "send").Info(r.SMSText)

			smsTranasction := r
			smsTranasction.Result = "sms"
			if err := mb.smsSend(r, r.SMSText); err != nil {
				Errors.Inc()
				return err
			}
			if err := writeTransaction(r); err != nil {
				Errors.Inc()
				return err
			}
		} else {
			logCtx.WithField("serviceId", r.ServiceId).Debug("send sms disabled")
		}
	}

	// send everything, pixels module will decide to send pixel, or not to send
	if r.Pixel != "" && r.AttemptsCount == 0 && r.SubscriptionStatus != "postpaid" {
		if err := notifyPixel(r); err != nil {
			Errors.Inc()
		}
	}

	return nil
}
func (mb *mobilink) smsSend(record rec.Record, msg string) error {
	record.SMSText = msg
	if err := mb.publishToTelcoAPISMS(record); err != nil {
		err = fmt.Errorf("notifyOperatorRequest: %s, queue: %s", err.Error(), mb.conf.SMSRequests)
		log.WithFields(log.Fields{
			"tid":    record.Tid,
			"msisdn": record.Msisdn,
			"error":  err.Error(),
		}).Error("cannot send sms")
		return err
	}
	return nil
}

// ============================================================
// sms responses
func (mb *mobilink) processSMSResponses(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var e EventNotifyTarifficate
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			mb.m.ResponseSMSDropped.Inc()

			log.WithFields(log.Fields{
				"error":    err.Error(),
				"msg":      "dropped",
				"response": string(msg.Body),
			}).Error("failed")
			goto ack
		}

		if err := handleSMSResponse(e.EventData); err != nil {
			mb.m.ResponseSMSErrors.Inc()
			msg.Nack(false, true)
			continue
		}
		mb.m.ResponseSMSSuccess.Inc()
	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"tid":   e.EventData.Tid,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}

	}
}
func handleSMSResponse(record rec.Record) error {
	smsTranasction := record
	smsTranasction.Result = "sms"
	if smsTranasction.OperatorErr != "" {
		smsTranasction.Result = "sms failed"
	} else {
		smsTranasction.Result = "sms sent"
	}
	if err := writeTransaction(smsTranasction); err != nil {
		Errors.Inc()
		return err
	}
	return nil
}
