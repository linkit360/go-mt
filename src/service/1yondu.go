package service

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	cache "github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	amqp_driver "github.com/streadway/amqp"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	content_client "github.com/vostrok/contentd/rpcclient"
	content_service "github.com/vostrok/contentd/service"
	inmem_client "github.com/vostrok/inmem/rpcclient"
	yondu_service "github.com/vostrok/operator/ph/yondu/src/service"
	transaction_log_service "github.com/vostrok/qlistener/src/service"
	"github.com/vostrok/utils/amqp"
	queue_config "github.com/vostrok/utils/config"
	m "github.com/vostrok/utils/metrics"
	rec "github.com/vostrok/utils/rec"
)

type yondu struct {
	conf             YonduConfig
	m                *YonduMetrics
	prevCache        *cache.Cache
	retriesWg        *sync.WaitGroup
	MOCh             <-chan amqp_driver.Delivery
	MOConsumer       *amqp.Consumer
	CallBackCh       <-chan amqp_driver.Delivery
	CallBackConsumer *amqp.Consumer
}

type YonduConfig struct {
	Enabled            bool                            `yaml:"enabled" default:"false"`
	OperatorName       string                          `yaml:"operator_name" default:"yondu"`
	OperatorCode       int64                           `yaml:"operator_code" default:"51501"`
	ContentUrl         string                          `yaml:"content_url"`
	Periodic           PeriodicConfig                  `yaml:"periodic" `
	Retries            RetriesConfig                   `yaml:"retries"`
	SentConsent        string                          `yaml:"sent_consent"`
	MT                 string                          `yaml:"mt"`
	Charge             string                          `yaml:"charge"`
	NewSubscription    queue_config.ConsumeQueueConfig `yaml:"new"`
	CallBack           queue_config.ConsumeQueueConfig `yaml:"callback"`
	UnsubscribeMarkers []string                        `yaml:"unsubscribe"`
}

type PeriodicConfig struct {
	Enabled    bool `yaml:"enabled" default:"false"`
	FetchLimit int  `yaml:"fetch_limit" default:"500"`
}

func initYondu(yConf YonduConfig, consumerConfig amqp.ConsumerConfig, contentConf content_client.RPCClientConfig) *yondu {
	if !yConf.Enabled {
		return nil
	}
	y := &yondu{
		conf: yConf,
	}
	y.initMetrics()
	y.initPrevSubscriptionsCache()

	content_client.Init(contentConf)

	if yConf.NewSubscription.Enabled {
		if yConf.NewSubscription.Name == "" {
			log.Fatal("empty queue name new subscriptions")
		}
		y.MOConsumer = amqp.NewConsumer(
			consumerConfig,
			yConf.NewSubscription.Name,
			yConf.NewSubscription.PrefetchCount,
		)
		if err := y.MOConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			y.MOConsumer,
			y.MOCh,
			y.processNewSubscription,
			yConf.NewSubscription.ThreadsCount,
			yConf.NewSubscription.Name,
			yConf.NewSubscription.Name,
		)
	} else {
		log.Debug("new subscription disabled")
	}

	if yConf.CallBack.Enabled {
		if yConf.CallBack.Name == "" {
			log.Fatal("empty queue name callback")
		}
		y.CallBackConsumer = amqp.NewConsumer(
			consumerConfig,
			yConf.CallBack.Name,
			yConf.CallBack.PrefetchCount,
		)
		if err := y.CallBackConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			y.CallBackConsumer,
			y.CallBackCh,
			y.processCallBack,
			yConf.CallBack.ThreadsCount,
			yConf.CallBack.Name,
			yConf.CallBack.Name,
		)
	} else {
		log.Debug("callbacks disabled")
	}

	if yConf.Periodic.Enabled {
		go func() {
			for {
				time.Sleep(time.Second)
				y.processPeriodic()
			}
		}()
	} else {
		log.Debug("periodic disabled")
	}

	if yConf.Retries.Enabled {
		go func() {
		retries:
			for range time.Tick(time.Duration(yConf.Retries.Period) * time.Second) {
				for _, queue := range yConf.Retries.CheckQueuesFree {
					queueSize, err := svc.notifier.GetQueueSize(queue)
					if err != nil {
						log.WithFields(log.Fields{
							"operator": yConf.OperatorName,
							"queue":    queue,
							"error":    err.Error(),
						}).Error("cannot get queue size")
						continue retries
					}
					log.WithFields(log.Fields{
						"operator":  yConf.OperatorName,
						"queue":     queue,
						"queueSize": queueSize,
						"waitFor":   yConf.Retries.QueueFreeSize,
					}).Debug("")
					if queueSize > yConf.Retries.QueueFreeSize {
						continue retries
					}
				}
				log.WithFields(log.Fields{
					"operator": yConf.OperatorName,
					"waitFor":  yConf.Retries.QueueFreeSize,
				}).Debug("achieve free queues size")
				ProcessRetries(yConf.OperatorCode, yConf.Retries.FetchLimit, y.publishCharge)
			}
		}()

	}
	return y
}

// get periodic for this day and time
// generate subscriptions tid
// tid := rec.GenerateTID()
// create new subscriptions
// generate send_content_text
// send sms via Yondu API
// create periodic transactions
// update periodic last_request_at
func (y *yondu) processPeriodic() {

	begin := time.Now()
	notIn, err := y.getPendingPeriodicSubscriptionIds()
	if err != nil {
		err = fmt.Errorf("y.getPendingSubscriptionIds: %s", err.Error())
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("cannot get periodic not ins")
		return
	}
	log.WithField("ids", notIn).Debug("pending subscription ids")

	periodics, err := rec.GetPeriodics(
		y.conf.OperatorCode,
		y.conf.Periodic.FetchLimit,
		notIn,
	)
	if err != nil {
		err = fmt.Errorf("rec.GetPeriodics: %s", err.Error())
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("cannot get periodics")
		return
	}
	y.m.GetPeriodicsDuration.Observe(time.Since(begin).Seconds())

	begin = time.Now()
	for _, r := range periodics {
		logCtx := log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
		})

		service, err := inmem_client.GetServiceById(r.ServiceId)
		if err != nil {
			y.m.MOUnknownService.Inc()

			err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
			log.WithFields(log.Fields{
				"serviceId": r.ServiceId,
				"error":     err.Error(),
			}).Error("cannot get service by id")
			continue
		}
		contentProperties, err := content_client.GetUniqueUrl(content_service.GetUniqueUrlParams{
			Msisdn:     r.Msisdn,
			CampaignId: r.CampaignId,
		})
		if contentProperties.Error != "" {
			err = fmt.Errorf("content_client.GetUniqueUrl: %s", contentProperties.Error)
			log.WithFields(log.Fields{
				"serviceId": r.ServiceId,
				"error":     err.Error(),
			}).Error("cannotd internal error")
			continue
		}
		if err != nil {

			err = fmt.Errorf("content_client.GetUniqueUrl: %s", err.Error())
			log.WithFields(log.Fields{
				"serviceId": r.ServiceId,
				"error":     err.Error(),
			}).Error("cannot get unique content url")
			continue
		}

		y.setRecCache(r)
		if service.SendContentTextTemplate == "" {
			service.SendContentTextTemplate = "Thanks! You could get content here: %s"
		}
		url := y.conf.ContentUrl + contentProperties.UniqueUrl
		r.SMSText = fmt.Sprintf(service.SendContentTextTemplate, url)
		if err := y.publishMT(r); err != nil {
			logCtx.WithField("error", err.Error()).Error("publishYonduMT")
		}
		if err := y.publishCharge(1, r); err != nil {
			logCtx.WithField("error", err.Error()).Error("publishYonduCharge")
		}
		y.setPendingPeriodicSubscriptionCache(r)
	}
	y.m.ProcessPeriodicsDuration.Observe(time.Since(begin).Seconds())
}

// ============================================================
// new subscritpion
type EventNotifyMO struct {
	EventName string                     `json:"event_name,omitempty"`
	EventData yondu_service.MOParameters `json:"event_data,omitempty"`
}

func (y *yondu) processNewSubscription(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var r rec.Record
		var err error
		var logCtx *log.Entry
		var transactionMsg transaction_log_service.OperatorTransactionLog

		var e EventNotifyMO
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			y.m.MODropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"mo":    string(msg.Body),
			}).Error("consume from " + y.conf.NewSubscription.Name)
			goto ack
		}

		r, err = y.getRecordByMO(e.EventData)
		if err != nil {
			msg.Nack(false, true)
			continue
		}

		for _, marker := range y.conf.UnsubscribeMarkers {
			if strings.Contains(e.EventData.Params.KeyWord, marker) {
				r.SubscriptionStatus = "canceled"
				if err := writeSubscriptionStatus(r); err != nil {
					msg.Nack(false, true)
					continue
				}
				goto ack
			}
		}

		// in check MO func we have to have subscription id
		if err := rec.AddNewSubscriptionToDB(&r); err != nil {
			y.m.AddToDBErrors.Inc()
			msg.Nack(false, true)
			continue
		} else {
			y.m.AddToDbSuccess.Inc()
		}

		err = checkMO(&r, y.getPrevSubscriptionCache, y.setPrevSubscriptionCache)
		if err != nil {
			msg.Nack(false, true)
			continue
		}

		transactionMsg = transaction_log_service.OperatorTransactionLog{
			Tid:              r.Tid,
			Msisdn:           r.Msisdn,
			OperatorToken:    r.OperatorToken,
			OperatorCode:     r.OperatorCode,
			CountryCode:      r.CountryCode,
			Error:            "",
			Price:            r.Price,
			ServiceId:        r.ServiceId,
			SubscriptionId:   r.SubscriptionId,
			CampaignId:       r.CampaignId,
			RequestBody:      e.EventData.Raw,
			ResponseBody:     "",
			ResponseDecision: "",
			ResponseCode:     200,
			SentAt:           r.SentAt,
			Type:             e.EventName,
		}
		if err := publishTransactionLog("mo", transactionMsg); err != nil {
			logCtx.WithField("error", err.Error()).Error("publishTransactionLog")
		}
		if r.Result == "" {
			y.setRecCache(r)
			if err := y.publishSentConsent(r); err != nil {
				logCtx.WithField("error", err.Error()).Error("publishYonduSentConsent")
			}
		} else {
			if err := y.publishMT(r); err != nil {
				logCtx.WithField("error", err.Error()).Error("publishYonduMT")
			}
		}

	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"mo":    msg.Body,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}

	}
}
func (y *yondu) getRecordByMO(req yondu_service.MOParameters) (rec.Record, error) {
	r := rec.Record{}
	campaign, err := inmem_client.GetCampaignByKeyWord(req.Params.KeyWord)
	if err != nil {
		y.m.MOUnknownCampaign.Inc()

		err = fmt.Errorf("inmem_client.GetCampaignByKeyWord: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword": req.Params.KeyWord,
			"error":   err.Error(),
		}).Error("cannot get campaign by keyword")
		return r, err
	}
	svc, err := inmem_client.GetServiceById(campaign.ServiceId)
	if err != nil {
		y.m.MOUnknownService.Inc()

		err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword":    req.Params.KeyWord,
			"serviceId":  campaign.ServiceId,
			"campaignId": campaign.Id,
			"error":      err.Error(),
		}).Error("cannot get service by id")
		return r, err
	}
	publisher := ""
	pixelSetting, err := inmem_client.GetPixelSettingByCampaignId(campaign.Id)
	if err != nil {
		y.m.MOUnknownPublisher.Inc()

		err = fmt.Errorf("inmem_client.GetPixelSettingByCampaignId: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword":    req.Params.KeyWord,
			"serviceId":  campaign.ServiceId,
			"campaignId": campaign.Id,
			"error":      err.Error(),
		}).Error("cannot get pixel setting by campaign id")
	} else {
		publisher = pixelSetting.Publisher
	}

	sentAt, err := time.Parse("20060102150405", req.Params.Timestamp)
	if err != nil {
		y.m.MOParseTimeError.Inc()

		err = fmt.Errorf("time.Parse: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword":    req.Params.KeyWord,
			"serviceId":  campaign.ServiceId,
			"campaignId": campaign.Id,
			"error":      err.Error(),
		}).Error("cannot parse operators time")
		sentAt = time.Now().UTC()
	}
	r = rec.Record{
		SentAt:                   sentAt,
		Msisdn:                   req.Params.Msisdn,
		Tid:                      req.Tid,
		SubscriptionStatus:       "",
		CountryCode:              515,
		OperatorCode:             y.conf.OperatorCode,
		Publisher:                publisher,
		Pixel:                    "",
		CampaignId:               campaign.Id,
		ServiceId:                campaign.ServiceId,
		DelayHours:               svc.DelayHours,
		PaidHours:                svc.PaidHours,
		KeepDays:                 svc.KeepDays,
		Price:                    int(svc.Price),
		OperatorToken:            req.Params.TransID,
		Periodic:                 true,
		PeriodicDays:             svc.PeriodicDays,
		PeriodicAllowedFromHours: svc.PeriodicAllowedFrom,
		PeriodicAllowedToHours:   svc.PeriodicAllowedTo,
		SMSText:                  "Unfortunately, you cannot access our service",
	}
	return r, nil
}

// ============================================================
// callback
type EventNotifyCallBack struct {
	EventName string                           `json:"event_name,omitempty"`
	EventData yondu_service.CallbackParameters `json:"event_data,omitempty"`
}

func (y *yondu) processCallBack(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var r rec.Record
		var err error
		var logCtx *log.Entry
		var transactionMsg transaction_log_service.OperatorTransactionLog

		log.WithFields(log.Fields{
			"priority": msg.Priority,
			"body":     string(msg.Body),
		}).Debug("start process")

		var e EventNotifyCallBack

		if err := json.Unmarshal(msg.Body, &e); err != nil {
			y.m.CallBackDropped.Inc()

			log.WithFields(log.Fields{
				"error":    err.Error(),
				"msg":      "dropped",
				"callback": string(msg.Body),
				"q":        y.conf.CallBack.Name,
			}).Error("failed")
			goto ack
		}
		r, err = y.getRecordByCallback(e.EventData)
		if err != nil {
			goto ack
		}
		if e.EventData.Params.StatusCode == "0" {
			r.Paid = true
			y.m.SinceLastSuccessPay.Set(.0)
		}
		if err = processResponse(&r); err != nil {
			msg.Nack(false, true)
			continue
		}
		y.deleteRecCache(r)
		logCtx = log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
		})

		transactionMsg = transaction_log_service.OperatorTransactionLog{
			Tid:              r.Tid,
			Msisdn:           r.Msisdn,
			OperatorToken:    r.OperatorToken,
			OperatorCode:     r.OperatorCode,
			CountryCode:      r.CountryCode,
			Error:            "",
			Price:            r.Price,
			ServiceId:        r.ServiceId,
			SubscriptionId:   r.SubscriptionId,
			CampaignId:       r.CampaignId,
			RequestBody:      e.EventData.Raw,
			ResponseBody:     "",
			ResponseDecision: r.SubscriptionStatus,
			ResponseCode:     200,
			SentAt:           r.SentAt,
			Type:             e.EventName,
		}
		if err := publishTransactionLog("callback", transactionMsg); err != nil {
			logCtx.WithFields(log.Fields{
				"event":    e.EventName,
				"callback": msg.Body,
				"error":    err.Error(),
			}).Error("sent to transaction log failed")
			msg.Nack(false, true)
			continue
		} else {
			logCtx.WithFields(log.Fields{
				"queue": y.conf.CallBack.Name,
				"event": e.EventName,
				"tid":   r.Tid,
			}).Info("sent to transaction log")
		}

	ack:
		y.deletePendingPeriodicSubscriptionCache(r)
		if err := msg.Ack(false); err != nil {
			logCtx.WithFields(log.Fields{
				"callback": msg.Body,
				"error":    err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}

	}
}
func (y *yondu) getRecordByCallback(req yondu_service.CallbackParameters) (rec.Record, error) {
	r := y.getRecByTransId(req.Params.TransID)
	if r.SubscriptionId == 0 {
		y.m.CallBackTransIdError.Inc()

		err := fmt.Errorf("record not found %s", req.Params.TransID)
		return rec.Record{}, err
	}
	sentAt, err := time.Parse("20060102150405", req.Params.Timestamp)
	if err != nil {
		y.m.CallBackParseTimeError.Inc()

		err = fmt.Errorf("time.Parse: %s", err.Error())
		log.WithFields(log.Fields{
			"error":     err.Error(),
			"timestamp": req.Params.Timestamp,
		}).Error("cannot parse callback time")
		sentAt = time.Now().UTC()
	}

	r.SentAt = sentAt
	return r, nil
}

// ============================================================
// metrics
type YonduMetrics struct {
	MODropped                m.Gauge
	MOUnknownCampaign        m.Gauge
	MOUnknownService         m.Gauge
	MOUnknownPublisher       m.Gauge
	MOParseTimeError         m.Gauge
	CallBackDropped          m.Gauge
	CallBackParseTimeError   m.Gauge
	CallBackTransIdError     m.Gauge
	AddToDBErrors            m.Gauge
	AddToDbSuccess           m.Gauge
	SinceLastSuccessPay      prometheus.Gauge
	GetPeriodicsDuration     prometheus.Summary
	ProcessPeriodicsDuration prometheus.Summary
}

func (y *yondu) initMetrics() {
	telcoName := "yondu"
	ym := &YonduMetrics{
		MODropped:                m.NewGauge(appName, telcoName, "mo_dropped", "yondu mo dropped"),
		MOUnknownCampaign:        m.NewGauge(appName, telcoName, "mo_unknown_campaign", "yondu MO unknown campaign"),
		MOUnknownService:         m.NewGauge(appName, telcoName, "mo_unknown_service", "yondu MO unknown service"),
		MOUnknownPublisher:       m.NewGauge(appName, telcoName, "mo_unknown_pixel_setting", "yondu MO unknown pixel setting"),
		MOParseTimeError:         m.NewGauge(appName, telcoName, "mo_parse_time_error", "yondu MO parse operators time error"),
		CallBackDropped:          m.NewGauge(appName, telcoName, "callback_dropped", "yondu callback dropped"),
		CallBackParseTimeError:   m.NewGauge(appName, telcoName, "callback_parse_time_error", "yondu callback parse operators time error"),
		CallBackTransIdError:     m.NewGauge(appName, telcoName, "callback_transid_error", "yondu callback cannot get record by transid"),
		AddToDBErrors:            m.NewGauge(appName, telcoName, "add_to_db_errors", "subscription add to db errors"),
		AddToDbSuccess:           m.NewGauge(appName, telcoName, "add_to_db_success", "subscription add to db success"),
		SinceLastSuccessPay:      m.PrometheusGauge(appName, y.conf.OperatorName, "since_last_success_pay_seconds", "seconds since success pay"),
		GetPeriodicsDuration:     m.NewSummary("get_periodics_duration_seconds", "get periodics duration seconds"),
		ProcessPeriodicsDuration: m.NewSummary("process_periodics_duration_seconds", "process periodics duration seconds"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			ym.MODropped.Update()
			ym.MOUnknownCampaign.Update()
			ym.MOUnknownService.Update()
			ym.MOUnknownPublisher.Update()
			ym.MOParseTimeError.Update()
			ym.CallBackDropped.Update()
			ym.CallBackParseTimeError.Update()
			ym.CallBackTransIdError.Update()
			ym.AddToDBErrors.Update()
			ym.AddToDbSuccess.Update()
		}
	}()

	go func() {
		for range time.Tick(time.Second) {
			ym.SinceLastSuccessPay.Inc()
		}
	}()

	y.m = ym
}

// ============================================================
// notifier functions
func (y *yondu) publishSentConsent(r rec.Record) error {
	r.SentAt = time.Now().UTC()
	event := amqp.EventNotify{
		EventName: "sent_consent",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		NotifyErrors.Inc()

		return fmt.Errorf("json.Marshal: %s", err.Error())
	}

	svc.notifier.Publish(amqp.AMQPMessage{y.conf.SentConsent, 0, body})
	return nil
}
func (y *yondu) publishMT(r rec.Record) error {
	r.SentAt = time.Now().UTC()
	event := amqp.EventNotify{
		EventName: "mt",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		NotifyErrors.Inc()

		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	svc.notifier.Publish(amqp.AMQPMessage{y.conf.MT, 0, body})
	return nil
}
func (y *yondu) publishCharge(priority uint8, r rec.Record) error {
	r.SentAt = time.Now().UTC()
	event := amqp.EventNotify{
		EventName: "charge",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		NotifyErrors.Inc()

		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	svc.notifier.Publish(amqp.AMQPMessage{y.conf.Charge, priority, body})
	return nil
}

// ============================================================
// go-cache inmemory cache for incoming mo subscriptions
func (y *yondu) initPrevSubscriptionsCache() {
	prev, err := rec.LoadPreviousSubscriptions(y.conf.OperatorCode)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load previous subscriptions")
	}
	log.WithField("count", len(prev)).Debug("loaded previous subscriptions")
	y.prevCache = cache.New(24*time.Hour, time.Minute)
	for _, v := range prev {
		key := v.Msisdn + strconv.FormatInt(v.ServiceId, 10)
		y.prevCache.Set(key, struct{}{}, time.Now().Sub(v.CreatedAt))
	}
}
func (y *yondu) getPrevSubscriptionCache(r rec.Record) bool {
	key := r.Msisdn + strconv.FormatInt(r.ServiceId, 10)
	_, found := y.prevCache.Get(key)
	log.WithFields(log.Fields{
		"tid":   r.Tid,
		"key":   key,
		"found": found,
	}).Debug("get previous subscription cache")
	return found
}
func (y *yondu) setPrevSubscriptionCache(r rec.Record) {
	key := r.Msisdn + strconv.FormatInt(r.ServiceId, 10)
	_, found := y.prevCache.Get(key)
	if !found {
		y.prevCache.Set(key, struct{}{}, 24*time.Hour)
		log.WithFields(log.Fields{
			"tid": r.Tid,
			"key": key,
		}).Debug("set previous subscription cache")
	}
}

const ldbPeriodicKey = "yondu_periodic_"
const ldbTransIdRecordKey = "yondu_transid_"

// ============================================================
// leveldb cache for pending periodic subscription:
// get ids and do not initiate new charge if id is in this list
func (y *yondu) setPendingPeriodicSubscriptionCache(r rec.Record) {
	if r.SubscriptionId == 0 {
		log.WithFields(log.Fields{
			"transid": r.OperatorToken,
			"tid":     r.Tid,
			"msisdn":  r.Msisdn,
			"error":   "no subscription id found",
		}).Fatal("set pending subscription cache failed")
	}
	key := []byte(ldbPeriodicKey + strconv.FormatInt(r.SubscriptionId, 10))
	err := svc.ldb.Put(key, []byte(fmt.Sprintf("%d", r.SubscriptionId)), nil)
	if err != nil {
		log.WithFields(log.Fields{
			"transid": r.OperatorToken,
			"tid":     r.Tid,
			"msisdn":  r.Msisdn,
			"error":   err.Error(),
		}).Fatal("set pending subscription cache failed")
	}
	log.WithFields(log.Fields{
		"transid": r.OperatorToken,
		"tid":     r.Tid,
		"key":     string(key),
	}).Debug("set pending subscription cache")
}
func (y *yondu) deletePendingPeriodicSubscriptionCache(r rec.Record) {
	key := []byte(ldbPeriodicKey + strconv.FormatInt(r.SubscriptionId, 10))
	err := svc.ldb.Delete(key, nil)
	if err != nil {
		log.WithFields(log.Fields{
			"transid": r.OperatorToken,
			"tid":     r.Tid,
			"msisdn":  r.Msisdn,
			"error":   err.Error(),
		}).Fatal("delete pending subscription cache")
	}
	log.WithFields(log.Fields{
		"subscriptionId": r.SubscriptionId,
		"tid":            r.Tid,
		"key":            string(key),
	}).Debug("deleted pending subscription cache")
}
func (y *yondu) getPendingPeriodicSubscriptionIds() (ids []int64, err error) {
	iter := svc.ldb.NewIterator(util.BytesPrefix([]byte(ldbPeriodicKey)), nil)
	for iter.Next() {
		value := iter.Value()
		var id int64
		id, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"value": value,
			}).Error("parse periodics id")
			continue
		}
		ids = append(ids, id)
	}
	iter.Release()
	if err = iter.Error(); err != nil {
		err = fmt.Errorf("iter.Error: %s", err.Error())
		return
	}
	return
}

// ============================================================
// leveldb cache: when callback arrives, we could get the record by callback transid parameter
// if no record found in leveldb, then we search in database
func (y *yondu) getRecByTransId(operatorToken string) rec.Record {
	var r rec.Record
	if operatorToken == "" || len(operatorToken) < 17 {
		log.WithFields(log.Fields{
			"token": operatorToken,
			"error": "token is empty",
		}).Error("cannot get transaction id cache")
		return r
	}

	// operatorToken == 2910KRE9055209652148403127752002
	// we use 2910KRE9055209652, without timestamp and their code
	key := []byte(ldbTransIdRecordKey + operatorToken[:17])
	recordJson, err := svc.ldb.Get(key, nil)
	if err == leveldb.ErrNotFound {
		log.WithFields(log.Fields{
			"transid": operatorToken,
		}).Debug("record not in transaction id cache")
		r, err := rec.GetPeriodicSubscriptionByToken(operatorToken)
		if err != nil || r.Tid == "" {
			fields := log.Fields{
				"transid": operatorToken,
				"key":     string(key),
			}
			if err != nil {
				fields["error"] = err.Error()
			}
			log.WithFields(fields).Error("cannot find record by transId")
			return rec.Record{}
		}
		log.WithFields(log.Fields{
			"tid":     r.Tid,
			"msisdn":  r.Msisdn,
			"transid": operatorToken,
		}).Debug("got transaction id rec from db")
		y.setRecCache(r)
		return r
	}
	if err != nil {
		log.WithFields(log.Fields{
			"transid": operatorToken,
			"key":     string(key),
			"error":   err.Error(),
		}).Error("cannot transaction id rec from cache")
		return rec.Record{}
	}
	if err := json.Unmarshal(recordJson, &r); err != nil {
		err = fmt.Errorf("json.Unmarshal: %s", err.Error())
		log.WithFields(log.Fields{
			"transid": operatorToken,
			"error":   err.Error(),
		}).Error("cannot unmarshal record from transid cache")
		return rec.Record{}
	}
	log.WithFields(log.Fields{
		"tid":     r.Tid,
		"msisdn":  r.Msisdn,
		"transid": operatorToken,
		"key":     string(key),
	}).Debug("got transaction id rec from ldb")
	return r
}

func (y *yondu) setRecCache(r rec.Record) {
	if r.OperatorToken == "" {
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"error":  "operator token is empty",
		}).Error("cannot set transaction id cache")
		return
	}
	recStr, err := json.Marshal(r)
	if err != nil {
		log.WithFields(log.Fields{
			"transid": r.OperatorToken,
			"tid":     r.Tid,
			"msisdn":  r.Msisdn,
			"error":   err.Error(),
		}).Error("cannot marshal")
		return
	}
	key := []byte(ldbTransIdRecordKey + y.transId(r))
	err = svc.ldb.Put(key, recStr, nil)
	if err != nil {
		log.WithFields(log.Fields{
			"transid": r.OperatorToken,
			"tid":     r.Tid,
			"msisdn":  r.Msisdn,
			"key":     string(key),
			"error":   err.Error(),
		}).Error("cannot set transaction id cache")
		return
	}
	log.WithFields(log.Fields{
		"transid": r.OperatorToken,
		"tid":     r.Tid,
		"token":   r.OperatorToken,
		"key":     string(key),
	}).Debug("set transaction id cache")
}
func (y *yondu) transId(r rec.Record) string {
	token := strings.Replace(r.OperatorToken, "DMP", "KRE", 1)
	return token[:7] + r.Msisdn[2:]
}
func (y *yondu) deleteRecCache(r rec.Record) {
	if r.OperatorToken == "" {
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"error":  "operator token is empty",
		}).Error("cannot delete transaction id cache")
		return
	}

	key := []byte(ldbTransIdRecordKey + y.transId(r))
	err := svc.ldb.Delete(key, nil)
	if err != nil {
		log.WithFields(log.Fields{
			"transid": r.OperatorToken,
			"tid":     r.Tid,
			"msisdn":  r.Msisdn,
			"error":   err.Error(),
			"key":     string(key),
		}).Fatal("cannot delete record cache")
	}
	log.WithFields(log.Fields{
		"transid": r.OperatorToken,
		"tid":     r.Tid,
		"msisdn":  r.Msisdn,
		"key":     string(key),
	}).Debug("deleted by transaction id cache")
}
