package service

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics/prometheus"
	cache "github.com/patrickmn/go-cache"
	amqp_driver "github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	yondu_service "github.com/vostrok/operator/ph/yondu/src/service"
	transaction_log_service "github.com/vostrok/qlistener/src/service"
	"github.com/vostrok/utils/amqp"
	queue_config "github.com/vostrok/utils/config"
	m "github.com/vostrok/utils/metrics"
	rec "github.com/vostrok/utils/rec"
)

type yondu struct {
	conf              YonduConfig
	m                 *YonduMetrics
	transactionsCache *cache.Cache
	prevCache         *cache.Cache
	retriesWg         *sync.WaitGroup
	MOCh              <-chan amqp_driver.Delivery
	MOConsumer        *amqp.Consumer
	CallBackCh        <-chan amqp_driver.Delivery
	CallBackConsumer  *amqp.Consumer
}

type YonduConfig struct {
	Enabled         bool                            `yaml:"enabled" default:"false"`
	OperatorName    string                          `yaml:"operator_name" default:"yondu"`
	OperatorCode    int                             `yaml:"operator_code" default:"51500"`
	Periodic        PeriodicConfig                  `yaml:"periodic" `
	NewSubscription queue_config.ConsumeQueueConfig `yaml:"new"`
	SentConsent     string                          `yaml:"sent_consent"`
	MT              string                          `yaml:"mt"`
	Charge          string                          `yaml:"charge"`
	CallBack        queue_config.ConsumeQueueConfig `yaml:"callBack"`
}

type PeriodicConfig struct {
	Enabled    bool `yaml:"periodic" default:"false"`
	FetchLimit int  `yaml:"fetch_limit" default:"500"`
}

func initYondu(yConf YonduConfig, consumerConfig amqp.ConsumerConfig) *yondu {
	if !yConf.Enabled {
		return
	}
	y := &yondu{
		conf:              yConf,
		transactionsCache: cache.New(24*time.Hour, time.Minute),
	}
	y.initPrevSubscriptionsCache()

	if yConf.NewSubscription.Enabled {
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
			y.MOConsumer,
			y.processNewSubscription,
			yConf.NewSubscription.ThreadsCount,
			yConf.NewSubscription.Name,
			yConf.NewSubscription.Name,
		)
	}

	if yConf.CallBack.Enabled {
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
			y.CallBackConsumer,
			y.processCallBack,
			yConf.CallBack.ThreadsCount,
			yConf.CallBack.Name,
			yConf.CallBack.Name,
		)
	}

	go func() {
		for {
			time.Sleep(time.Second)
			if yConf.Periodic.Enabled {
				y.processPeriodic()
			}
		}
	}()

	return y
}

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

		log.WithFields(log.Fields{
			"priority": msg.Priority,
			"body":     string(msg.Body),
		}).Debug("start process")

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
		err = checkMO(&r, y.getPrevSubscriptionCache, y.setPrevSubscriptionCache)
		if err != nil {
			msg.Nack(false, true)
			continue
		}
		if err := rec.AddNewSubscriptionToDB(&r); err != nil {
			y.m.AddToDBErrors.Inc()
			msg.Nack(false, true)
			continue
		} else {
			y.m.AddToDbSuccess.Inc()
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
			RequestBody:      string(msg.Body),
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
	campaign, err := inmem_client.GetCampaignByKeyWord(req.KeyWord)
	if err != nil {
		y.m.MOUnknownCampaign.Inc()

		err = fmt.Errorf("inmem_client.GetCampaignByKeyWord: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword": req.KeyWord,
			"error":   err.Error(),
		}).Error("cannot get campaign by keyword")
		return r, err
	}
	svc, err := inmem_client.GetServiceById(campaign.ServiceId)
	if err != nil {
		y.m.MOUnknownService.Inc()

		err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword":    req.KeyWord,
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
			"keyword":    req.KeyWord,
			"serviceId":  campaign.ServiceId,
			"campaignId": campaign.Id,
			"error":      err.Error(),
		}).Error("cannot get pixel setting by campaign id")
	} else {
		publisher = pixelSetting.Publisher
	}

	sentAt, err := time.Parse("20060102150405", req.Timestamp)
	if err != nil {
		y.m.MOParseTimeError.Inc()

		err = fmt.Errorf("time.Parse: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword":    req.KeyWord,
			"serviceId":  campaign.ServiceId,
			"campaignId": campaign.Id,
			"error":      err.Error(),
		}).Error("cannot parse operators time")
		sentAt = time.Now().UTC()
	}
	r = rec.Record{
		SentAt:                   sentAt,
		Msisdn:                   req.Msisdn,
		Tid:                      rec.GenerateTID(),
		SubscriptionStatus:       "",
		CountryCode:              515,
		OperatorCode:             51500,
		Publisher:                publisher,
		Pixel:                    "",
		CampaignId:               campaign.Id,
		ServiceId:                campaign.ServiceId,
		DelayHours:               svc.DelayHours,
		PaidHours:                svc.PaidHours,
		KeepDays:                 svc.KeepDays,
		Price:                    100 * int(svc.Price),
		OperatorToken:            req.TransID,
		Periodic:                 true,
		PeriodicDays:             svc.PeriodicDays,
		PeriodicAllowedToHours:   svc.PeriodicAllowedFrom,
		PeriodicAllowedFromHours: svc.PeriodicAllowedTo,
	}
	return r, nil
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
	periodics, err := rec.GetPeriodics(
		y.conf.OperatorCode,
		y.conf.Periodic.FetchLimit,
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
	for _, p := range periodics {
		logCtx := log.WithFields(log.Fields{
			"tid":    p.Tid,
			"msisdn": p.Msisdn,
		})

		service, err := inmem_client.GetServiceById(p.ServiceId)
		if err != nil {
			y.m.MOUnknownService.Inc()

			err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
			log.WithFields(log.Fields{
				"serviceId": p.ServiceId,
				"error":     err.Error(),
			}).Error("cannot get service by id")
			continue
		}
		y.setRecCache(p)
		// todo: get content via content service
		p.SMSText = fmt.Sprintf(service.SendContentTextTemplate, "")

		if err := y.publishMT(p); err != nil {
			logCtx.WithField("error", err.Error()).Error("publishYonduMT")
		}
		if err := y.publishYonduCharge(p); err != nil {
			logCtx.WithField("error", err.Error()).Error("publishYonduCharge")
		}
		if err := rec.SetSubscriptionStatus("pending", p.SubscriptionId); err != nil {
			logCtx.WithField("error", err.Error()).Error("SetSubscriptionStatus")
		}
	}
	y.m.ProcessPeriodicsDuration.Observe(time.Since(begin).Seconds())
}

type EventNotifyCallBack struct {
	EventName string                           `json:"event_name,omitempty"`
	EventData yondu_service.CallbackParameters `json:"event_data,omitempty"`
}

func (y *yondu) processCallBack(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var r rec.Record
		var err error
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
			}).Error("consume from " + y.conf.CallBack.Name)
			goto ack
		}
		r, err = y.getRecordByCallback(e.EventData)
		if err != nil {
			goto ack
		}

		if e.EventData.StatusCode == 0 {
			r.SubscriptionStatus = "paid"
		} else {
			r.SubscriptionStatus = "failed"
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
			RequestBody:      string(msg.Body),
			ResponseBody:     "",
			ResponseDecision: r.SubscriptionStatus,
			ResponseCode:     e.EventData.StatusCode,
			SentAt:           r.SentAt,
			Type:             e.EventName,
		}
		if err := publishTransactionLog("callback", transactionMsg); err != nil {
			log.WithFields(log.Fields{
				"event": e.EventName,
				"mo":    msg.Body,
				"error": err.Error(),
			}).Error("sent to transaction log failed")
			msg.Nack(false, true)
			continue
		} else {
			log.WithFields(log.Fields{
				"queue": y.conf.CallBack.Name,
				"event": e.EventName,
				"tid":   r.Tid,
			}).Info("success (sent to transaction log)")
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
func (y *yondu) getRecordByCallback(req yondu_service.CallbackParameters) (rec.Record, error) {
	r := y.getRecByTransId(req.TransID)
	if r.SubscriptionId == 0 {
		y.m.CallBackTransIdError.Inc()

		err := fmt.Errorf("record not found %s", req.TransID)
		return err
	}
	sentAt, err := time.Parse("20060102150405", req.Timestamp)
	if err != nil {
		y.m.CallBackParseTimeError.Inc()

		err = fmt.Errorf("time.Parse: %s", err.Error())
		log.WithFields(log.Fields{
			"error":     err.Error(),
			"timestamp": req.Timestamp,
		}).Error("cannot parse callback time")
		sentAt = time.Now().UTC()
	}

	r.SentAt = sentAt
	return r, nil
}
func (y *yondu) getRecByTransId(transId string) rec.Record {
	record, found := y.transactionsCache.Get(transId)
	if !found {
		log.WithFields(log.Fields{
			"transid": transId,
		}).Debug("cannot get record by transaction id in cache")

		r, err := rec.GetPeriodicSubscriptionByToken(transId)
		if err != nil {
			log.WithFields(log.Fields{
				"transid": transId,
				"error":   err.Error(),
			}).Error("cannot find record by transaction id")
			return rec.Record{}
		}
		y.setRecCache(r)
		return r
	}
	if r, ok := record.(rec.Record); ok {
		log.WithFields(log.Fields{
			"transid": transId,
			"found":   found,
		}).Debug("get record from cache")
		return r
	}
	return rec.Record{}
}
func (y *yondu) setRecCache(r rec.Record) {
	y.transactionsCache.Set(r.OperatorToken, r, 24*time.Hour)
	log.WithFields(log.Fields{
		"transid": r.OperatorToken,
		"tid":     r.Tid,
	}).Debug("set record cache")
}
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
	GetPeriodicsDuration     prometheus.Summary
	ProcessPeriodicsDuration prometheus.Summary
}

func (y *yondu) initYonduMetrics() {
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

	y.m = ym
}

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
func (y *yondu) publishYonduCharge(r rec.Record) error {
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
	svc.notifier.Publish(amqp.AMQPMessage{y.conf.Charge, 0, body})
	return nil
}
