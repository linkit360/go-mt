package service

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	amqp_driver "github.com/streadway/amqp"

	"database/sql"
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
	conf                   YonduConfig
	m                      *YonduMetrics
	loc                    *time.Location
	activeSubscriptions    *activeSubscriptions
	shedulledSubscriptions *sync.WaitGroup
	MOCh                   <-chan amqp_driver.Delivery
	MOConsumer             *amqp.Consumer
	CallBackCh             <-chan amqp_driver.Delivery
	CallBackConsumer       *amqp.Consumer
}

type YonduConfig struct {
	Enabled            bool                            `yaml:"enabled" default:"false"`
	OperatorName       string                          `yaml:"operator_name" default:"yondu"`
	OperatorCode       int64                           `yaml:"operator_code" default:"51501"`
	CountryCode        int64                           `yaml:"country_code" default:"515"`
	Location           string                          `yaml:"location"`
	ChargeOnRejected   bool                            `yaml:"charge_on_rejected" default:"false"`
	Content            ContentConfig                   `yaml:"content"`
	Texts              TextsConfig                     `yaml:"texts"`
	Periodic           PeriodicConfig                  `yaml:"periodic" `
	Retries            RetriesConfig                   `yaml:"retries"`
	PeriodicsCharge    SubscriptionChargeConfig        `yaml:"subscription"`
	Consent            ConsentConfig                   `yaml:"consent"`
	MT                 string                          `yaml:"mt"`
	Charge             string                          `yaml:"charge"`
	NewSubscription    queue_config.ConsumeQueueConfig `yaml:"new"`
	CallBack           queue_config.ConsumeQueueConfig `yaml:"callback"`
	UnsubscribeMarkers []string                        `yaml:"unsubscribe"`
}
type ConsentConfig struct {
	Enabled bool                `yaml:"enabled"`
	Queue   string              `yaml:"sent_consent"`
	Repeat  RepeatConsentConfig `yaml:"repeat"`
}

type RepeatConsentConfig struct {
	Enabled      bool `yaml:"enabled"`
	DelayMinites int  `yaml:"delay_minutes"`
	FetchLimit   int  `yaml:"fetch_limit" default:"500"`
}
type SubscriptionChargeConfig struct {
	Enabled      bool  `yaml:"enabled"`
	DelayMinutes int   `yaml:"delay_minutes"`
	FetchLimit   int   `yaml:"fetch_limit"`
	Priority     uint8 `yaml:"priority"`
}
type ContentConfig struct {
	SendEnabled bool   `yaml:"enabled"`
	Url         string `yaml:"url"`
}
type TextsConfig struct {
	Rejected       string `yaml:"rejected" default:"You already subscribed on this service"`
	RejectedCharge string `yaml:"rejected_charge" default:"You already subscribed on this service. Please find more games here: %s"`
	BlackListed    string `yaml:"blacklisted" default:"Sorry, service not available"`
	PostPaid       string `yaml:"postpaid" default:"Sorry, service not available"`
	Unsubscribe    string `yaml:"unsubscribe" default:"You have been unsubscribed"`
}

type PeriodicConfig struct {
	Enabled               bool   `yaml:"enabled" default:"false"`
	Period                int    `yaml:"period" default:"600"`
	IntervalType          string `yaml:"interval_type" default:"hour"`
	FailedRepeatInMinutes int    `yaml:"failed_repeat_in_minutes" default:"60"`
	FetchLimit            int    `yaml:"fetch_limit" default:"500"`
}

func initYondu(yConf YonduConfig, consumerConfig amqp.ConsumerConfig, contentConf content_client.RPCClientConfig) *yondu {
	if !yConf.Enabled {
		return nil
	}
	y := &yondu{
		conf: yConf,
		shedulledSubscriptions: &sync.WaitGroup{},
	}
	y.initMetrics()
	y.initActiveSubscriptionsCache()

	var err error
	y.loc, err = time.LoadLocation(yConf.Location)
	if err != nil {
		log.WithFields(log.Fields{
			"location": yConf.Location,
			"error":    err,
		}).Fatal("location")
	}

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
			y.processMO,
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
				time.Sleep(time.Duration(yConf.Periodic.Period) * time.Second)
				y.processPeriodic()
			}
		}()
	} else {
		log.Debug("periodic disabled")
	}

	if yConf.Consent.Enabled && yConf.Consent.Repeat.Enabled {
		go func() {
			for range time.Tick(time.Second) {
				y.repeatSentConsent()
			}
		}()
	} else {
		log.Debug("repeat consent disabled")
	}

	go func() {
		if yConf.PeriodicsCharge.Enabled {
			for range time.Tick(time.Second) {
				begin := time.Now()
				y.processChargeShedulledSubscriptions()
				y.m.ProcessShedulledPeriodicsChargeDuration.Observe(time.Since(begin).Seconds())
			}
		}
	}()

	go func() {
		if yConf.Retries.Enabled {
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
				ProcessRetries(
					yConf.OperatorCode,
					yConf.Retries.FetchLimit,
					yConf.Retries.PaidOnceHours,
					y.publishCharge,
				)
			}
		}
	}()
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
	periodics, err := rec.GetPeriodics(
		y.conf.Periodic.FetchLimit,
		y.conf.Periodic.FailedRepeatInMinutes,
		y.conf.Periodic.IntervalType,
		y.loc,
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

		if err := y.sentContent(r); err != nil {
			Errors.Inc()
		}

		if err := y.charge(r, 1); err != nil {
			Errors.Inc()
		}

		begin := time.Now()
		if err = rec.SetSubscriptionStatus("pending", r.SubscriptionId); err != nil {
			return
		}
		SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())
	}
	y.m.ProcessPeriodicsDuration.Observe(time.Since(begin).Seconds())
}

func (y *yondu) charge(r rec.Record, priority uint8) (err error) {
	logCtx := log.WithFields(log.Fields{
		"tid": r.Tid,
	})
	if err = y.publishCharge(priority, r); err != nil {
		logCtx.WithField("error", err.Error()).Error("publishYonduCharge")
		return
	}
	logCtx.WithFields(log.Fields{
		"amount":   r.Price,
		"priority": priority,
	}).Info("charge")
	return
}

func (y *yondu) sentContent(r rec.Record) (err error) {
	if y.conf.Content.SendEnabled {
		log.Info("send content disabled")
		return nil
	}
	logCtx := log.WithFields(log.Fields{
		"tid": r.Tid,
	})
	service, err := inmem_client.GetServiceById(r.ServiceId)
	if err != nil {
		y.m.MOUnknownService.Inc()

		err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"serviceId": r.ServiceId,
			"error":     err.Error(),
		}).Error("cannot get service by id")
		return
	}
	contentProperties, err := content_client.GetUniqueUrl(content_service.GetContentParams{
		Msisdn:         r.Msisdn,
		Tid:            r.Tid,
		ServiceId:      r.ServiceId,
		CampaignId:     r.CampaignId,
		OperatorCode:   r.OperatorCode,
		CountryCode:    r.CountryCode,
		SubscriptionId: r.SubscriptionId,
	})

	if contentProperties.Error != "" {
		ContentdRPCDialError.Inc()
		err = fmt.Errorf("content_client.GetUniqueUrl: %s", contentProperties.Error)
		logCtx.WithFields(log.Fields{
			"serviceId": r.ServiceId,
			"error":     err.Error(),
		}).Error("contentd internal error")
		return
	}
	if err != nil {
		ContentdRPCDialError.Inc()
		err = fmt.Errorf("content_client.GetUniqueUrl: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"serviceId": r.ServiceId,
			"error":     err.Error(),
		}).Error("cannot get unique content url")
		return
	}

	url := y.conf.Content.Url + contentProperties.UniqueUrl
	r.SMSText = fmt.Sprintf(service.SendContentTextTemplate, url)
	if err = y.publishMT(r); err != nil {
		logCtx.WithField("error", err.Error()).Error("publishYonduMT")
		return
	}
	logCtx.WithFields(log.Fields{
		"text": r.SMSText,
	}).Info("send text")
	return
}

// ============================================================
// new subscritpion
type YonduEventNotifyMO struct {
	EventName string                     `json:"event_name,omitempty"`
	EventData yondu_service.MOParameters `json:"event_data,omitempty"`
}

func (y *yondu) processMO(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var r rec.Record
		var err error
		var logCtx *log.Entry
		var transactionMsg transaction_log_service.OperatorTransactionLog

		var e YonduEventNotifyMO
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
		transactionMsg = transaction_log_service.OperatorTransactionLog{
			Tid:              e.EventData.Tid,
			Msisdn:           e.EventData.Params.Msisdn,
			OperatorToken:    e.EventData.Params.TransID, // important, do not use from record - it's changed
			OperatorCode:     y.conf.OperatorCode,
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
		if err = publishTransactionLog("mo", transactionMsg); err != nil {
			Errors.Inc()
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"data":  fmt.Sprintf("%#v", transactionMsg),
			}).Error("publishTransactionLog")
		}
		for _, marker := range y.conf.UnsubscribeMarkers {
			if strings.Contains(e.EventData.Params.KeyWord, marker) {
				if err := unsubscribe(r); err != nil {
					msg.Nack(false, true)
					continue
				}
				r.SMSText = y.conf.Texts.Unsubscribe
				if err := y.publishMT(r); err != nil {
					logCtx.WithField("error", err.Error()).Error("publishYonduMT")
				}
				y.deleteActiveSubscriptionCache(r)
				goto ack
			}
		}

		// in check MO func we have to have subscription id
		if err := rec.AddNewSubscriptionToDB(&r); err != nil {
			Errors.Inc()
			y.m.AddToDBErrors.Inc()
			msg.Nack(false, true)
			continue
		} else {
			y.m.AddToDbSuccess.Inc()
		}

		err = checkMO(&r, y.getActiveSubscriptionCache, y.setActiveSubscriptionCache)
		if err != nil {
			msg.Nack(false, true)
			continue
		}
		if r.Result == "" {

			if err := y.sentContent(r); err != nil {
				Errors.Inc()
			}
			if y.conf.Consent.Enabled {
				if err := y.publishSentConsent(r); err != nil {
					Errors.Inc()
					logCtx.WithField("error", err.Error()).Error("publishYonduSentConsent")
				}
				begin := time.Now()
				if err := rec.SetSubscriptionStatus("consent", r.SubscriptionId); err != nil {
					return
				}
				SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())
			} else {
				if err := y.charge(r, 1); err != nil {
					Errors.Inc()
				}

				begin := time.Now()
				if err = rec.SetSubscriptionStatus("pending", r.SubscriptionId); err != nil {
					Errors.Inc()
				}
				SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())
			}
		} else {
			if r.Result == "rejected" {
				r.Periodic = false
				if y.conf.ChargeOnRejected {
					r.SubscriptionStatus = ""
					r.Periodic = false
					if err := writeSubscriptionPeriodic(r); err != nil {
						Errors.Inc()
					}
					if err := y.sentContent(r); err != nil {
						Errors.Inc()
					}
					if err := y.charge(r, 1); err != nil {
						Errors.Inc()
					}
					begin := time.Now()
					if err = rec.SetSubscriptionStatus("pending", r.SubscriptionId); err != nil {
						Errors.Inc()
					}
					SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())
					goto ack
				}
				r.SMSText = y.conf.Texts.Rejected
			}
			if r.Result == "blacklisted" {
				r.SMSText = y.conf.Texts.BlackListed
			}
			if r.Result == "postpaid" {
				r.SMSText = y.conf.Texts.PostPaid
			}
			if r.SMSText != "" {
				if err := y.publishMT(r); err != nil {
					logCtx.WithField("error", err.Error()).Error("publishYonduMT")
				}
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
		err = nil
	}
	r = rec.Record{
		SentAt:                   sentAt,
		Msisdn:                   req.Params.Msisdn,
		Tid:                      req.Tid,
		SubscriptionStatus:       "",
		CountryCode:              515,
		OperatorCode:             y.conf.OperatorCode,
		Publisher:                "",
		Pixel:                    "",
		CampaignId:               campaign.Id,
		ServiceId:                campaign.ServiceId,
		DelayHours:               svc.DelayHours,
		PaidHours:                svc.PaidHours,
		KeepDays:                 svc.KeepDays,
		Price:                    int(svc.Price),
		Periodic:                 true,
		PeriodicDays:             svc.PeriodicDays,
		PeriodicAllowedFromHours: svc.PeriodicAllowedFrom,
		PeriodicAllowedToHours:   svc.PeriodicAllowedTo,
		SMSText:                  "OK",
	}
	r.OperatorToken = y.transId(req.Params.TransID, req.Params.Msisdn)
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
		var begin time.Time
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
		logCtx = log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": e.EventData.Params.Msisdn,
		})

		transactionMsg = transaction_log_service.OperatorTransactionLog{
			Tid:              r.Tid,
			Msisdn:           e.EventData.Params.Msisdn,
			OperatorToken:    e.EventData.Params.TransID,
			OperatorCode:     y.conf.OperatorCode,
			CountryCode:      r.CountryCode,
			Error:            r.OperatorErr,
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
		if transErr := publishTransactionLog("callback", transactionMsg); transErr != nil {
			logCtx.WithFields(log.Fields{
				"event":    e.EventName,
				"callback": msg.Body,
				"error":    transErr.Error(),
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
		if err != nil {
			goto ack
		}

	processResponse:
		if err = processResponse(&r, y.conf.Retries.Enabled); err != nil {
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"sleep": 1,
			}).Error("process response error")
			time.Sleep(time.Second)
			goto processResponse
		}
		if r.Periodic {
			//
		}

		// is was pending
		begin = time.Now()
		if err := rec.SetSubscriptionStatus("", r.SubscriptionId); err != nil {
			Errors.Inc()
			err = fmt.Errorf("rec.SetSubscriptionStatus: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot set subscription status")
			return
		}
		SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())

	ack:
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
func (y *yondu) getRecordByCallback(req yondu_service.CallbackParameters) (r rec.Record, err error) {
	defer func() {
		if err != nil {
			r.OperatorErr = err.Error()
		}
		if r.SentAt.IsZero() {
			r.SentAt = time.Now().UTC()
		}
	}()

	logCtx := log.WithFields(log.Fields{
		"otid": req.Params.TransID,
	})
	if y.conf.Retries.Enabled {
		r, err = rec.GetRetryByMsisdn(req.Params.Msisdn, "pending")
		if err != nil && err != sql.ErrNoRows {
			err := fmt.Errorf("rec.GetRetryByMsisdn: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot get from retries")
			return rec.Record{}, err
		}
	}
	if err == sql.ErrNoRows || r.RetryId == 0 {
		r, err = rec.GetSubscriptionByToken(y.transId(req.Params.TransID, req.Params.Msisdn))
		if err != nil {
			err = fmt.Errorf("rec.GetPeriodicSubscriptionByToken: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot get from subscriptions")
			return rec.Record{}, err
		}
	}

	if r.SubscriptionId == 0 {
		y.m.CallBackTransIdError.Inc()

		err = fmt.Errorf("CallBack not found %s", req.Params.TransID)
		r.Tid = rec.GenerateTID()

		logCtx.WithFields(log.Fields{
			"tid":   r.Tid,
			"error": err.Error(),
		}).Error("cann't process")
		return r, err
	}
	if req.Params.StatusCode == "0" {
		r.Paid = true
		r.SubscriptionStatus = "paid" // for operator transaction log
	} else {
		r.SubscriptionStatus = "failed"
	}
	r.SentAt, err = time.Parse("20060102150405", req.Params.Timestamp)
	if err != nil {
		y.m.CallBackParseTimeError.Inc()

		log.WithFields(log.Fields{
			"tid":       r.Tid,
			"error":     err.Error(),
			"timestamp": req.Params.Timestamp,
		}).Error("cannot parse callback time")
		r.SentAt = time.Now().UTC()
		err = nil
	}
	return r, nil
}

// ============================================================
// metrics
type YonduMetrics struct {
	MODropped                               m.Gauge
	MOUnknownCampaign                       m.Gauge
	MOUnknownService                        m.Gauge
	MOParseTimeError                        m.Gauge
	CallBackDropped                         m.Gauge
	CallBackParseTimeError                  m.Gauge
	CallBackTransIdError                    m.Gauge
	AddToDBErrors                           m.Gauge
	AddToDbSuccess                          m.Gauge
	DelayMinitsArentPassed                  m.Gauge
	GetPeriodicsDuration                    prometheus.Summary
	ProcessPeriodicsDuration                prometheus.Summary
	ProcessShedulledPeriodicsChargeDuration prometheus.Summary
}

func (y *yondu) initMetrics() {
	telcoName := "yondu"
	ym := &YonduMetrics{
		MODropped:                               m.NewGauge(appName, telcoName, "mo_dropped", "yondu mo dropped"),
		MOUnknownCampaign:                       m.NewGauge(appName, telcoName, "mo_unknown_campaign", "yondu MO unknown campaign"),
		MOUnknownService:                        m.NewGauge(appName, telcoName, "mo_unknown_service", "yondu MO unknown service"),
		MOParseTimeError:                        m.NewGauge(appName, telcoName, "mo_parse_time_error", "yondu MO parse operators time error"),
		CallBackDropped:                         m.NewGauge(appName, telcoName, "callback_dropped", "yondu callback dropped"),
		CallBackParseTimeError:                  m.NewGauge(appName, telcoName, "callback_parse_time_error", "yondu callback parse operators time error"),
		CallBackTransIdError:                    m.NewGauge(appName, telcoName, "callback_transid_error", "yondu callback cannot get record by transid"),
		AddToDBErrors:                           m.NewGauge(appName, telcoName, "add_to_db_errors", "subscription add to db errors"),
		AddToDbSuccess:                          m.NewGauge(appName, telcoName, "add_to_db_success", "subscription add to db success"),
		DelayMinitsArentPassed:                  m.NewGauge(appName, telcoName, "delay_minutes_not_passed", "delay minutes not passed"),
		GetPeriodicsDuration:                    m.NewSummary("get_periodics_duration_seconds", "get periodics duration seconds"),
		ProcessPeriodicsDuration:                m.NewSummary("process_periodics_duration_seconds", "process periodics duration seconds"),
		ProcessShedulledPeriodicsChargeDuration: m.NewSummary("process_shedulled_periodics_charge_duration_seconds", "process shedulled periodics charge duration seconds"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			ym.MODropped.Update()
			ym.MOUnknownCampaign.Update()
			ym.MOUnknownService.Update()
			ym.MOParseTimeError.Update()
			ym.CallBackDropped.Update()
			ym.CallBackParseTimeError.Update()
			ym.CallBackTransIdError.Update()
			ym.AddToDBErrors.Update()
			ym.AddToDbSuccess.Update()
			ym.DelayMinitsArentPassed.Update()
		}
	}()

	y.m = ym
}

// ============================================================
// notifier functions
func (y *yondu) publishSentConsent(r rec.Record) error {
	if !y.conf.Consent.Enabled {
		return nil
	}
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

	svc.notifier.Publish(amqp.AMQPMessage{y.conf.Consent.Queue, 0, body})
	log.WithField("tid", r.Tid).Debug("sent consent")
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
// inmemory cache for incoming mo subscriptions
type activeSubscriptions struct {
	byKey map[string]int64
}

func (y *yondu) initActiveSubscriptionsCache() {
	// load all active subscriptions
	prev, err := rec.LoadActiveSubscriptions(0)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load active subscriptions")
	}
	y.activeSubscriptions = &activeSubscriptions{
		byKey: make(map[string]int64),
	}
	for _, v := range prev {
		key := v.Msisdn + strconv.FormatInt(v.CampaignId, 10)
		y.activeSubscriptions.byKey[key] = v.Id
	}
}
func (y *yondu) getActiveSubscriptionCache(r rec.Record) bool {
	key := r.Msisdn + strconv.FormatInt(r.CampaignId, 10)
	_, found := y.activeSubscriptions.byKey[key]
	log.WithFields(log.Fields{
		"tid":   r.Tid,
		"key":   key,
		"found": found,
	}).Debug("get active subscriptions cache")
	return found
}
func (y *yondu) setActiveSubscriptionCache(r rec.Record) {
	key := r.Msisdn + strconv.FormatInt(r.CampaignId, 10)
	if _, found := y.activeSubscriptions.byKey[key]; found {
		return
	}
	y.activeSubscriptions.byKey[key] = r.SubscriptionId
	log.WithFields(log.Fields{
		"tid": r.Tid,
		"key": key,
	}).Debug("set active subscriptions cache")
}

func (y *yondu) deleteActiveSubscriptionCache(r rec.Record) {
	key := r.Msisdn + strconv.FormatInt(r.CampaignId, 10)
	delete(y.activeSubscriptions.byKey, key)
	log.WithFields(log.Fields{
		"tid": r.Tid,
		"key": key,
	}).Debug("deleted from active subscriptions cache")
}

func (y *yondu) transId(operatorToken, msisdn string) string {
	token := strings.Replace(operatorToken, "DMP", "KRE", 1)
	return token[:7] + msisdn[2:]
}

// if it wasn't paid the day we send content,
// try to charge same day after some delay
func (y *yondu) processChargeShedulledSubscriptions() {
	periodics, err := rec.GetNotPaidPeriodics(
		y.conf.PeriodicsCharge.DelayMinutes,
		y.conf.PeriodicsCharge.FetchLimit,
	)
	if err != nil {
		return
	}
	if len(periodics) == 0 {
		return
	}
	for _, r := range periodics {
		y.shedulledSubscriptions.Add(1)
		go y.hanlePeriodic(r, y.publishCharge)
	}
	y.shedulledSubscriptions.Wait()
}

func (y *yondu) hanlePeriodic(r rec.Record, notifyFnSendChargeRequest func(uint8, rec.Record) error) {
	defer y.shedulledSubscriptions.Done()
	logCtx := log.WithFields(log.Fields{
		"tid":            r.Tid,
		"attempts_count": r.AttemptsCount,
	})

	sincePreviousAttempt := time.Now().Sub(r.LastPayAttemptAt).Minutes()
	delayMinutes := (time.Duration(y.conf.PeriodicsCharge.DelayMinutes) * time.Minute).Minutes()
	if sincePreviousAttempt < delayMinutes {
		y.m.DelayMinitsArentPassed.Inc()

		logCtx.WithFields(log.Fields{
			"prev":   r.LastPayAttemptAt,
			"now":    time.Now().UTC(),
			"passed": sincePreviousAttempt,
			"delay":  delayMinutes,
		}).Debug("delay minutes were not passed")
		return
	}
	logCtx.Debug("start processsing")
	if err := checkBlackListedPostpaid(&r); err != nil {
		err = fmt.Errorf("checkBlackListedPostpaid: %s", err.Error())
		return
	}
	begin := time.Now()
	if err := rec.SetSubscriptionStatus("pending", r.SubscriptionId); err != nil {
		return
	}
	SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())

	if err := notifyFnSendChargeRequest(y.conf.PeriodicsCharge.Priority, r); err != nil {
		Errors.Inc()
		NotifyErrors.Inc()

		err = fmt.Errorf("notifyFnSendChargeRequest: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cannot send to queue")
		return
	}
	logCtx.WithField("took", time.Since(begin).Seconds()).Debug("notify operator")
}

func (y *yondu) repeatSentConsent() {
	records, err := rec.GetRepeatSentConsent(
		y.conf.OperatorCode,
		y.conf.Consent.Repeat.DelayMinites,
		y.conf.Consent.Repeat.FetchLimit,
	)
	if err != nil {
		return
	}
	for _, r := range records {
		r.Notice = "sent consent repeat"
		if err := y.publishSentConsent(r); err != nil {
			Errors.Inc()
			log.WithFields(log.Fields{
				"tid":   r.Tid,
				"error": err.Error(),
			}).Error("publishYonduSentConsent")
			continue
		}
		begin := time.Now()
		if err := rec.SetSubscriptionStatus("consent", r.SubscriptionId); err != nil {
			return
		}
		SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())
	}
}
