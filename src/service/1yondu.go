package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	amqp_driver "github.com/streadway/amqp"

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
	conf                YonduConfig
	m                   *YonduMetrics
	loc                 *time.Location
	periodicSync        *sync.WaitGroup
	activeSubscriptions *activeSubscriptions
	MOCh                <-chan amqp_driver.Delivery
	MOConsumer          *amqp.Consumer
	DNCh                <-chan amqp_driver.Delivery
	DNConsumer          *amqp.Consumer
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
	Periodic           PeriodicConfig                  `yaml:"periodic"`
	Retries            RetriesConfig                   `yaml:"retries"`
	MT                 string                          `yaml:"mt"`
	MO                 queue_config.ConsumeQueueConfig `yaml:"mo"`
	DN                 queue_config.ConsumeQueueConfig `yaml:"dn"`
	UnsubscribeMarkers []string                        `yaml:"unsubscribe"`
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
		conf:         yConf,
		periodicSync: &sync.WaitGroup{},
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

	if yConf.MO.Enabled {
		if yConf.MO.Name == "" {
			log.Fatal("empty queue name mo")
		}
		y.MOConsumer = amqp.NewConsumer(
			consumerConfig,
			yConf.MO.Name,
			yConf.MO.PrefetchCount,
		)
		if err := y.MOConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			y.MOConsumer,
			y.MOCh,
			y.processMO,
			yConf.MO.ThreadsCount,
			yConf.MO.Name,
			yConf.MO.Name,
		)
	} else {
		log.Debug("mo disabled")
	}

	if yConf.DN.Enabled {
		if yConf.DN.Name == "" {
			log.Fatal("empty queue name dn")
		}
		y.DNConsumer = amqp.NewConsumer(
			consumerConfig,
			yConf.DN.Name,
			yConf.DN.PrefetchCount,
		)
		if err := y.DNConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			y.DNConsumer,
			y.DNCh,
			y.processDN,
			yConf.DN.ThreadsCount,
			yConf.DN.Name,
			yConf.DN.Name,
		)
	} else {
		log.Debug("dn disabled")
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
	return y
}

// get periodic for this day and time
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
		y.periodicSync.Add(1)
		y.hanlePeriodic(r, y.charge)
	}
	y.periodicSync.Wait()
	y.m.ProcessPeriodicsDuration.Observe(time.Since(begin).Seconds())
}

func (y *yondu) hanlePeriodic(r rec.Record, notifyFnSendChargeRequest func(rec.Record, uint8) error) {
	defer y.periodicSync.Done()
	logCtx := log.WithFields(log.Fields{
		"tid":            r.Tid,
		"attempts_count": r.AttemptsCount,
	})

	sincePreviousAttemptMinutes := time.Now().Sub(r.LastPayAttemptAt).Minutes()
	delayMinutes := (time.Duration(y.conf.Periodic.FailedRepeatInMinutes) * time.Minute).Minutes()
	if sincePreviousAttemptMinutes < delayMinutes {
		y.m.DelayMinitsArentPassed.Inc()

		logCtx.WithFields(log.Fields{
			"prev":   r.LastPayAttemptAt,
			"now":    time.Now().UTC(),
			"passed": sincePreviousAttemptMinutes,
			"delay":  delayMinutes,
		}).Debug("delay minutes were not passed")
		return
	}
	logCtx.Debug("start processsing")
	if err := checkBlackListedPostpaid(&r); err != nil {
		err = fmt.Errorf("checkBlackListedPostpaid: %s", err.Error())
		return
	}
	if err := notifyFnSendChargeRequest(r, 1); err != nil {
		Errors.Inc()
		NotifyErrors.Inc()

		err = fmt.Errorf("notifyFnSendChargeRequest: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cannot send to queue")
		return
	}
}

func (y *yondu) charge(r rec.Record, priority uint8) (err error) {
	logCtx := log.WithFields(log.Fields{
		"tid": r.Tid,
	})
	if err = y.publishCharge(priority, r); err != nil {
		logCtx.WithField("error", err.Error()).Error("publishYonduCharge")
		return
	}

	begin := time.Now()
	if err = rec.SetSubscriptionStatus("pending", r.SubscriptionId); err != nil {
		return
	}
	SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())

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
		logCtx.WithField("error", err.Error()).Error("send content")
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
			}).Error("consume from " + y.conf.MO.Name)
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
			OperatorToken:    e.EventData.Params.RRN, // important, do not use from record - it's changed
			OperatorCode:     y.conf.OperatorCode,
			CountryCode:      r.CountryCode,
			Error:            r.OperatorErr,
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
			if strings.Contains(e.EventData.Params.Message, marker) {
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
			if err := y.charge(r, 1); err != nil {
				Errors.Inc()
			}
		} else if r.Result == "rejected" {
			r.Periodic = false
			if y.conf.ChargeOnRejected {
				// todo: add retries for rejected simple
				r.SubscriptionStatus = ""
				r.Periodic = false
				if err := writeSubscriptionPeriodic(r); err != nil {
					Errors.Inc()
				}
				if err := y.charge(r, 1); err != nil {
					Errors.Inc()
				}
				goto ack
			}
			r.SMSText = y.conf.Texts.Rejected
		} else if r.Result == "blacklisted" {
			r.SMSText = y.conf.Texts.BlackListed
			if err := y.publishMT(r); err != nil {
				Errors.Inc()
				logCtx.WithField("error", err.Error()).Error("inform blacklisted failed")
			}
		} else if r.Result == "postpaid" {
			r.SMSText = y.conf.Texts.PostPaid
			if err := y.publishMT(r); err != nil {
				Errors.Inc()
				logCtx.WithField("error", err.Error()).Error("inform postpaid failed")
			}
		} else if r.SMSText != "" {
			Errors.Inc()
			logCtx.WithField("result", r.Result).Error("unknown result")
			goto ack
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
	campaign, err := inmem_client.GetCampaignByKeyWord(req.Params.Message)
	if err != nil {
		y.m.MOUnknownCampaign.Inc()

		err = fmt.Errorf("inmem_client.GetCampaignByKeyWord: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword": req.Params.Message,
			"error":   err.Error(),
		}).Error("cannot get campaign by keyword")
		return r, err
	}
	svc, err := inmem_client.GetServiceById(campaign.ServiceId)
	if err != nil {
		y.m.MOUnknownService.Inc()

		err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
		log.WithFields(log.Fields{
			"message":    req.Params.Message,
			"serviceId":  campaign.ServiceId,
			"campaignId": campaign.Id,
			"error":      err.Error(),
		}).Error("cannot get service by id")
		return r, err
	}

	r = rec.Record{
		SentAt:                   req.ReceivedAt,
		Msisdn:                   req.Params.Msisdn,
		Tid:                      req.Tid,
		SubscriptionStatus:       "",
		CountryCode:              515,
		OperatorCode:             y.conf.OperatorCode,
		OperatorToken:            req.Params.RRN,
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
		SMSText:                  "",
	}

	return r, nil
}

// ============================================================
// dn
type EventNotifyDN struct {
	EventName string                     `json:"event_name,omitempty"`
	EventData yondu_service.DNParameters `json:"event_data,omitempty"`
}

func (y *yondu) processDN(deliveries <-chan amqp_driver.Delivery) {
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

		var e EventNotifyDN

		if err := json.Unmarshal(msg.Body, &e); err != nil {
			y.m.DNDropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"dn":    string(msg.Body),
				"q":     y.conf.DN.Name,
			}).Error("failed")
			goto ack
		}

		r, err = y.getRecordByDN(e.EventData)
		logCtx = log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": e.EventData.Params.Msisdn,
		})

		transactionMsg = transaction_log_service.OperatorTransactionLog{
			Tid:              r.Tid,
			Msisdn:           e.EventData.Params.Msisdn,
			OperatorToken:    e.EventData.Params.RRN,
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
		if transErr := publishTransactionLog("dn", transactionMsg); transErr != nil {
			logCtx.WithFields(log.Fields{
				"event": e.EventName,
				"dn":    msg.Body,
				"error": transErr.Error(),
			}).Error("sent to transaction log failed")
			msg.Nack(false, true)
			continue
		} else {
			logCtx.WithFields(log.Fields{
				"queue": y.conf.DN.Name,
				"event": e.EventName,
				"tid":   r.Tid,
			}).Info("sent to transaction log")
		}
		if err != nil {
			goto ack
		}

	processResponse:
		// false - retries disabled
		if err = processResponse(&r, false); err != nil {
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"sleep": 1,
			}).Error("process response error")
			time.Sleep(time.Second)
			goto processResponse
		}
		if r.Paid {
			if err := y.sentContent(r); err != nil {
				logCtx.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("send content failed")
			}
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
				"dn":    msg.Body,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}

	}
}
func (y *yondu) getRecordByDN(req yondu_service.DNParameters) (r rec.Record, err error) {
	defer func() {
		if err != nil {
			r.OperatorErr = err.Error()
		}
		if r.SentAt.IsZero() {
			r.SentAt = time.Now().UTC()
		}
	}()

	logCtx := log.WithFields(log.Fields{
		"rrn": req.Params.RRN,
	})

	r, err = rec.GetSubscriptionByToken(req.Params.RRN)
	if err != nil && err != sql.ErrNoRows {
		y.m.DNRRnError.Inc()

		err := fmt.Errorf("rec.GetSubscriptionByToken: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("cannot get rec")
		return rec.Record{}, err
	}
	if r.SubscriptionId == 0 {
		r, err = rec.GetRetryByMsisdn(req.Params.Msisdn, "pending")
		if err != nil && err != sql.ErrNoRows {
			y.m.DNRRnError.Inc()

			err := fmt.Errorf("rec.GetRetryByMsisdn: %s", err.Error())
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("cannot get rec")
			return rec.Record{}, err
		} else {
			logCtx.WithFields(log.Fields{
				"tid": r.Tid,
			}).Debug("'pending' status")
		}
		if r.SubscriptionId == 0 {
			r, err = rec.GetRetryByMsisdn(req.Params.Msisdn, "")
			if err != nil && err != sql.ErrNoRows {
				y.m.DNRRnError.Inc()

				err := fmt.Errorf("rec.GetRetryByMsisdn: %s", err.Error())
				logCtx.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("cannot get rec")
				return rec.Record{}, err
			} else {
				logCtx.WithFields(log.Fields{
					"tid": r.Tid,
				}).Debug("'' status")
			}
		}
	}

	if r.SubscriptionId == 0 {
		y.m.DNRRnError.Inc()

		err := fmt.Errorf("rec.GetRetryByMsisdn: %s", "No subscription id")
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("cannot get rec")
		return rec.Record{}, err
	}

	if req.Params.Code == "201" || req.Params.Code == "200" {
		r.Paid = true
		r.SubscriptionStatus = "paid" // for operator transaction log
	} else {
		r.SubscriptionStatus = "failed"
	}
	r.SentAt, err = time.Parse("20060102150405", req.Params.Timestamp)
	if err != nil {
		y.m.DNParseTimeError.Inc()

		log.WithFields(log.Fields{
			"tid":       r.Tid,
			"error":     err.Error(),
			"timestamp": req.Params.Timestamp,
		}).Error("cannot parse dn time")
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
	DNDropped                               m.Gauge
	DNParseTimeError                        m.Gauge
	DNRRnError                              m.Gauge
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
		DNDropped:                               m.NewGauge(appName, telcoName, "dn_dropped", "yondu dn dropped"),
		DNParseTimeError:                        m.NewGauge(appName, telcoName, "dn_parse_time_error", "yondu dn parse operators time error"),
		DNRRnError:                              m.NewGauge(appName, telcoName, "dn_rrn_error", "yondu dn cannot get record by rrn"),
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
			ym.DNDropped.Update()
			ym.DNParseTimeError.Update()
			ym.DNRRnError.Update()
			ym.AddToDBErrors.Update()
			ym.AddToDbSuccess.Update()
			ym.DelayMinitsArentPassed.Update()
		}
	}()

	y.m = ym
}

// ============================================================
// notifier functions
func (y *yondu) publishMT(r rec.Record) error {
	if r.SMSText == "" {
		return fmt.Errorf("sms text is empty: %s", "")
	}
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
	svc.notifier.Publish(amqp.AMQPMessage{y.conf.MT, 0, body, event.EventName})
	return nil
}
func (y *yondu) publishCharge(priority uint8, r rec.Record) error {
	r.SMSText = ""
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
	svc.notifier.Publish(amqp.AMQPMessage{y.conf.MT, priority, body, event.EventName})
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
