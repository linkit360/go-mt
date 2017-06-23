package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	content_client "github.com/linkit360/go-contentd/rpcclient"
	mid_client "github.com/linkit360/go-mid/rpcclient"
	mid_service "github.com/linkit360/go-mid/service"
	yondu_service "github.com/linkit360/go-operator/ph/yondu/src/service"
	transaction_log_service "github.com/linkit360/go-qlistener/src/service"
	"github.com/linkit360/go-utils/amqp"
	queue_config "github.com/linkit360/go-utils/config"
	m "github.com/linkit360/go-utils/metrics"
	rec "github.com/linkit360/go-utils/rec"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
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
	CountryCode        int64                           `yaml:"country_code" default:"63"`
	Location           string                          `yaml:"location"`
	ChargeOnRejected   bool                            `yaml:"charge_on_rejected" default:"false"`
	Content            ContentConfig                   `yaml:"content"`
	Periodic           PeriodicConfig                  `yaml:"periodic"`
	MT                 string                          `yaml:"mt"`
	MO                 queue_config.ConsumeQueueConfig `yaml:"mo"`
	DN                 queue_config.ConsumeQueueConfig `yaml:"dn"`
	DNResponseCode     map[string]string               `yaml:"dn_code"`
	UnsubscribeMarkers []string                        `yaml:"unsubscribe"`
}

type ContentConfig struct {
	Enabled            bool   `yaml:"enabled"`
	Url                string `yaml:"url"`
	FetchLimit         int    `yaml:"fetch_limit"`
	FetchPeriodSeconds int    `yaml:"fetch_period_seconds"`
}

type PeriodicConfig struct {
	Enabled               bool   `yaml:"enabled"`
	Period                int    `yaml:"period" default:"600"`
	IntervalType          string `yaml:"interval_type" default:"hour"`
	FailedRepeatInMinutes int    `yaml:"failed_repeat_in_minutes" default:"60"`
	FetchLimit            int    `yaml:"fetch_limit" default:"500"`
}

func initYondu(yConf YonduConfig, consumerConfig amqp.ConsumerConfig, contentConf content_client.ClientConfig) *yondu {
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
	periodics, err := rec.GetPeriodicsSpecificTime(
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
		SetPendingStatusErrors.Inc()
		return
	}
	SetPeriodicPendingStatusDuration.Observe(time.Since(begin).Seconds())

	logCtx.WithFields(log.Fields{
		"amount":     r.Price,
		"service_id": r.ServiceCode,
		"priority":   priority,
	}).Info("charge")
	return
}

func (y *yondu) sentContent(r rec.Record) (err error) {
	if y.conf.Content.Enabled {
		log.Info("send content disabled")
		return nil
	}
	logCtx := log.WithFields(log.Fields{
		"tid": r.Tid,
	})
	service, err := mid_client.GetServiceByCode(r.ServiceCode)
	if err != nil {
		y.m.MOUnknownService.Inc()

		err = fmt.Errorf("mid_client.GetServiceById: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"serviceId": r.ServiceCode,
			"error":     err.Error(),
		}).Error("cannot get service by id")
		return
	}
	contentHash, err := getContentUniqueHash(r)
	if err != nil {
		Errors.Inc()
		err = fmt.Errorf("mid_client.GetServiceById: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"error":      err.Error(),
			"service_id": r.ServiceCode,
		}).Error("cannot get service by id")
		return err
	}
	url := y.conf.Content.Url + contentHash
	r.SMSText = fmt.Sprintf(service.SMSOnContent, url)

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
		var s xmp_api_structs.Service
		var err error
		var logCtx *log.Entry
		var transactionMsg transaction_log_service.OperatorTransactionLog

		y.m.MOIncoming.Inc()
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

		r, s, err = y.getRecordByMO(e.EventData)
		if err != nil {
			msg.Nack(false, true)
			continue
		}
		if err == nil && r.CampaignCode == "" {
			goto ack
		}
		transactionMsg = transaction_log_service.OperatorTransactionLog{
			Tid:              e.EventData.Tid,
			Msisdn:           e.EventData.Params.Msisdn,
			OperatorToken:    e.EventData.Params.RRN, // important, do not use from record - it's changed
			OperatorCode:     y.conf.OperatorCode,
			CountryCode:      r.CountryCode,
			Error:            r.OperatorErr,
			Price:            r.Price,
			ServiceCode:      r.ServiceCode,
			SubscriptionId:   r.SubscriptionId,
			CampaignCode:     r.CampaignCode,
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
				r.SMSText = s.SMSOnUnsubscribe
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
			} else {
				y.m.MOSuccess.Inc()
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
			r.SMSText = s.SMSOnRejected
		} else if r.Result == "blacklisted" {
			r.SMSText = s.SMSOnBlackListed
			if err := y.publishMT(r); err != nil {
				Errors.Inc()
				logCtx.WithField("error", err.Error()).Error("inform blacklisted failed")
			}
		} else if r.Result == "postpaid" {
			r.SMSText = s.SMSOnPostPaid
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
			Errors.Inc()

			log.WithFields(log.Fields{
				"mo":    msg.Body,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}

	}
}

func (y *yondu) getRecordByMO(req yondu_service.MOParameters) (r rec.Record, svc xmp_api_structs.Service, err error) {
	keyWords := strings.Split(req.Params.Message, " ")
	var campaign mid_service.Campaign
	campaign, err = mid_client.GetCampaignByKeyWord(keyWords[0])
	if err != nil {
		y.m.MOUnknownCampaign.Inc()

		err = fmt.Errorf("mid_client.GetCampaignByKeyWord: %s", err.Error())
		log.WithFields(log.Fields{
			"keyword": req.Params.Message,
			"error":   err.Error(),
		}).Error("cannot get campaign by keyword")
		return
	}
	svc, err = mid_client.GetServiceByCode(campaign.ServiceCode)
	if err != nil {
		y.m.MOUnknownService.Inc()

		err = fmt.Errorf("mid_client.GetServiceById: %s", err.Error())
		log.WithFields(log.Fields{
			"message":    req.Params.Message,
			"serviceId":  campaign.ServiceCode,
			"campaignId": campaign.Code,
			"error":      err.Error(),
		}).Error("cannot get service by id")
		return
	}

	r = rec.Record{
		SentAt:                   req.ReceivedAt,
		Msisdn:                   req.Params.Msisdn,
		Tid:                      req.Tid,
		SubscriptionStatus:       "",
		CountryCode:              y.conf.CountryCode,
		OperatorCode:             y.conf.OperatorCode,
		OperatorToken:            req.Params.RRN,
		Publisher:                "",
		Pixel:                    "",
		CampaignCode:             campaign.Code,
		ServiceCode:              campaign.ServiceCode,
		DelayHours:               svc.DelayHours,
		PaidHours:                svc.PaidHours,
		RetryDays:                svc.RetryDays,
		Price:                    svc.PriceCents,
		Periodic:                 true,
		PeriodicDays:             svc.PeriodicDays,
		PeriodicAllowedFromHours: svc.PeriodicAllowedFrom,
		PeriodicAllowedToHours:   svc.PeriodicAllowedTo,
		SMSText:                  "",
	}

	return
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
		var ok bool
		var resultCode string
		var logCtx *log.Entry
		var transactionMsg transaction_log_service.OperatorTransactionLog

		y.m.DNIncoming.Inc()

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
			CountryCode:      y.conf.CountryCode,
			Error:            r.OperatorErr,
			Price:            r.Price,
			ServiceCode:      r.ServiceCode,
			SubscriptionId:   r.SubscriptionId,
			CampaignCode:     r.CampaignCode,
			RequestBody:      e.EventData.Raw,
			ResponseBody:     "",
			ResponseDecision: r.SubscriptionStatus,
			ResponseCode:     200,
			SentAt:           r.SentAt,
			Type:             e.EventName,
		}
		resultCode, ok = y.conf.DNResponseCode[e.EventData.Params.Code]
		if ok {
			transactionMsg.Notice = resultCode
		} else {
			logCtx.WithFields(log.Fields{
				"error": "cannot find DN code",
				"code":  e.EventData.Params.Code,
			}).Warning("unknown code")
			transactionMsg.Notice = "unknown dn code"
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
		} else {
			y.m.DNSuccess.Inc()
		}
		if r.Paid {
			if err := y.sentContent(r); err != nil {
				logCtx.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("send content failed")
			}
		}
		// is was pending
		// it is not necessary to update subscription status since
		// it would be updated via qlistener in process response

	ack:
		if err := msg.Ack(false); err != nil {
			Errors.Inc()

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
	} else if req.Params.Code == "414" {
		r.SubscriptionStatus = "postpaid"
	} else if req.Params.Code == "1003" {
		r.SubscriptionStatus = "blacklisted"
	} else {
		r.SubscriptionStatus = "failed"
	}

	unixTime, err := strconv.ParseInt(req.Params.Timestamp, 10, 64)
	if err != nil {
		y.m.DNParseTimeError.Inc()
		log.WithFields(log.Fields{
			"tid":       r.Tid,
			"error":     fmt.Errorf("strconv.ParseInt: %s", err.Error()),
			"timestamp": req.Params.Timestamp,
		}).Error("cannot parse dn time")
		r.SentAt = time.Now().UTC()
	} else {
		r.SentAt = time.Unix(unixTime, 0)
	}

	return r, nil
}

// ============================================================
// metrics
type YonduMetrics struct {
	MOIncoming                              m.Gauge
	MOSuccess                               m.Gauge
	MODropped                               m.Gauge
	MOUnknownCampaign                       m.Gauge
	MOUnknownService                        m.Gauge
	DNIncoming                              m.Gauge
	DNDropped                               m.Gauge
	DNParseTimeError                        m.Gauge
	DNRRnError                              m.Gauge
	DNSuccess                               m.Gauge
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
		MOIncoming:                              m.NewGauge(appName, telcoName, "mo_incoming", "yondu dn incoming"),
		MOSuccess:                               m.NewGauge(appName, telcoName, "mo_success", "yondu mo success"),
		DNIncoming:                              m.NewGauge(appName, telcoName, "dn_incoming", "yondu mo incoming"),
		DNSuccess:                               m.NewGauge(appName, telcoName, "dn_success", "yondu dn success"),
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
			ym.MOIncoming.Update()
			ym.MOSuccess.Update()
			ym.MODropped.Update()
			ym.MOUnknownCampaign.Update()
			ym.MOUnknownService.Update()
			ym.DNDropped.Update()
			ym.DNIncoming.Update()
			ym.DNSuccess.Update()
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
// midory cache for incoming mo subscriptions
type activeSubscriptions struct {
	byKey map[string]rec.ActiveSubscription
}

func (y *yondu) initActiveSubscriptionsCache() {
	// load all active subscriptions
	prev, err := rec.LoadActiveSubscriptions()
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load active subscriptions")
	}
	y.activeSubscriptions = &activeSubscriptions{
		byKey: make(map[string]rec.ActiveSubscription),
	}
	for _, v := range prev {
		key := v.Msisdn + "-" + v.CampaignCode
		y.activeSubscriptions.byKey[key] = v
	}
}
func (y *yondu) getActiveSubscriptionCache(r rec.Record) bool {
	key := r.Msisdn + "-" + r.CampaignCode
	_, found := y.activeSubscriptions.byKey[key]
	log.WithFields(log.Fields{
		"tid":   r.Tid,
		"key":   key,
		"found": found,
	}).Debug("get active subscriptions cache")
	return found
}
func (y *yondu) setActiveSubscriptionCache(r rec.Record) {
	key := r.Msisdn + "-" + r.CampaignCode
	if _, found := y.activeSubscriptions.byKey[key]; found {
		return
	}
	y.activeSubscriptions.byKey[key] = rec.ActiveSubscription{
		Id:            r.SubscriptionId,
		AttemptsCount: r.AttemptsCount,
	}
	log.WithFields(log.Fields{
		"tid": r.Tid,
		"key": key,
	}).Debug("set active subscriptions cache")
}

func (y *yondu) deleteActiveSubscriptionCache(r rec.Record) {
	key := r.Msisdn + "-" + r.CampaignCode
	delete(y.activeSubscriptions.byKey, key)
	log.WithFields(log.Fields{
		"tid": r.Tid,
		"key": key,
	}).Debug("deleted from active subscriptions cache")
}
