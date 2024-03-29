package service

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	"github.com/linkit360/go-utils/amqp"
	queue_config "github.com/linkit360/go-utils/config"
	m "github.com/linkit360/go-utils/metrics"
	rec "github.com/linkit360/go-utils/rec"
)

type qrtech struct {
	conf                QRTechConfig
	m                   *QRTechMetrics
	loc                 *time.Location
	activeSubscriptions *activeSubscriptions
	MOCh                <-chan amqp_driver.Delivery
	DNCh                <-chan amqp_driver.Delivery
	MOConsumer          *amqp.Consumer
	DNConsumer          *amqp.Consumer
}

type QRTechConfig struct {
	Enabled      bool                            `yaml:"enabled" default:"false"`
	OperatorName string                          `yaml:"operator_name" default:"qrtech"`
	AisMNC       string                          `yaml:"ais_mnc"`
	DtacMNC      string                          `yaml:"dtac_mnc"`
	TruehMNC     string                          `yaml:"trueh_mnc"`
	MCC          string                          `yaml:"mcc"`
	CountryCode  int64                           `yaml:"country_code" default:"66"`
	Location     string                          `yaml:"location"`
	MO           queue_config.ConsumeQueueConfig `yaml:"mo"`
	DN           queue_config.ConsumeQueueConfig `yaml:"dn"`
}

func initQRTech(
	qrTechConf QRTechConfig,
	consumerConfig amqp.ConsumerConfig,
) *qrtech {
	if !qrTechConf.Enabled {
		return nil
	}
	qr := &qrtech{
		conf: qrTechConf,
	}
	var err error
	qr.loc, err = time.LoadLocation(qrTechConf.Location)
	if err != nil {
		log.WithFields(log.Fields{
			"location": qrTechConf.Location,
			"error":    err,
		}).Fatal("location")
	}

	qr.initMetrics()
	qr.initActiveSubscriptionsCache()

	if qrTechConf.MO.Enabled {
		if qrTechConf.MO.Name == "" {
			log.Fatal("empty queue name mo")
		}
		qr.MOConsumer = amqp.NewConsumer(
			consumerConfig,
			qrTechConf.MO.Name,
			qrTechConf.MO.PrefetchCount,
		)
		if err := qr.MOConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			qr.MOConsumer,
			qr.MOCh,
			qr.processMO,
			qrTechConf.MO.ThreadsCount,
			qrTechConf.MO.Name,
			qrTechConf.MO.Name,
		)
	} else {
		log.Debug("mo disabled")
	}

	if qrTechConf.DN.Enabled {
		if qrTechConf.DN.Name == "" {
			log.Fatal("empty queue name dn")
		}
		qr.DNConsumer = amqp.NewConsumer(
			consumerConfig,
			qrTechConf.DN.Name,
			qrTechConf.DN.PrefetchCount,
		)
		if err := qr.DNConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			qr.DNConsumer,
			qr.DNCh,
			qr.processDN,
			qrTechConf.DN.ThreadsCount,
			qrTechConf.DN.Name,
			qrTechConf.DN.Name,
		)
	} else {
		log.Debug("dn queue disabled")
	}
	return qr
}

type QRTechMetrics struct {
	MODropped      m.Gauge
	AddToDBErrors  m.Gauge
	AddToDbSuccess m.Gauge

	DNDropped m.Gauge
	DNErrors  m.Gauge
	DNSuccess m.Gauge

	GetPeriodicsDuration     prometheus.Summary
	ProcessPeriodicsDuration prometheus.Summary
}

func (qr *qrtech) initMetrics() {

	qrm := &QRTechMetrics{
		MODropped:                m.NewGauge(appName, qr.conf.OperatorName, "mo_dropped", "qrtech mo dropped"),
		AddToDBErrors:            m.NewGauge(appName, qr.conf.OperatorName, "add_to_db_errors", "subscription add to db errors"),
		AddToDbSuccess:           m.NewGauge(appName, qr.conf.OperatorName, "add_to_db_success", "subscription add to db success"),
		DNDropped:                m.NewGauge(appName, qr.conf.OperatorName, "dn_dropped", "qrtech dn dropped"),
		DNErrors:                 m.NewGauge(appName, qr.conf.OperatorName, "dn_errors", "qrtech dn errors"),
		DNSuccess:                m.NewGauge(appName, qr.conf.OperatorName, "dn_success", "qrtech dn success"),
		GetPeriodicsDuration:     m.NewSummary("get_periodics_duration_seconds", "get periodics duration seconds"),
		ProcessPeriodicsDuration: m.NewSummary("process_periodics_duration_seconds", "process periodics duration seconds"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			qrm.MODropped.Update()
			qrm.AddToDBErrors.Update()
			qrm.AddToDbSuccess.Update()
			qrm.DNDropped.Update()
			qrm.DNErrors.Update()
			qrm.DNSuccess.Update()
		}
	}()

	qr.m = qrm
}

// ============================================================
// mo

type QRTechEventNotifyMO struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

func (qr *qrtech) processMO(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var r rec.Record

		var e QRTechEventNotifyMO
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			qr.m.MODropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"mo":    string(msg.Body),
			}).Error("consume from " + qr.conf.MO.Name)
			goto ack
		}
		r = e.EventData
		if err := rec.AddNewSubscriptionToDB(&r); err != nil {
			Errors.Inc()
			qr.m.AddToDBErrors.Inc()
			msg.Nack(false, true)
			continue
		} else {
			qr.m.AddToDbSuccess.Inc()
		}
		qr.setActiveSubscriptionCache(r)

		if err := notifyRestorePixel(r); err != nil {
			Errors.Inc()
		}

		r.Result = ""
		r.AttemptsCount = 0
		publishReporter(svc.conf.Queues.ReporterTransaction, r)
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

// ============================================================
// dn

type QRTechEventNotifyDN struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

func (qr *qrtech) processDN(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var r rec.Record
		var err error
		var e QRTechEventNotifyDN
		var as rec.ActiveSubscription
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			qr.m.DNDropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"dn":    string(msg.Body),
			}).Error("consume from " + qr.conf.DN.Name)
			goto ack
		}
		r = e.EventData

		as, err = qr.getActiveSubscriptionCache(r)
		if err != nil {
			time.Sleep(time.Second)
			msg.Nack(false, true)
			log.WithFields(log.Fields{
				"q":   qr.conf.DN.Name,
				"msg": "requeue",
				"dn":  string(msg.Body),
			}).Error("consume")
			continue
		}

		if as.Id != 0 {
			r.SubscriptionId = as.Id
			r.AttemptsCount = as.AttemptsCount
		} else {
			if err := rec.AddNewSubscriptionToDB(&r); err != nil {
				Errors.Inc()
				qr.m.AddToDBErrors.Inc()
				msg.Nack(false, true)
				log.WithFields(log.Fields{
					"msg": "requeue",
				}).Error("consume from " + qr.conf.DN.Name)
				continue
			}
			qr.m.AddToDbSuccess.Inc()
			qr.setActiveSubscriptionCache(r)
		}
		if err := processResponse(&r, false); err != nil {
			qr.m.DNErrors.Inc()
			msg.Nack(false, true)
			continue
		} else {
			qr.m.DNSuccess.Inc()
		}
		r.AttemptsCount = r.AttemptsCount + 1
		qr.setActiveSubscriptionCache(r)

	ack:
		if err := msg.Ack(false); err != nil {
			Errors.Inc()

			log.WithFields(log.Fields{
				"dn":    msg.Body,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}

	}
}

// ============================================================
// active subscriptions cache to store subscription id for dn

func (qr *qrtech) initActiveSubscriptionsCache() {
	// load all active subscriptions
	prev, err := rec.LoadActiveSubscriptions()
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load active subscriptions")
	}
	qr.activeSubscriptions = &activeSubscriptions{
		byKey: make(map[string]rec.ActiveSubscription),
	}
	for _, v := range prev {
		key := v.Msisdn + "-" + v.CampaignCode
		qr.activeSubscriptions.byKey[key] = v
	}
}

func (qr *qrtech) getActiveSubscriptionCache(r rec.Record) (as rec.ActiveSubscription, err error) {
	key := r.Msisdn + "-" + r.CampaignId
	as, found := qr.activeSubscriptions.byKey[key]
	if !found {
		var oldRec rec.Record
		oldRec, err = rec.GetSubscriptionByMsisdn(r.Msisdn)
		if err != nil {
			if err == sql.ErrNoRows {
				err = nil
				return
			}
			return
		}

		as = rec.ActiveSubscription{
			Id:            oldRec.SubscriptionId,
			AttemptsCount: oldRec.AttemptsCount,
		}
		return
	}
	log.WithFields(log.Fields{
		"tid":             r.Tid,
		"subscription_id": as.Id,
		"attempts_count":  as.AttemptsCount,
	}).Debug("get active subscriptions cache")
	return
}

func (qr *qrtech) setActiveSubscriptionCache(r rec.Record) {
	key := r.Msisdn + "-" + r.CampaignId

	// even if found, we need update attempts count
	qr.activeSubscriptions.byKey[key] = rec.ActiveSubscription{
		Id:            r.SubscriptionId,
		AttemptsCount: r.AttemptsCount,
	}
	log.WithFields(log.Fields{
		"tid": r.Tid,
		"key": key,
	}).Debug("set active subscriptions cache")
}

func (qr *qrtech) deleteActiveSubscriptionCache(r rec.Record) {
	key := r.Msisdn + "-" + r.CampaignId
	delete(qr.activeSubscriptions.byKey, key)
	log.WithFields(log.Fields{
		"tid": r.Tid,
		"key": key,
	}).Debug("deleted from active subscriptions cache")
}
