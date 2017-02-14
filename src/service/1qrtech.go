package service

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	amqp_driver "github.com/streadway/amqp"

	"github.com/vostrok/utils/amqp"
	queue_config "github.com/vostrok/utils/config"
	m "github.com/vostrok/utils/metrics"
	rec "github.com/vostrok/utils/rec"
)

type qrtech struct {
	conf                   QRTechConfig
	m                      *QRTechMetrics
	activeSubscriptions    *activeSubscriptions
	shedulledSubscriptions *sync.WaitGroup
	MOCh                   <-chan amqp_driver.Delivery
	MOConsumer             *amqp.Consumer
}

type QRTechConfig struct {
	Enabled         bool                            `yaml:"enabled" default:"false"`
	OperatorName    string                          `yaml:"operator_name" default:"qrtech"`
	AisMNC          string                          `yaml:"ais_mnc" `
	DtacMNC         string                          `yaml:"dtac_mnc" `
	TruehMNC        string                          `yaml:"trueh_mnc" `
	MCC             string                          `yaml:"mcc"`
	CountryCode     int64                           `yaml:"country_code" default:"66"`
	NewSubscription queue_config.ConsumeQueueConfig `yaml:"new"`
	Charge          string                          `yaml:"charge" default:"qrtech_mt"`
	Periodic        PeriodicConfig                  `yaml:"periodic"`
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

	qr.initMetrics()

	if qrTechConf.NewSubscription.Enabled {
		if qrTechConf.NewSubscription.Name == "" {
			log.Fatal("empty queue name new subscriptions")
		}
		qr.MOConsumer = amqp.NewConsumer(
			consumerConfig,
			qrTechConf.NewSubscription.Name,
			qrTechConf.NewSubscription.PrefetchCount,
		)
		if err := qr.MOConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			qr.MOConsumer,
			qr.MOCh,
			qr.processMO,
			qrTechConf.NewSubscription.ThreadsCount,
			qrTechConf.NewSubscription.Name,
			qrTechConf.NewSubscription.Name,
		)
	} else {
		log.Debug("new subscription disabled")
	}

	if qrTechConf.Periodic.Enabled {
		go func() {
			for {
				time.Sleep(time.Duration(qrTechConf.Periodic.Period) * time.Second)
				qr.processPeriodic()
			}
		}()
	} else {
		log.Debug("periodic disabled")
	}

	return qr
}

type QRTechMetrics struct {
	MODropped      m.Gauge
	AddToDBErrors  m.Gauge
	AddToDbSuccess m.Gauge

	GetPeriodicsDuration     prometheus.Summary
	ProcessPeriodicsDuration prometheus.Summary
}

func (ch *qrtech) initMetrics() {

	ym := &QRTechMetrics{
		MODropped:                m.NewGauge(appName, ch.conf.OperatorName, "mo_dropped", "yondu mo dropped"),
		AddToDBErrors:            m.NewGauge(appName, ch.conf.OperatorName, "add_to_db_errors", "subscription add to db errors"),
		AddToDbSuccess:           m.NewGauge(appName, ch.conf.OperatorName, "add_to_db_success", "subscription add to db success"),
		GetPeriodicsDuration:     m.NewSummary("get_periodics_duration_seconds", "get periodics duration seconds"),
		ProcessPeriodicsDuration: m.NewSummary("process_periodics_duration_seconds", "process periodics duration seconds"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			ym.MODropped.Update()
			ym.AddToDBErrors.Update()
			ym.AddToDbSuccess.Update()
		}
	}()

	ch.m = ym
}

// ============================================================
// mo

type QRTechEventNotifyMO struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

func (qrTech *qrtech) processMO(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var r rec.Record

		var e QRTechEventNotifyMO
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			qrTech.m.MODropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"mo":    string(msg.Body),
			}).Error("consume from " + qrTech.conf.NewSubscription.Name)
			goto ack
		}
		r = e.EventData
		if err := rec.AddNewSubscriptionToDB(&r); err != nil {
			Errors.Inc()
			qrTech.m.AddToDBErrors.Inc()
			msg.Nack(false, true)
			continue
		} else {
			qrTech.m.AddToDbSuccess.Inc()
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

// ============================================================
// periodics
func (qr *qrtech) processPeriodic() {

	begin := time.Now()
	periodics, err := rec.GetPeriodics(qr.conf.Periodic.FetchLimit)
	if err != nil {
		err = fmt.Errorf("rec.GetPeriodics: %s", err.Error())
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("cannot get periodics")
		return
	}

	qr.m.GetPeriodicsDuration.Observe(time.Since(begin).Seconds())

	begin = time.Now()
	for _, r := range periodics {
		if err := qr.publishCharge(1, r); err != nil {
			Errors.Inc()
		}
		setStatusBegin := time.Now()
		if err = rec.SetSubscriptionStatus("pending", r.SubscriptionId); err != nil {
			return
		}
		SetPeriodicPendingStatusDuration.Observe(time.Since(setStatusBegin).Seconds())
	}
	qr.m.ProcessPeriodicsDuration.Observe(time.Since(begin).Seconds())
}

func (qr *qrtech) publishCharge(priority uint8, r rec.Record) error {
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
	svc.notifier.Publish(amqp.AMQPMessage{qr.conf.Charge, priority, body})
	return nil
}
