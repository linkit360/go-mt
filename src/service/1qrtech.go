package service

import (
	"encoding/json"
	"time"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	"github.com/vostrok/utils/amqp"
	queue_config "github.com/vostrok/utils/config"
	m "github.com/vostrok/utils/metrics"
	rec "github.com/vostrok/utils/rec"
)

type qrtech struct {
	conf                QRTechConfig
	m                   *QRTechMetrics
	activeSubscriptions *activeSubscriptions
	MOCh                <-chan amqp_driver.Delivery
	MOConsumer          *amqp.Consumer
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
}

func initQRTech(
	qrTechConf QRTechConfig,
	consumerConfig amqp.ConsumerConfig,
) *qrtech {
	if !qrTechConf.Enabled {
		return nil
	}
	ch := &qrtech{
		conf: qrTechConf,
	}
	if qrTechConf.NewSubscription.Enabled {
		if qrTechConf.NewSubscription.Name == "" {
			log.Fatal("empty queue name new subscriptions")
		}
		ch.MOConsumer = amqp.NewConsumer(
			consumerConfig,
			qrTechConf.NewSubscription.Name,
			qrTechConf.NewSubscription.PrefetchCount,
		)
		if err := ch.MOConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			ch.MOConsumer,
			ch.MOCh,
			ch.processMO,
			qrTechConf.NewSubscription.ThreadsCount,
			qrTechConf.NewSubscription.Name,
			qrTechConf.NewSubscription.Name,
		)
	} else {
		log.Debug("new subscription disabled")
	}

	ch.initMetrics()
	return ch
}

type QRTechMetrics struct {
	MODropped      m.Gauge
	AddToDBErrors  m.Gauge
	AddToDbSuccess m.Gauge
}

func (ch *qrtech) initMetrics() {

	ym := &QRTechMetrics{
		MODropped:      m.NewGauge(appName, ch.conf.OperatorName, "mo_dropped", "yondu mo dropped"),
		AddToDBErrors:  m.NewGauge(appName, ch.conf.OperatorName, "add_to_db_errors", "subscription add to db errors"),
		AddToDbSuccess: m.NewGauge(appName, ch.conf.OperatorName, "add_to_db_success", "subscription add to db success"),
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
