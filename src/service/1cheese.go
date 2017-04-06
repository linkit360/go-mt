package service

import (
	"encoding/json"
	"time"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	reporter_client "github.com/linkit360/go-reporter/rpcclient"
	reporter_collector "github.com/linkit360/go-reporter/server/src/collector"
	"github.com/linkit360/go-utils/amqp"
	queue_config "github.com/linkit360/go-utils/config"
	m "github.com/linkit360/go-utils/metrics"
	rec "github.com/linkit360/go-utils/rec"
)

type cheese struct {
	conf       CheeseConfig
	m          *CheeseMetrics
	MOCh       <-chan amqp_driver.Delivery
	MOConsumer *amqp.Consumer
}

type CheeseConfig struct {
	Enabled         bool                            `yaml:"enabled" default:"false"`
	OperatorName    string                          `yaml:"operator_name" default:"cheese"`
	AisMNC          string                          `yaml:"ais_mnc"`
	DtacMNC         string                          `yaml:"dtac_mnc"`
	TruehMNC        string                          `yaml:"trueh_mnc"`
	MCC             string                          `yaml:"mcc"`
	CountryCode     int64                           `yaml:"country_code" default:"66"`
	NewSubscription queue_config.ConsumeQueueConfig `yaml:"new"`
}

func initCheese(
	cheeseConf CheeseConfig,
	consumerConfig amqp.ConsumerConfig,
) *cheese {
	if !cheeseConf.Enabled {
		return nil
	}
	ch := &cheese{
		conf: cheeseConf,
	}
	if cheeseConf.NewSubscription.Enabled {
		if cheeseConf.NewSubscription.Name == "" {
			log.Fatal("empty queue name new subscriptions")
		}
		ch.MOConsumer = amqp.NewConsumer(
			consumerConfig,
			cheeseConf.NewSubscription.Name,
			cheeseConf.NewSubscription.PrefetchCount,
		)
		if err := ch.MOConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			ch.MOConsumer,
			ch.MOCh,
			ch.processMO,
			cheeseConf.NewSubscription.ThreadsCount,
			cheeseConf.NewSubscription.Name,
			cheeseConf.NewSubscription.Name,
		)
	} else {
		log.Debug("new subscription disabled")
	}

	ch.initMetrics()
	return ch
}

type CheeseMetrics struct {
	MODropped      m.Gauge
	AddToDBErrors  m.Gauge
	AddToDbSuccess m.Gauge
}

func (ch *cheese) initMetrics() {

	ym := &CheeseMetrics{
		MODropped:      m.NewGauge(appName, ch.conf.OperatorName, "mo_dropped", "cheese mo dropped"),
		AddToDBErrors:  m.NewGauge(appName, ch.conf.OperatorName, "add_to_db_errors", "cheese subscription add to db errors"),
		AddToDbSuccess: m.NewGauge(appName, ch.conf.OperatorName, "add_to_db_success", "cheese subscription add to db success"),
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

type CheeseEventNotifyMO struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

func (ch *cheese) processMO(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var r rec.Record

		var e CheeseEventNotifyMO
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			ch.m.MODropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"mo":    string(msg.Body),
			}).Error("consume from " + ch.conf.NewSubscription.Name)
			goto ack
		}
		r = e.EventData
		if err := rec.AddNewSubscriptionToDB(&r); err != nil {
			Errors.Inc()
			ch.m.AddToDBErrors.Inc()
			msg.Nack(false, true)
			continue
		} else {
			// no response processing, so it's here
			reporter_client.IncPaid(reporter_collector.Collect{
				CampaignId:        r.CampaignId,
				OperatorCode:      r.OperatorCode,
				Msisdn:            r.Msisdn,
				TransactionResult: "paid",
			})
			ch.m.AddToDbSuccess.Inc()
		}

		if err := notifyRestorePixel(r); err != nil {
			Errors.Inc()
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
