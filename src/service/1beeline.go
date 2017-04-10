package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	inmem_client "github.com/linkit360/go-inmem/rpcclient"
	beeline_service "github.com/linkit360/go-operator/ru/beeline/src/service"
	transaction_log_service "github.com/linkit360/go-qlistener/src/service"
	reporter_client "github.com/linkit360/go-reporter/rpcclient"
	reporter_collector "github.com/linkit360/go-reporter/server/src/collector"
	"github.com/linkit360/go-utils/amqp"
	queue_config "github.com/linkit360/go-utils/config"
	m "github.com/linkit360/go-utils/metrics"
	rec "github.com/linkit360/go-utils/rec"
)

type beeline struct {
	conf         BeelineConfig
	m            *BeelineMetrics
	MOCh         <-chan amqp_driver.Delivery
	MOConsumer   *amqp.Consumer
	SMPPCh       <-chan amqp_driver.Delivery
	SMPPConsumer *amqp.Consumer
}

type BeelineConfig struct {
	Enabled         bool                            `yaml:"enabled"`
	OperatorName    string                          `yaml:"operator_name"`
	MCCMNC          int64                           `yaml:"mccmnc"`
	CountryCode     int64                           `yaml:"country_code" default:"7"`
	NewSubscription queue_config.ConsumeQueueConfig `yaml:"new"`
	SMPPIn          queue_config.ConsumeQueueConfig `yaml:"smpp"`
}

func initBeeline(
	beelineConf BeelineConfig,
	consumerConfig amqp.ConsumerConfig,
) *beeline {
	if !beelineConf.Enabled {
		return nil
	}
	bee := &beeline{
		conf: beelineConf,
	}
	if beelineConf.NewSubscription.Enabled {
		if beelineConf.NewSubscription.Name == "" {
			log.Fatal("empty queue name new subscriptions")
		}
		bee.MOConsumer = amqp.NewConsumer(
			consumerConfig,
			beelineConf.NewSubscription.Name,
			beelineConf.NewSubscription.PrefetchCount,
		)
		if err := bee.MOConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			bee.MOConsumer,
			bee.MOCh,
			bee.processMO,
			beelineConf.NewSubscription.ThreadsCount,
			beelineConf.NewSubscription.Name,
			beelineConf.NewSubscription.Name,
		)
	} else {
		log.Debug("new subscription disabled")
	}

	if beelineConf.SMPPIn.Enabled {
		if beelineConf.SMPPIn.Name == "" {
			log.Fatal("empty queue name smpp in")
		}
		bee.SMPPConsumer = amqp.NewConsumer(
			consumerConfig,
			beelineConf.SMPPIn.Name,
			beelineConf.SMPPIn.PrefetchCount,
		)
		if err := bee.SMPPConsumer.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		amqp.InitQueue(
			bee.SMPPConsumer,
			bee.SMPPCh,
			bee.processSMPP,
			beelineConf.SMPPIn.ThreadsCount,
			beelineConf.SMPPIn.Name,
			beelineConf.SMPPIn.Name,
		)
	} else {
		log.Debug("smpp in queue disabled")
	}

	bee.initMetrics()
	return bee
}

// ============================================================
// metrics

type BeelineMetrics struct {
	MO             m.Gauge
	SMPPMO         m.Gauge
	MODropped      m.Gauge
	AddToDBErrors  m.Gauge
	AddToDbSuccess m.Gauge
	SMPPIn         m.Gauge
	SMPPDropped    m.Gauge
	SMPPEmpty      m.Gauge
	Unsubscribe    m.Gauge
}

func (be *beeline) initMetrics() {

	bm := &BeelineMetrics{
		MODropped:      m.NewGauge(appName, be.conf.OperatorName, "mo_dropped", "beeline mo dropped"),
		AddToDBErrors:  m.NewGauge(appName, be.conf.OperatorName, "add_to_db_errors", "beeline subscription add to db errors"),
		AddToDbSuccess: m.NewGauge(appName, be.conf.OperatorName, "add_to_db_success", "beeline subscription add to db success"),
		SMPPIn:         m.NewGauge(appName, be.conf.OperatorName, "smpp_in", "beeline smpp in"),
		SMPPDropped:    m.NewGauge(appName, be.conf.OperatorName, "dropped", "beeline smpp dropped"),
		SMPPEmpty:      m.NewGauge(appName, be.conf.OperatorName, "empty", "beeline smpp empty"),
		Unsubscribe:    m.NewGauge(appName, be.conf.OperatorName, "unsubscribe", "beeline smpp unsubscribe"),
		MO:             m.NewGauge(appName, be.conf.OperatorName, "mo", "beeline new mo"),
		SMPPMO:         m.NewGauge(appName, be.conf.OperatorName, "smpp_mo", "beeline new smpp mo"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			bm.MODropped.Update()
			bm.AddToDBErrors.Update()
			bm.AddToDbSuccess.Update()
			bm.SMPPDropped.Update()
			bm.SMPPEmpty.Update()
			bm.SMPPIn.Update()
			bm.Unsubscribe.Update()
			bm.MO.Update()
			bm.SMPPMO.Update()
		}
	}()

	be.m = bm
}

// ============================================================
// from dispatcher and from smpp by myself

type BeelineEventNotifyMO struct {
	EventName string     `json:"event_name,omitempty"`
	EventData rec.Record `json:"event_data,omitempty"`
}

func (be *beeline) processMO(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		var r rec.Record
		var e BeelineEventNotifyMO
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			be.m.MODropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"mo":    string(msg.Body),
			}).Error("consume from " + be.conf.NewSubscription.Name)
			goto ack
		}
		r = e.EventData
		if err := rec.AddNewSubscriptionToDB(&r); err != nil {
			Errors.Inc()
			be.m.AddToDBErrors.Inc()
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
			be.m.AddToDbSuccess.Inc()
			be.m.MO.Inc()
			SinceLastSuccessPay.Set(.0)
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
// from smpp in one queue

type BeelineEventNotifySMPP struct {
	EventName string                   `json:"event_name,omitempty"`
	EventData beeline_service.Incoming `json:"event_data,omitempty"`
}

func (be *beeline) processSMPP(deliveries <-chan amqp_driver.Delivery) {
	for msg := range deliveries {
		logCtx := log.WithFields(log.Fields{
			"q": be.conf.SMPPIn.Name,
		})
		var err error
		var i beeline_service.Incoming
		var e BeelineEventNotifySMPP
		var r rec.Record
		var tl transaction_log_service.OperatorTransactionLog
		var fieldsJSON []byte

		if err = json.Unmarshal(msg.Body, &e); err != nil {
			be.m.SMPPDropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"mo":    string(msg.Body),
			}).Error("consume from " + be.conf.NewSubscription.Name)
			goto ack
		}
		if len(e.EventData.DstAddr) < 4 {
			be.m.SMPPDropped.Inc()
			be.m.SMPPEmpty.Inc()

			log.WithFields(log.Fields{
				"msg":     "dropped",
				"error":   "dstaddrr is too short",
				"dstAddr": e.EventData.DstAddr,
				"body":    string(msg.Body),
			}).Error("consume from " + be.conf.NewSubscription.Name)
			goto ack
		}

		i = e.EventData
		logCtx = logCtx.WithField("tid", i.Tid)

		fieldsJSON, _ = json.Marshal(i)

		r, err = be.resolveRec(i)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"i":     fmt.Sprintf("%#v", i),
			}).Info("requeue")
			msg.Nack(false, true)
			return
		}
		tl = transaction_log_service.OperatorTransactionLog{
			Tid:           r.Tid,
			Msisdn:        r.Msisdn,
			OperatorToken: r.OperatorToken,
			OperatorCode:  r.OperatorCode,
			CountryCode:   r.CountryCode,
			Error:         r.OperatorErr,
			Price:         r.Price,
			ServiceId:     r.ServiceId,
			CampaignId:    r.CampaignId,
			RequestBody:   string(fieldsJSON),
			ResponseBody:  "",
			ResponseCode:  200,
			Type:          "smpp",
		}

		switch i.SourcePort {
		case 3:
			tl.ResponseDecision = "subscription enabled"
			r.Paid = true
			r.SubscriptionStatus = "paid"
			logCtx.Debug(tl.ResponseDecision)
			be.publishMO(r)
			be.m.SMPPMO.Inc()
		case 4:
			tl.ResponseDecision = "subscription disabled"
			r.SubscriptionStatus = "canceled"
			logCtx.Debug(tl.ResponseDecision)
			be.m.Unsubscribe.Inc()
			unsubscribe(r)
		case 5:
			logCtx.Debug("charge notify")
			r.Paid = true
			r.SubscriptionStatus = "paid"
			r.Result = "paid"
			writeTransaction(r)
			reporter_client.IncPaid(reporter_collector.Collect{
				CampaignId:        r.CampaignId,
				OperatorCode:      r.OperatorCode,
				Msisdn:            r.Msisdn,
				TransactionResult: "paid",
			})
		case 6:
			tl.ResponseDecision = "unsubscribe all"
			logCtx.Debug(tl.ResponseDecision)
			r.SubscriptionStatus = "canceled"
			be.m.Unsubscribe.Inc()
			unsubscribeAll(r)
		case 1:
			tl.ResponseDecision = "unsubscribe all"
			logCtx.Debug(tl.ResponseDecision)
			r.SubscriptionStatus = "canceled"
			be.m.Unsubscribe.Inc()
			unsubscribeAll(r)
		case 7:
			tl.ResponseDecision = "block"
			logCtx.Debug(tl.ResponseDecision)
		case 9:
			tl.ResponseDecision = "msisdn migration"
			logCtx.Debug(tl.ResponseDecision)
		}
		publishTransactionLog("smpp", tl)

	ack:
		if err = msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"mo":    msg.Body,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}

	}
}

func (be *beeline) resolveRec(i beeline_service.Incoming) (r rec.Record, err error) {
	logCtx := log.WithField("tid", r.Tid)

	r = rec.Record{
		Tid:           i.Tid,
		CountryCode:   be.conf.CountryCode,
		OperatorCode:  be.conf.MCCMNC,
		Msisdn:        i.SourceAddr,
		OperatorToken: i.Seq,
		Notice:        i.ShortMessage + fmt.Sprintf(" source_port: %v", i.SourcePort),
	}
	serviceToken := i.DstAddr[0:4] // short number
	campaign, err := inmem_client.GetCampaignByKeyWord(serviceToken)
	if err != nil {
		Errors.Inc()
		err = fmt.Errorf("inmem_client.GetCampaignByKeyWord: %s", err.Error())

		logCtx.WithFields(log.Fields{
			"serviceToken": serviceToken,
			"error":        err.Error(),
		}).Error("cannot find campaign by serviceToken")
		return
	}

	r.CampaignId = campaign.Id
	r.ServiceId = campaign.ServiceId
	service, err := inmem_client.GetServiceById(campaign.ServiceId)
	if err != nil {
		Errors.Inc()
		err = fmt.Errorf("inmem_client.GetServiceById: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"service_id": campaign.ServiceId,
			"error":      err.Error(),
		}).Error("cannot get service by id")
		return
	}

	r.Price = int(service.Price) * 100
	r.DelayHours = service.DelayHours
	r.PaidHours = service.PaidHours
	r.KeepDays = service.KeepDays
	r.Periodic = false
	return
}
func (be *beeline) publishMO(r rec.Record) (err error) {
	event := amqp.EventNotify{
		EventName: "mo",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return
	}
	svc.notifier.Publish(amqp.AMQPMessage{QueueName: be.conf.NewSubscription.Name, Priority: 0, Body: body})
	return nil
}
