package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	"github.com/vostrok/db"
	rec "github.com/vostrok/mt_manager/src/service/instance"
	m "github.com/vostrok/mt_manager/src/service/metrics"
	pixels "github.com/vostrok/pixels/src/notifier"
	"github.com/vostrok/rabbit"
)

var svc MTService

func Init(
	sConf MTServiceConfig,
	dbConf db.DataBaseConfig,
	publisherConf rabbit.NotifierConfig,
	consumerConfig rabbit.ConnectionConfig,

) {
	log.SetLevel(log.DebugLevel)

	svc.conf = sConf
	svc.dbConf = dbConf
	rec.Init(dbConf)

	m.Init()

	svc.publisher = rabbit.NewNotifier(publisherConf)

	if err := initInMem(dbConf); err != nil {
		log.WithField("error", err.Error()).Fatal("init in memory tables")
	}
	log.Info("inmemory tables init ok")

	svc.newSubscriptionsChan = make(map[string]<-chan amqp_driver.Delivery)
	svc.operatorTarifficateResponsesChan = make(map[string]<-chan amqp_driver.Delivery)
	// process consumer
	svc.consumer = rabbit.NewConsumer(consumerConfig)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}

	// handle operators responses
	for operatorName, queue := range sConf.Queues.Operator {

		// queue for new subscritpions: asap tarifficate
		log.Info("initialising operator " + operatorName)
		var err error
		svc.newSubscriptionsChan[operatorName], err =
			svc.consumer.AnnounceQueue(queue.NewSubscription, queue.NewSubscription)
		if err != nil {
			log.WithFields(log.Fields{
				"queue": queue.NewSubscription,
				"error": err.Error(),
			}).Fatal("rbmq consumer: AnnounceQueue")
		}
		go svc.consumer.Handle(
			svc.newSubscriptionsChan[operatorName],
			processSubscriptions,
			sConf.ThreadsCount,
			queue.NewSubscription,
			queue.NewSubscription,
		)

		// queue for responses
		svc.operatorTarifficateResponsesChan[operatorName], err =
			svc.consumer.AnnounceQueue(queue.Responses, queue.Responses)
		if err != nil {
			log.WithFields(log.Fields{
				"queue": queue.Responses,
				"error": err.Error(),
			}).Fatal("rbmq consumer: AnnounceQueue")
		}
		go svc.consumer.Handle(
			svc.operatorTarifficateResponsesChan[operatorName],
			processResponses,
			sConf.ThreadsCount,
			queue.Responses,
			queue.Responses,
		)

	}
}

type MTService struct {
	newSubscriptionsChan             map[string]<-chan amqp_driver.Delivery
	operatorTarifficateResponsesChan map[string]<-chan amqp_driver.Delivery
	conf                             MTServiceConfig
	dbConf                           db.DataBaseConfig
	publisher                        rabbit.Notifier
	consumer                         rabbit.Consumer
}
type OperatorQueueConfig struct {
	NewSubscription string `yaml:"-"`
	Requests        string `yaml:"-"`
	Responses       string `yaml:"-"`
	SMS             string `yaml:"-"`
}
type QueuesConfig struct {
	Pixels   string                         `default:"pixels" yaml:"pixels"`
	Operator map[string]OperatorQueueConfig `yaml:"-"`
}
type OperatorConfig struct {
	Name           string `yaml:"name"`
	RetriesEnabled bool   `yaml:"retries_enabled"`
}
type MTServiceConfig struct {
	RetrySec     int              `default:"600" yaml:"retry_period"`
	RetryCount   int              `default:"600" yaml:"retry_count"`
	ThreadsCount int              `default:"1" yaml:"threads_count"`
	Queues       QueuesConfig     `yaml:"queues"`
	Operators    []OperatorConfig `yaml:"operators"`
}

func notifyPixel(msg pixels.Pixel) error {
	log.WithField("pixel", fmt.Sprintf("%#v", msg)).Debug("got pixel")

	event := rabbit.EventNotify{
		EventName: "pixels",
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	log.WithField("body", string(body)).Debug("sent pixels")
	svc.publisher.Publish(rabbit.AMQPMessage{svc.conf.Queues.Pixels, body})
	return nil
}

func notifyOperatorRequest(queue, eventName string, msg interface{}) error {
	if eventName == "" {
		return fmt.Errorf("QueueSend: %s", "empty event name")
	}
	if queue == "" {
		return fmt.Errorf("QueueSend: %s", "empty queue name")
	}

	event := rabbit.EventNotify{
		EventName: eventName,
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	log.WithFields(log.Fields{
		"queue": queue,
		"event": eventName,
		"data":  fmt.Sprintf("%#v", msg),
	}).Debug("prepare to send in queue")
	svc.publisher.Publish(rabbit.AMQPMessage{queue, body})
	return nil
}