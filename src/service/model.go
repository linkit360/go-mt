package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	pixels "github.com/vostrok/pixels/src/notifier"
	"github.com/vostrok/utils/amqp"
	queue_config "github.com/vostrok/utils/config"
	"github.com/vostrok/utils/db"
	rec "github.com/vostrok/utils/rec"
)

// queues:
// in: new subscription, reponses, (?sms_responses)
// out: pixels, requests, send_sms
var svc MTService

func Init(
	sConf MTServiceConfig,
	operatorConfig map[string]queue_config.OperatorConfig,
	queueOperators map[string]queue_config.OperatorQueueConfig,
	dbConf db.DataBaseConfig,
	publisherConf rabbit.NotifierConfig,
	consumerConfig rabbit.ConsumerConfig,

) {
	log.SetLevel(log.DebugLevel)

	svc.conf = sConf
	svc.dbConf = dbConf
	svc.conf.QueueOperators = queueOperators
	rec.Init(dbConf)

	initMetrics()

	svc.publisher = rabbit.NewNotifier(publisherConf)

	if err := initInMem(dbConf); err != nil {
		log.WithField("error", err.Error()).Fatal("init in memory tables")
	}
	log.Info("inmemory tables init ok")

	svc.operatorRequestQueueFree = make(map[string]chan struct{}, len(operatorConfig))
	go func() {
		for range time.Tick(time.Second) {
			for operatorName, queue := range queueOperators {
				queueSize, err := svc.publisher.GetQueueSize(queue.Requests)
				if err != nil {
					log.WithFields(log.Fields{
						"operator": operatorName,
						"error":    err.Error(),
					}).Error("cannot get queue size")
				}
				if queueSize < operatorConfig[operatorName].OperatorRequestQueueSize {
					svc.operatorRequestQueueFree[operatorName] <- struct{}{}
				}
			}
		}
	}()

	// retries
	go func() {
		for operatorName, operatorConf := range operatorConfig {
			if operatorConf.RetriesEnabled {
				operator, ok := memOperators.ByName[strings.ToLower(operatorName)]
				if !ok {
					log.WithField("operatorName", operatorName).Fatal("cannot find operator")
				}
				select {
				case <-svc.operatorRequestQueueFree[operatorName]:
					processRetries(operator.Code)
				}

			}
		}

	}()

	// process consumer
	svc.consumer = rabbit.NewConsumer(consumerConfig)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}

	svc.MOTarifficateRequestsChan = make(map[string]<-chan amqp_driver.Delivery, len(operatorConfig))
	svc.operatorTarifficateResponsesChan = make(map[string]<-chan amqp_driver.Delivery, len(operatorConfig))
	for operatorName, queue := range queueOperators {
		// queue for mo tarifficate requests
		log.Info("initialising operator " + operatorName)
		var err error
		svc.MOTarifficateRequestsChan[operatorName], err =
			svc.consumer.AnnounceQueue(
				queue.MOTarifficate,
				queue.MOTarifficate)
		if err != nil {
			log.WithFields(log.Fields{
				"queue": queue.NewSubscription,
				"error": err.Error(),
			}).Fatal("rbmq consumer: AnnounceQueue")
		}
		go svc.consumer.Handle(
			svc.MOTarifficateRequestsChan[operatorName],
			processSubscriptions,
			sConf.ThreadsCount,
			queue.MOTarifficate,
			queue.MOTarifficate,
		)
		log.Info(queue.MOTarifficate + " consume queue init done")

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
		log.Info(queue.Responses + " consume queue init done")
	}
}

type MTService struct {
	MOTarifficateRequestsChan        map[string]<-chan amqp_driver.Delivery
	operatorTarifficateResponsesChan map[string]<-chan amqp_driver.Delivery
	operatorRequestQueueFree         map[string]chan struct{}
	conf                             MTServiceConfig
	dbConf                           db.DataBaseConfig
	publisher                        *rabbit.Notifier
	consumer                         *rabbit.Consumer
}
type OperatorQueueConfig struct {
	NewSubscription string `yaml:"-"`
	Requests        string `yaml:"-"`
	Responses       string `yaml:"-"`
	SMS             string `yaml:"-"`
}
type QueuesConfig struct {
	Pixels string `default:"pixels" yaml:"pixels"`
}
type OperatorConfig struct {
	Name           string `yaml:"name"`
	RetriesEnabled bool   `yaml:"retries_enabled"`
}
type MTServiceConfig struct {
	RetrySec       int                                         `default:"600" yaml:"retry_period"`
	RetryCount     int                                         `default:"600" yaml:"retry_count"`
	ThreadsCount   int                                         `default:"1" yaml:"threads_count"`
	Queues         QueuesConfig                                `yaml:"queues"`
	QueueOperators map[string]queue_config.OperatorQueueConfig `yaml:"-"`
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
		"data":  fmt.Sprintf("%#v", msg),
		"queue": queue,
		"event": eventName,
	}).Debug("prepare to send")
	svc.publisher.Publish(rabbit.AMQPMessage{queue, body})
	return nil
}
