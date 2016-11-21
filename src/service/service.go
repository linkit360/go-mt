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
	publisherConf amqp.NotifierConfig,
	consumerConfig amqp.ConsumerConfig,

) {
	log.SetLevel(log.DebugLevel)

	svc.conf = sConf
	svc.dbConf = dbConf
	svc.conf.QueueOperators = queueOperators
	rec.Init(dbConf)

	initMetrics()

	svc.publisher = amqp.NewNotifier(publisherConf)

	if err := initInMem(dbConf); err != nil {
		log.WithField("error", err.Error()).Fatal("init in memory tables")
	}
	log.Info("inmemory tables init ok")

	// if the operator requests queue size is less than the amount if items specified in config
	// then - get records from database and make retries
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
					continue
				}
				log.WithFields(log.Fields{
					"operator":  operatorName,
					"queue":     queue.Requests,
					"queueSize": queueSize,
				}).Debug("got size")

				if len(svc.operatorRequestQueueFree[operatorName]) == 0 &&
					queueSize < operatorConfig[operatorName].OperatorRequestQueueSize {

					svc.operatorRequestQueueFree[operatorName] <- struct{}{}
					log.WithFields(log.Fields{
						"operator":  operatorName,
						"queue":     queue.Requests,
						"queueSize": queueSize,
					}).Debug("sent signal to start procesing")
				}
			}
		}
	}()

	// get a signal and send them to operator requests queue in rabbitmq
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

	// create new consumer
	svc.consumer = amqp.NewConsumer(consumerConfig)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}

	svc.MOTarifficateRequestsChan = make(map[string]<-chan amqp_driver.Delivery, len(operatorConfig))
	svc.operatorTarifficateResponsesChan = make(map[string]<-chan amqp_driver.Delivery, len(operatorConfig))
	svc.operatorSMSResponsesChan = make(map[string]<-chan amqp_driver.Delivery, len(operatorConfig))

	for operatorName, queue := range queueOperators {
		log.Info("initialising operator " + operatorName)

		// queue for mo tarifficate requests
		amqp.InitQueue(
			svc.consumer,
			svc.MOTarifficateRequestsChan[operatorName],
			processSubscriptions,
			sConf.ThreadsCount,
			queue.MOTarifficate,
			queue.MOTarifficate,
		)

		// queue for responses
		amqp.InitQueue(
			svc.consumer,
			svc.operatorTarifficateResponsesChan[operatorName],
			processResponses,
			sConf.ThreadsCount,
			queue.Responses,
			queue.Responses,
		)

		// queue for sms responses
		amqp.InitQueue(
			svc.consumer,
			svc.operatorSMSResponsesChan[operatorName],
			processSMSResponses,
			sConf.ThreadsCount,
			queue.SMSResponse,
			queue.SMSResponse,
		)
	}
}

type MTService struct {
	MOTarifficateRequestsChan        map[string]<-chan amqp_driver.Delivery
	operatorTarifficateResponsesChan map[string]<-chan amqp_driver.Delivery
	operatorSMSResponsesChan         map[string]<-chan amqp_driver.Delivery
	operatorRequestQueueFree         map[string]chan struct{}
	conf                             MTServiceConfig
	dbConf                           db.DataBaseConfig
	publisher                        *amqp.Notifier
	consumer                         *amqp.Consumer
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

	eventName := "pixels"
	event := amqp.EventNotify{
		EventName: eventName,
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	log.WithField("body", string(body)).Debug("sent pixels")
	log.WithFields(log.Fields{
		"data":  fmt.Sprintf("%#v", msg),
		"queue": svc.conf.Queues.Pixels,
		"event": eventName,
	}).Debug("sent")
	svc.publisher.Publish(amqp.AMQPMessage{svc.conf.Queues.Pixels, body})
	return nil
}

func notifyOperatorRequest(queue, eventName string, msg interface{}) error {
	if eventName == "" {
		return fmt.Errorf("QueueSend: %s", "empty event name")
	}
	if queue == "" {
		return fmt.Errorf("QueueSend: %s", "empty queue name")
	}

	event := amqp.EventNotify{
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
	}).Debug("sent")
	svc.publisher.Publish(amqp.AMQPMessage{queue, body})
	return nil
}
