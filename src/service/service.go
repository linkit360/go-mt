package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
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
	inMemConfig inmem_client.RPCClientConfig,
	operatorConfig map[string]queue_config.OperatorConfig,
	queueOperators map[string]queue_config.OperatorQueueConfig,
	dbConf db.DataBaseConfig,
	publisherConf amqp.NotifierConfig,
	consumerConfig amqp.ConsumerConfig,

) {
	log.SetLevel(log.DebugLevel)

	if err := inmem_client.Init(inMemConfig); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init inmem client")
	}

	svc.conf = sConf
	svc.dbConf = dbConf
	svc.conf.QueueOperators = queueOperators
	rec.Init(dbConf)

	initMetrics()

	svc.notifier = amqp.NewNotifier(publisherConf)

	// get a signal and send them to operator requests queue in rabbitmq
	go func() {
		for range time.Tick(time.Second) {
			for operatorName, operatorConf := range operatorConfig {
				if !operatorConf.RetriesEnabled {
					continue
				}
				queue, ok := queueOperators[operatorName]
				if !ok {
					log.WithFields(log.Fields{
						"operator": operatorName,
					}).Fatal("queue is not defined")
				}
				queueSize, err := svc.notifier.GetQueueSize(queue.Requests)
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
					"wiatFor":   operatorConf.OperatorRequestQueueSize,
					"cond":      queueSize <= operatorConf.OperatorRequestQueueSize,
				}).Debug("got queue size")
				if queueSize <= operatorConf.OperatorRequestQueueSize {
					operator, err := inmem_client.GetOperatorByName(operatorName)
					if err != nil {
						log.WithFields(log.Fields{
							"error":    err.Error(),
							"operator": operatorName,
						}).Error("cannot find operator by operator name")
						continue
					}
					processRetries(operator.Code, operatorConf.GetFromDBRetryCount)
				}

			}
		}
	}()

	svc.consumer = make(map[string]Consumers, len(operatorConfig))
	svc.MOTarifficateRequestsChan = make(map[string]<-chan amqp_driver.Delivery, len(operatorConfig))
	svc.operatorTarifficateResponsesChan = make(map[string]<-chan amqp_driver.Delivery, len(operatorConfig))
	svc.operatorSMSResponsesChan = make(map[string]<-chan amqp_driver.Delivery, len(operatorConfig))

	for operatorName, queue := range queueOperators {
		log.Info("initialising operator " + operatorName)

		svc.consumer[operatorName] = Consumers{
			Mo:           amqp.NewConsumer(consumerConfig, queue.MOTarifficate),
			Responses:    amqp.NewConsumer(consumerConfig, queue.Responses),
			SMSResponses: amqp.NewConsumer(consumerConfig, queue.SMSResponse),
		}
		if err := svc.consumer[operatorName].Mo.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		if err := svc.consumer[operatorName].Responses.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}
		if err := svc.consumer[operatorName].SMSResponses.Connect(); err != nil {
			log.Fatal("rbmq consumer connect:", err.Error())
		}

		// queue for mo tarifficate requests
		amqp.InitQueue(
			svc.consumer[operatorName].Mo,
			svc.MOTarifficateRequestsChan[operatorName],
			processSubscriptions,
			sConf.ThreadsCount,
			queue.MOTarifficate,
			queue.MOTarifficate,
		)

		// queue for responses
		amqp.InitQueue(
			svc.consumer[operatorName].Responses,
			svc.operatorTarifficateResponsesChan[operatorName],
			processResponses,
			sConf.ThreadsCount,
			queue.Responses,
			queue.Responses,
		)

		// queue for sms responses
		amqp.InitQueue(
			svc.consumer[operatorName].SMSResponses,
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
	conf                             MTServiceConfig
	dbConf                           db.DataBaseConfig
	notifier                         *amqp.Notifier
	consumer                         map[string]Consumers
}

type Consumers struct {
	Mo           *amqp.Consumer
	Responses    *amqp.Consumer
	SMSResponses *amqp.Consumer
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
	svc.notifier.Publish(amqp.AMQPMessage{svc.conf.Queues.Pixels, uint8(0), body})
	return nil
}

func notifyOperatorRequest(queue string, priority uint8, eventName string, msg interface{}) error {
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
	svc.notifier.Publish(amqp.AMQPMessage{
		QueueName: queue,
		Priority:  priority,
		Body:      body,
	})
	return nil
}
