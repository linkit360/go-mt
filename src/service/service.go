package service

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	cache "github.com/patrickmn/go-cache"
	amqp_driver "github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	pixels "github.com/vostrok/pixels/src/notifier"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/config"
	"github.com/vostrok/utils/db"
	rec "github.com/vostrok/utils/rec"
)

// queues:
// in: new subscription, reponses, (?sms_responses)
// out: pixels, requests, send_sms
var svc MTService

func Init(
	serviceConf MTServiceConfig,
	inMemConfig inmem_client.RPCClientConfig,
	dbConf db.DataBaseConfig,
	publisherConf amqp.NotifierConfig,
	consumerConfig amqp.ConsumerConfig,

) {
	log.SetLevel(log.DebugLevel)
	rec.Init(dbConf)
	initCache()

	if err := inmem_client.Init(inMemConfig); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init inmem client")
	}
	svc.retriesWg = make(map[int64]*sync.WaitGroup, len(serviceConf.Operators))

	svc.conf = serviceConf
	svc.dbConf = dbConf

	initMetrics()

	svc.notifier = amqp.NewNotifier(publisherConf)

	// get a signal and send them to operator requests queue in rabbitmq
	go func() {
		for range time.Tick(time.Second) {
			for operatorName, operatorConf := range serviceConf.Operators {
				if !operatorConf.Enabled || !operatorConf.Retries.Enabled {
					log.WithFields(log.Fields{
						"operator": operatorName,
					}).Debug("not enabled")
					continue
				}
				queueSize, err := svc.notifier.GetQueueSize(operatorConf.GetRequestsQueueName())
				if err != nil {
					log.WithFields(log.Fields{
						"operator": operatorName,
						"error":    err.Error(),
					}).Error("cannot get queue size")
					continue
				}

				queueResponsesSize, err := svc.notifier.GetQueueSize(operatorConf.GetResponsesQueueName())
				if err != nil {
					log.WithFields(log.Fields{
						"operator": operatorName,
						"error":    err.Error(),
					}).Error("cannot get queue size")
					continue
				}

				log.WithFields(log.Fields{
					"operator":       operatorName,
					"queueRequests":  queueSize,
					"queueResponses": queueResponsesSize,
					"waitFor":        operatorConf.Retries.QueueSize,
				}).Debug("")
				if queueSize <= operatorConf.Retries.QueueSize &&
					queueResponsesSize <= operatorConf.Retries.QueueSize {
					operator, err := inmem_client.GetOperatorByName(operatorName)
					if err != nil {
						log.WithFields(log.Fields{
							"error":    err.Error(),
							"operator": operatorName,
						}).Error("cannot find operator by operator name")
						continue
					}
					// XXX: if more than two operators work,
					// another wait, it's not ok
					processRetries(operator.Code, operatorConf.Retries.FromDBCount)
				}
			}
		}
	}()

	svc.consumer = make(map[string]Consumers, len(serviceConf.Operators))
	svc.MOTarifficateRequestsChan = make(map[string]<-chan amqp_driver.Delivery, len(serviceConf.Operators))
	svc.operatorTarifficateResponsesChan = make(map[string]<-chan amqp_driver.Delivery, len(serviceConf.Operators))
	svc.operatorSMSResponsesChan = make(map[string]<-chan amqp_driver.Delivery, len(serviceConf.Operators))

	for operatorName, opConf := range serviceConf.Operators {
		if !opConf.Enabled {
			log.WithFields(log.Fields{
				"operator": operatorName,
				"enabled":  "false",
			}).Debug("skip")
			continue
		}

		log.Info("initialising operator " + operatorName)
		moConsumeSettings, ok := serviceConf.ConsumeSettings[operatorName]["mo"]
		if !ok {
			log.WithFields(log.Fields{
				"operator": operatorName,
				"type":     "mo",
			}).Fatal("no consume settings")
		}
		responsesConsumeSettings, ok := serviceConf.ConsumeSettings[operatorName]["responses"]
		if !ok {
			log.WithFields(log.Fields{
				"operator": operatorName,
				"type":     "responses",
			}).Fatal("no consume settings")
		}
		smsConsumeSettings, ok := serviceConf.ConsumeSettings[operatorName]["sms_responses"]
		if !ok {
			log.WithFields(log.Fields{
				"operator": operatorName,
				"type":     "sms_responses",
			}).Fatal("no consume settings")
		}

		svc.consumer[operatorName] = Consumers{
			Mo:           amqp.NewConsumer(consumerConfig, opConf.GetMOQueueName(), moConsumeSettings.PrefetchCount),
			Responses:    amqp.NewConsumer(consumerConfig, opConf.GetResponsesQueueName(), responsesConsumeSettings.PrefetchCount),
			SMSResponses: amqp.NewConsumer(consumerConfig, opConf.GetSMSResponsesQueueName(), smsConsumeSettings.PrefetchCount),
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
			moConsumeSettings.ThreadsCount,
			opConf.GetMOQueueName(),
			opConf.GetMOQueueName(),
		)

		// queue for responses
		amqp.InitQueue(
			svc.consumer[operatorName].Responses,
			svc.operatorTarifficateResponsesChan[operatorName],
			processResponses,
			responsesConsumeSettings.ThreadsCount,
			opConf.GetResponsesQueueName(),
			opConf.GetResponsesQueueName(),
		)

		// queue for sms responses
		amqp.InitQueue(
			svc.consumer[operatorName].SMSResponses,
			svc.operatorSMSResponsesChan[operatorName],
			processSMSResponses,
			smsConsumeSettings.ThreadsCount,
			opConf.GetSMSResponsesQueueName(),
			opConf.GetSMSResponsesQueueName(),
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
	prevCache                        *cache.Cache
	retriesWg                        map[int64]*sync.WaitGroup
}

type Consumers struct {
	Mo           *amqp.Consumer
	Responses    *amqp.Consumer
	SMSResponses *amqp.Consumer
}

type QueuesConfig struct {
	Pixels    string `default:"pixels" yaml:"pixels"`
	DBActions string `default:"mt_manager" yaml:"db_actions"`
}
type ConsumeSettingsConfig map[string]config.ConsumeQueueConfig

type MTServiceConfig struct {
	Queues          QueuesConfig                     `yaml:"queues"`
	Operators       map[string]config.OperatorConfig `yaml:"operators"`
	ConsumeSettings map[string]ConsumeSettingsConfig `yaml:"consume_queues"`
}

func notifyPixel(msg pixels.Pixel) (err error) {
	defer func() {
		fields := log.Fields{
			"tid":   msg.Tid,
			"queue": svc.conf.Queues.Pixels,
		}
		if err != nil {
			fields["errors"] = err.Error()
			fields["pixel"] = fmt.Sprintf("%#v", msg)
			log.WithFields(fields).Error("cannot enqueue")
		} else {
			log.WithFields(fields).Debug("sent")
		}
	}()
	eventName := "pixels"
	event := amqp.EventNotify{
		EventName: eventName,
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return err
	}
	svc.notifier.Publish(amqp.AMQPMessage{svc.conf.Queues.Pixels, uint8(0), body})
	return nil
}

func notifyOperatorRequest(queue string, priority uint8, eventName string, msg rec.Record) (err error) {

	defer func() {
		fields := log.Fields{
			"tid":   msg.Tid,
			"queue": queue,
			"event": eventName,
		}
		if err != nil {
			fields["errors"] = err.Error()
			fields["record"] = fmt.Sprintf("%#v", msg)
			log.WithFields(fields).Error("cannot enqueue")
		} else {
			log.WithFields(fields).Debug("sent")
		}
	}()
	if eventName == "" {
		err = fmt.Errorf("QueueSend: %s", "empty event name")
		return
	}
	if queue == "" {
		err = fmt.Errorf("QueueSend: %s", "empty queue name")
		return
	}

	event := amqp.EventNotify{
		EventName: eventName,
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return
	}
	svc.notifier.Publish(amqp.AMQPMessage{
		QueueName: queue,
		Priority:  priority,
		Body:      body,
	})
	return nil
}

func initCache() {
	prev, err := rec.LoadPreviousSubscriptions()
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load previous subscriptions")
	}
	log.WithField("count", len(prev)).Debug("loaded previous subscriptions")
	svc.prevCache = cache.New(24*time.Hour, time.Minute)
	for _, v := range prev {
		key := v.Msisdn + strconv.FormatInt(v.ServiceId, 10)
		svc.prevCache.Set(key, struct{}{}, time.Now().Sub(v.CreatedAt))
	}
}
func getPrevSubscriptionCache(msisdn string, serviceId int64, tid string) bool {
	key := msisdn + strconv.FormatInt(serviceId, 10)
	_, found := svc.prevCache.Get(key)
	log.WithFields(log.Fields{
		"tid":   tid,
		"key":   key,
		"found": found,
	}).Debug("get previous subscription cache")
	return found
}
func setPrevSubscriptionCache(msisdn string, serviceId int64, tid string) {
	key := msisdn + strconv.FormatInt(serviceId, 10)
	_, found := svc.prevCache.Get(key)
	if !found {
		svc.prevCache.Set(key, struct{}{}, 24*time.Hour)
		log.WithFields(log.Fields{
			"tid": tid,
			"key": key,
		}).Debug("set previous subscription cache")
	}
}

func startRetry(msg rec.Record) (err error) {
	return _notifyDBAction("StartRetry", msg)
}
func addPostPaidNumber(msg rec.Record) (err error) {
	return _notifyDBAction("AddPostPaidNumber", msg)
}
func touchRetry(msg rec.Record) (err error) {
	return _notifyDBAction("TouchRetry", msg)
}
func removeRetry(msg rec.Record) (err error) {
	return _notifyDBAction("RemoveRetry", msg)
}
func writeSubscriptionStatus(msg rec.Record) (err error) {
	return _notifyDBAction("WriteSubscriptionStatus", msg)
}
func writeTransaction(msg rec.Record) (err error) {
	return _notifyDBAction("WriteTransaction", msg)
}
func _notifyDBAction(eventName string, msg rec.Record) (err error) {
	msg.SentAt = time.Now().UTC()
	defer func() {
		if err != nil {
			NotifyErrors.Inc()
			fields := log.Fields{
				"data":  fmt.Sprintf("%#v", msg),
				"queue": svc.conf.Queues.DBActions,
				"event": eventName,
			}
			fields["error"] = fmt.Errorf(eventName+": %s", err.Error())
			fields["rec"] = fmt.Sprintf("%#v", msg)
			log.WithFields(fields).Error("cannot send")
		}
	}()

	if eventName == "" {
		err = fmt.Errorf("QueueSend: %s", "Empty event name")
		return
	}

	event := amqp.EventNotify{
		EventName: eventName,
		EventData: msg,
	}
	var body []byte
	body, err = json.Marshal(event)

	if err != nil {
		err = fmt.Errorf(eventName+" json.Marshal: %s", err.Error())
		return
	}
	svc.notifier.Publish(amqp.AMQPMessage{
		QueueName: svc.conf.Queues.DBActions,
		Body:      body,
	})
	return nil
}
