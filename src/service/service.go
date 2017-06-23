package service

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"

	content_client "github.com/linkit360/go-contentd/rpcclient"
	content_service "github.com/linkit360/go-contentd/server/src/service"
	mid_client "github.com/linkit360/go-mid/rpcclient"
	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/db"
	rec "github.com/linkit360/go-utils/rec"
)

var svc MTService
var appName string

type MTService struct {
	conf      MTServiceConfig
	retriesWg map[int64]*sync.WaitGroup
	notifier  *amqp.Notifier
	y         *yondu
	mb        *mobilink
	ch        *cheese
	qr        *qrtech
	bee       *beeline
}
type MTServiceConfig struct {
	Queues   QueuesConfig   `yaml:"queues"`
	Mobilink MobilinkConfig `yaml:"mobilink,omitempty"`
	Yondu    YonduConfig    `yaml:"yondu,omitempty"`
	Cheese   CheeseConfig   `yaml:"cheese,omitempty"`
	QRTech   QRTechConfig   `yaml:"qrtech,omitempty"`
	Beeline  BeelineConfig  `yaml:"beeline,omitempty"`
}

type QueuesConfig struct {
	Pixels              string `default:"pixels" yaml:"pixels"`
	RestorePixels       string `default:"restore_pixels" yaml:"restore_pixels"`
	DBActions           string `default:"mt_manager" yaml:"db_actions"`
	TransactionLog      string `default:"transaction_log" yaml:"transaction_log"`
	ReporterTransaction string `default:"reporter_transaction" yaml:"reporter_transaction"`
}
type RetriesConfig struct {
	Enabled         bool     `yaml:"enabled"`
	Period          int      `yaml:"period" default:"600"`
	FetchLimit      int      `yaml:"fetch_limit" default:"2500"`
	PaidOnceHours   int      `yaml:"paid_once_hours" default:"0"` // default must be 0
	CheckQueuesFree []string `yaml:"check_queues_free"`
	QueueFreeSize   int      `yaml:"queue_free_size" default:"2500"`
}

func Init(
	name string,
	serviceConf MTServiceConfig,
	midConfig mid_client.ClientConfig,
	dbConf db.DataBaseConfig,
	publisherConf amqp.NotifierConfig,
	consumerConfig amqp.ConsumerConfig,
	contentConfig content_client.ClientConfig,

) {
	appName = name
	log.SetLevel(log.DebugLevel)
	svc.notifier = amqp.NewNotifier(publisherConf)

	rec.Init(dbConf)

	if err := mid_client.Init(midConfig); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init mid client")
	}

	svc.conf = serviceConf
	svc.retriesWg = make(map[int64]*sync.WaitGroup)
	initMetrics(appName)

	svc.mb = initMobilink(serviceConf.Mobilink, consumerConfig, contentConfig)
	svc.y = initYondu(serviceConf.Yondu, consumerConfig, contentConfig)
	svc.ch = initCheese(serviceConf.Cheese, consumerConfig)
	svc.qr = initQRTech(serviceConf.QRTech, consumerConfig)
	svc.bee = initBeeline(serviceConf.Beeline, consumerConfig)
}

func publish(queueName, eventName string, r rec.Record, priorityOptional ...uint8) (err error) {
	priority := uint8(0)
	if len(priorityOptional) > 0 {
		priority = priorityOptional[0]
	}

	event := amqp.EventNotify{
		EventName: eventName,
		EventData: r,
	}
	var body []byte
	body, err = json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return
	}
	svc.notifier.Publish(amqp.AMQPMessage{
		EventName: eventName,
		QueueName: queueName,
		Priority:  priority,
		Body:      body,
	})
	return nil
}

func getContentUniqueHash(r rec.Record) (string, error) {
	logCtx := log.WithFields(log.Fields{
		"tid": r.Tid,
	})
	contentProperties, err := content_client.GetUniqueUrl(content_service.GetContentParams{
		Msisdn:         r.Msisdn,
		Tid:            r.Tid,
		ServiceCode:    r.ServiceCode,
		CampaignId:     r.CampaignId,
		OperatorCode:   r.OperatorCode,
		CountryCode:    r.CountryCode,
		SubscriptionId: r.SubscriptionId,
	})

	if contentProperties.Error != "" {
		ContentdRPCDialError.Inc()
		err = fmt.Errorf("content_client.GetUniqueUrl: %s", contentProperties.Error)
		logCtx.WithFields(log.Fields{
			"serviceId": r.ServiceCode,
			"error":     err.Error(),
		}).Error("contentd internal error")
		return "", err
	}
	if err != nil {
		ContentdRPCDialError.Inc()
		err = fmt.Errorf("content_client.GetUniqueUrl: %s", err.Error())
		logCtx.WithFields(log.Fields{
			"serviceId": r.ServiceCode,
			"error":     err.Error(),
		}).Error("cannot get unique content url")
		return "", err
	}

	return contentProperties.UniqueUrl, nil

}
func SaveState() {
	log.WithField("pid", os.Getpid()).Info("save state")
}
