package service

import (
	"fmt"
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"

	content_client "github.com/linkit360/go-contentd/rpcclient"
	inmem_client "github.com/linkit360/go-inmem/rpcclient"
	reporter_client "github.com/linkit360/go-reporter/rpcclient"
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
	Pixels         string `default:"pixels" yaml:"pixels"`
	RestorePixels  string `default:"restore_pixels" yaml:"restore_pixels"`
	DBActions      string `default:"mt_manager" yaml:"db_actions"`
	TransactionLog string `default:"transaction_log" yaml:"transaction_log"`
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
	inMemConfig inmem_client.ClientConfig,
	reporterConfig reporter_client.ClientConfig,
	dbConf db.DataBaseConfig,
	publisherConf amqp.NotifierConfig,
	consumerConfig amqp.ConsumerConfig,
	contentConfig content_client.ClientConfig,

) {
	appName = name
	log.SetLevel(log.DebugLevel)
	svc.notifier = amqp.NewNotifier(publisherConf)

	rec.Init(dbConf)

	if err := inmem_client.Init(inMemConfig); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init inmem client")
	}
	if err := reporter_client.Init(reporterConfig); err != nil && reporterConfig.Enabled {
		log.Error(fmt.Errorf("reporter_client.Init: %s", err.Error()))
	}

	svc.conf = serviceConf
	svc.retriesWg = make(map[int64]*sync.WaitGroup)
	initMetrics(appName)

	svc.mb = initMobilink(serviceConf.Mobilink, consumerConfig)
	svc.y = initYondu(serviceConf.Yondu, consumerConfig, contentConfig)
	svc.ch = initCheese(serviceConf.Cheese, consumerConfig)
	svc.qr = initQRTech(serviceConf.QRTech, consumerConfig)
	svc.bee = initBeeline(serviceConf.Beeline, consumerConfig)
}

func SaveState() {
	log.WithField("pid", os.Getpid()).Info("save state")
}
