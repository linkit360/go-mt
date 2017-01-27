package service

import (
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"

	content_client "github.com/vostrok/contentd/rpcclient"
	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
	rec "github.com/vostrok/utils/rec"
)

var svc MTService
var appName string

type MTService struct {
	conf      MTServiceConfig
	retriesWg map[int64]*sync.WaitGroup
	notifier  *amqp.Notifier
	y         *yondu
	mb        *mobilink
}
type MTServiceConfig struct {
	Queues   QueuesConfig   `yaml:"queues"`
	Mobilink MobilinkConfig `yaml:"mobilink,omitempty"`
	Yondu    YonduConfig    `yaml:"yondu,omitempty"`
}
type QueuesConfig struct {
	Pixels         string `default:"pixels" yaml:"pixels"`
	DBActions      string `default:"mt_manager" yaml:"db_actions"`
	TransactionLog string `default:"transaction_log" yaml:"transaction_log"`
}
type RetriesConfig struct {
	Enabled         bool     `yaml:"enabled" default:"true"`
	Period          int      `yaml:"period" default:"600"`
	FetchLimit      int      `yaml:"fetch_limit" default:"2500"`
	PaidOnceHours   int      `yaml:"paid_once_hours" default:"0"` // default must be 0
	CheckQueuesFree []string `yaml:"check_queues_free"`
	QueueFreeSize   int      `yaml:"queue_free_size" default:"2500"`
}

func Init(
	name string,
	serviceConf MTServiceConfig,
	inMemConfig inmem_client.RPCClientConfig,
	dbConf db.DataBaseConfig,
	publisherConf amqp.NotifierConfig,
	consumerConfig amqp.ConsumerConfig,
	contentConfig content_client.RPCClientConfig,

) {
	appName = name
	log.SetLevel(log.DebugLevel)
	svc.notifier = amqp.NewNotifier(publisherConf)
	svc.notifier.RestoreState()

	rec.Init(dbConf)

	if err := inmem_client.Init(inMemConfig); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init inmem client")
	}
	svc.conf = serviceConf
	svc.retriesWg = make(map[int64]*sync.WaitGroup)
	initMetrics(appName)

	svc.mb = initMobilink(serviceConf.Mobilink, consumerConfig)
	svc.y = initYondu(serviceConf.Yondu, consumerConfig, contentConfig)
}

func SaveState() {
	log.WithField("pid", os.Getpid()).Info("save leveldb state")
	svc.notifier.SaveState()
}
