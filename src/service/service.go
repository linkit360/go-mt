package service

import (
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	cache "github.com/patrickmn/go-cache"
	amqp_driver "github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/config"
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
	Yondu    YonduConfig    `yaml:"yondu"`
	Mobilink MobilinkConfig `yaml:"mobilink"`
}
type QueuesConfig struct {
	Pixels         string `default:"pixels" yaml:"pixels"`
	DBActions      string `default:"mt_manager" yaml:"db_actions"`
	TransactionLog string `default:"transactions_log" yaml:"transactions_log"`
}

func Init(
	name string,
	serviceConf MTServiceConfig,
	inMemConfig inmem_client.RPCClientConfig,
	dbConf db.DataBaseConfig,
	publisherConf amqp.NotifierConfig,
	consumerConfig amqp.ConsumerConfig,

) {
	appName = name
	log.SetLevel(log.DebugLevel)
	rec.Init(dbConf)

	if err := inmem_client.Init(inMemConfig); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init inmem client")
	}

	svc.conf = serviceConf
	svc.retriesWg = make(map[int64]*sync.WaitGroup)
	initMetrics(appName)

	svc.mb = initMobilink(serviceConf.Mobilink, consumerConfig)
	svc.y = initYondu(serviceConf.Yondu, consumerConfig)

	svc.notifier = amqp.NewNotifier(publisherConf)
}
