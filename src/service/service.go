package service

import (
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"

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
	ldb       *leveldb.DB
	y         *yondu
	mb        *mobilink
}
type MTServiceConfig struct {
	LevelDBFilePath string         `yaml:"leveldb_file"`
	Queues          QueuesConfig   `yaml:"queues"`
	Yondu           YonduConfig    `yaml:"yondu"`
	Mobilink        MobilinkConfig `yaml:"mobilink"`
}
type QueuesConfig struct {
	Pixels         string `default:"pixels" yaml:"pixels"`
	DBActions      string `default:"mt_manager" yaml:"db_actions"`
	TransactionLog string `default:"transactions_log" yaml:"transactions_log"`
}

type RetriesConfig struct {
	Enabled         bool     `yaml:"enabled" default:"true"`
	FetchCount      int      `yaml:"fetch_count" default:"2500"`
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

	var err error
	svc.ldb, err = leveldb.OpenFile(serviceConf.LevelDBFilePath, nil)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init leveldb")
	}

	svc.mb = initMobilink(serviceConf.Mobilink, consumerConfig)
	svc.y = initYondu(serviceConf.Yondu, consumerConfig)

	svc.notifier = amqp.NewNotifier(publisherConf)
}

func SaveState() {
	log.WithField("savestate", 1).Info("save leveldb state")
	svc.ldb.Close()
}
