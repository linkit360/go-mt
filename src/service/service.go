package service

import (
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"

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
	ldb       *leveldb.DB
	y         *yondu
	mb        *mobilink
}
type MTServiceConfig struct {
	LevelDBFilePath string         `yaml:"leveldb_file"`
	Queues          QueuesConfig   `yaml:"queues"`
	Yondu           YonduConfig    `yaml:"yondu,omitempty"`
	Mobilink        MobilinkConfig `yaml:"mobilink,omitempty"`
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

	var err error
	svc.ldb, err = leveldb.OpenFile(serviceConf.LevelDBFilePath, nil)
	if err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init leveldb")
	}

	svc.mb = initMobilink(serviceConf.Mobilink, consumerConfig)
	svc.y = initYondu(serviceConf.Yondu, consumerConfig, contentConfig)
}

func SaveState() {
	log.WithField("pid", os.Getpid()).Info("save leveldb state")
	svc.ldb.Close()
	svc.notifier.SaveState()
}
