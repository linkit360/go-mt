package mobilink

import (
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
	smpp_client "github.com/fiorix/go-smpp/smpp"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"

	rec "github.com/vostrok/mt_manager/src/service/instance"
	"github.com/vostrok/mt_manager/src/service/notifier"
	"os"
)

type Mobilink struct {
	conf            Config
	rps             int
	mtChannel       chan rec.Record
	Response        chan rec.Record
	M               Metrics
	location        *time.Location
	client          *http.Client
	smpp            *smpp_client.Transmitter
	responseLog     *log.Logger
	requestLog      *log.Logger
	transactionsLog notifier.Notifier
}
type Config struct {
	Enabled        bool                 `default:"true" yaml:"enabled"`
	MTChanCapacity int                  `default:"1000" yaml:"channel_camacity"`
	Connection     ConnnectionConfig    `yaml:"connection"`
	Location       string               `default:"Asia/Karachi" yaml:"location"`
	TransactionLog TransactionLogConfig `yaml:"log_transaction"`
}
type TransactionLogConfig struct {
	ResponseLogPath string `default:"/var/log/response_mobilink.log" yaml:"response"`
	RequestLogPath  string `default:"/var/log/request_mobilink.log" yaml:"request"`
}

type ConnnectionConfig struct {
	MT   MTConfig   `yaml:"mt" json:"mt"`
	Smpp SmppConfig `yaml:"smpp" json:"smpp"`
}
type SmppConfig struct {
	ShortNumber string `default:"4162" yaml:"short_number" json:"short_number"`
	Addr        string `default:"182.16.255.46:15019" yaml:"endpoint"`
	User        string `default:"SLYEPPLA" yaml:"user"`
	Password    string `default:"SLYPEE_1" yaml:"pass"`
	Timeout     int    `default:"20" yaml:"timeout"`
}

type MTConfig struct {
	Url                  string            `default:"http://182.16.255.46:10020/Air" yaml:"url" json:"url"`
	Headers              map[string]string `yaml:"headers" json:"headers"`
	TimeoutSec           int               `default:"20" yaml:"timeout" json:"timeout"`
	TarifficateBody      string            `yaml:"mt_body"`
	PaidBodyContains     []string          `yaml:"paid_body_contains" json:"paid_body_contains"`
	CheckBalanceBody     string            `yaml:"check_balance_body"`
	PostPaidBodyContains []string          `yaml:"postpaid_body_contains" json:"postpaid_body_contains"`
}

type Metrics struct {
	SMPPConnected   metrics.Gauge
	ResponseLen     metrics.Gauge
	PendingRequests metrics.Gauge
}

func initMetrics() Metrics {
	return Metrics{
		SMPPConnected:   expvar.NewGauge("mobilink_smpp_connected"),
		ResponseLen:     expvar.NewGauge("mobilink_responses_queue"),
		PendingRequests: expvar.NewGauge("mobilink_request_queue"),
	}
}

// todo: chan gap cannot be too big bzs of the size

func Init(
	mobilinkRps int,
	mobilinkConf Config,
	transactionsLog notifier.Notifier,
) *Mobilink {
	mb := &Mobilink{
		rps:  mobilinkRps,
		conf: mobilinkConf,
		M:    initMetrics(),
	}
	log.Info("mb metrics init done")

	mb.transactionsLog = transactionsLog

	mb.requestLog = getLogger(mobilinkConf.TransactionLog.RequestLogPath)
	log.Info("request logger init done")

	mb.responseLog = getLogger(mobilinkConf.TransactionLog.ResponseLogPath)
	log.Info("response logger init done")

	mb.mtChannel = make(chan rec.Record, mobilinkConf.MTChanCapacity)
	mb.Response = make(chan rec.Record, mobilinkConf.MTChanCapacity)
	log.WithField("capacity", mobilinkConf.MTChanCapacity).Info("channels ini done")

	var err error
	mb.location, err = time.LoadLocation(mobilinkConf.Location)
	if err != nil {
		log.WithFields(log.Fields{
			"location": mobilinkConf.Location,
			"error":    err,
		}).Fatal("init location")
	}
	log.Info("location init done")

	mb.client = &http.Client{
		Timeout: time.Duration(mobilinkConf.Connection.MT.TimeoutSec) * time.Second,
	}
	log.Info("http client init done")

	mb.smpp = &smpp_client.Transmitter{
		Addr:        mobilinkConf.Connection.Smpp.Addr,
		User:        mobilinkConf.Connection.Smpp.User,
		Passwd:      mobilinkConf.Connection.Smpp.Password,
		RespTimeout: time.Duration(mobilinkConf.Connection.Smpp.Timeout) * time.Second,
		SystemType:  "SMPP",
	}
	log.Info("smpp client init done")

	connStatus := mb.smpp.Bind()
	go func() {
		for c := range connStatus {
			if c.Status().String() != "Connected" {
				mb.M.SMPPConnected.Set(0)
				log.WithFields(log.Fields{
					"operator": "mobilink",
					"status":   c.Status().String(),
					"error":    "disconnected:" + c.Status().String(),
				}).Error("smpp moblink connect status")
			} else {
				log.WithFields(log.Fields{
					"operator": "mobilink",
					"status":   c.Status().String(),
				}).Info("smpp moblink connect status")
				mb.M.SMPPConnected.Set(1)
			}
		}
	}()

	go func() {
		mb.mtReader()
	}()

	return mb
}
func getLogger(path string) *log.Logger {
	handler, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		log.WithFields(log.Fields{
			"path":  path,
			"error": err.Error(),
		}).Fatal("cannot open file")
	}
	return &log.Logger{
		Out:       handler,
		Formatter: new(log.TextFormatter),
		Hooks:     make(log.LevelHooks),
		Level:     log.DebugLevel,
	}
}
