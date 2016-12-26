package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/mt_manager/src/service"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
)

type ServerConfig struct {
	Port string `default:"50304"`
}
type AppConfig struct {
	MetricInstancePrefix string                       `yaml:"metric_instance_prefix"`
	AppName              string                       `yaml:"app_name"`
	Server               ServerConfig                 `yaml:"server"`
	Service              service.MTServiceConfig      `yaml:"service"`
	InMemClientConfig    inmem_client.RPCClientConfig `yaml:"inmem_client"`
	DbConf               db.DataBaseConfig            `yaml:"db"`
	Publisher            amqp.NotifierConfig          `yaml:"publisher"`
	Consumer             amqp.ConsumerConfig          `yaml:"consumer"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/mt_manager.yml", "configuration yml file")
	flag.Parse()
	var appConfig AppConfig

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}
	if appConfig.AppName == "" {
		log.Fatal("app name must be defiled as <host>_<name>")
	}
	if strings.Contains(appConfig.AppName, "-") {
		log.Fatal("app name must be without '-' : it's not a valid metric name")
	}
	if appConfig.MetricInstancePrefix == "" {
		log.Fatal("metric_instance_prefix be defiled as <host>_<name>")
	}
	if strings.Contains(appConfig.MetricInstancePrefix, "-") {
		log.Fatal("metric_instance_prefix be without '-' : it's not a valid metric name")
	}

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Publisher.Conn.Host = envString("RBMQ_HOST", appConfig.Publisher.Conn.Host)
	appConfig.Consumer.Conn.Host = envString("RBMQ_HOST", appConfig.Consumer.Conn.Host)

	log.WithFields(log.Fields{
		"config": fmt.Sprintf("%#v", appConfig),
		"pid":    os.Getpid(),
	}).Info("Config loaded")
	return appConfig
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}

type OperatorQueueConfig struct {
	In  string `yaml:"-"`
	Out string `yaml:"-"`
}
type QueuesConfig struct {
	Pixels   string                         `default:"pixels" yaml:"pixels"`
	Operator map[string]OperatorQueueConfig `yaml:"-"`
}
type OperatorConfig struct {
	Name           string `yaml:"name"`
	RetriesEnabled bool   `yaml:"retries_enabled"`
}
type MTServiceConfig struct {
	RetrySec     int              `default:"600" yaml:"retry_period"`
	RetryCount   int              `default:"600" yaml:"retry_count"`
	ThreadsCount int              `default:"1" yaml:"threads_count"`
	Queues       QueuesConfig     `yaml:"queues"`
	Operators    []OperatorConfig `yaml:"operators"`
}
