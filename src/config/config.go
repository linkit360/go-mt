package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jinzhu/configor"
	log "github.com/sirupsen/logrus"

	content_client "github.com/linkit360/go-contentd/rpcclient"
	mid_client "github.com/linkit360/go-mid/rpcclient"
	"github.com/linkit360/go-mt/src/service"
	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/db"
)

type ServerConfig struct {
	Host string `default:"127.0.0.1" yaml:"host"`
	Port string `default:"50304" yaml:"port"`
}

type AppConfig struct {
	AppName              string                      `yaml:"app_name"`
	Server               ServerConfig                `yaml:"server"`
	MidConfig            mid_client.ClientConfig     `yaml:"mid_client"`
	ContentdClientConfig content_client.ClientConfig `yaml:"contentd_client"`
	Service              service.MTServiceConfig     `yaml:"service"`
	DbConf               db.DataBaseConfig           `yaml:"db"`
	Publisher            amqp.NotifierConfig         `yaml:"publisher"`
	Consumer             amqp.ConsumerConfig         `yaml:"consumer"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/mt.yml", "configuration yml file")
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
