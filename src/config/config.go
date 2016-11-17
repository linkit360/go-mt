package config

import (
	"flag"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/db"
	"github.com/vostrok/mt_manager/src/service"
	"github.com/vostrok/rabbit"
)

type ServerConfig struct {
	Port string `default:"50304"`
}
type AppConfig struct {
	Name      string                  `yaml:"name"`
	Server    ServerConfig            `yaml:"server"`
	Service   service.MTServiceConfig `yaml:"service"`
	DbConf    db.DataBaseConfig       `yaml:"db"`
	Publisher rabbit.NotifierConfig   `yaml:"publisher"`
	Consumer  rabbit.ConsumerConfig   `yaml:"consumer"`
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
	if appConfig.Name == "" {
		log.Fatal("app name must be defiled as <host>_<name>")
	}
	if strings.Contains(appConfig.Name, "-") {
		log.Fatal("app name must be without '-' : it's not a valid metric name")
	}

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Publisher.Conn.Host = envString("RBMQ_HOST", appConfig.Publisher.Conn.Host)
	appConfig.Consumer.Conn.Host = envString("RBMQ_HOST", appConfig.Consumer.Conn.Host)

	appConfig.Service.Queues.Operator = make(map[string]service.OperatorQueueConfig, len(appConfig.Service.Operators))
	for _, operator := range appConfig.Service.Operators {
		name := strings.ToLower(operator.Name)
		appConfig.Service.Queues.Operator[name] = service.OperatorQueueConfig{
			NewSubscription: name + "_new_subscritpions",
			Requests:        name + "_requests",
			Responses:       name + "_responses",
			SMS:             name + "_sms",
		}
	}

	log.WithField("config", appConfig).Info("Config loaded")
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
