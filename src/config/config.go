package config

import (
	"flag"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/db"
	"github.com/vostrok/mt_manager/src/service"
	"github.com/vostrok/mt_manager/src/service/mobilink"
	"github.com/vostrok/pixels/src/notifier"
)

type ServerConfig struct {
	Port string `default:"50304"`
}
type AppConfig struct {
	Server   ServerConfig            `yaml:"server"`
	Service  service.MTServiceConfig `yaml:"service"`
	DbConf   db.DataBaseConfig       `yaml:"db"`
	Notifier notifier.NotifierConfig `yaml:"notifier"`
	Mobilink mobilink.Config         `yaml:"mobilink"`
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

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Notifier.Rbmq.Conn.Host = envString("RBMQ_HOST", appConfig.Notifier.Rbmq.Conn.Host)

	appConfig.Mobilink.TransactionLog.ResponseLogPath =
		envString("MOBILINK_RESPONSE_LOG", appConfig.Mobilink.TransactionLog.ResponseLogPath)
	appConfig.Mobilink.TransactionLog.RequestLogPath =
		envString("MOBILINK_REQUEST_LOG", appConfig.Mobilink.TransactionLog.RequestLogPath)

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
