package config

import (
	"flag"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/mt/src/service"
)

type ServerConfig struct {
	Port string `default:"50304"`
}
type NewRelicConfig struct {
	AppName string `default:"dev.mt.linkit360.com"`
	License string `default:"4d635427ad90ca786ca2db6aa246ed651730b933"`
}
type AppConfig struct {
	Server   ServerConfig            `yaml:"server"`
	NewRelic NewRelicConfig          `yaml:"newrelic"`
	Service  service.MTServiceConfig `yaml:"service"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/appconfig.yml", "configuration yml file")
	flag.Parse()
	var appConfig AppConfig

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)

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
