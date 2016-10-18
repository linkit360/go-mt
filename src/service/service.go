package service

import (
	"database/sql"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/db"
)

var svc Service

const ACTIVE_STATUS = 1

func InitService(sConf ServiceConfig) {
	svc.db = db.Init(sConf.DbConf)
	svc.sConfig = sConf
	log.Info("mt service init ok")
}

type Service struct {
	db      *sql.DB
	sConfig ServiceConfig
}
type ServiceConfig struct {
	DbConf db.DataBaseConfig `yaml:"db"`
}
