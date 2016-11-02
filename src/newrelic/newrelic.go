package newrelic

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	newrelic "github.com/newrelic/go-agent"

	"github.com/vostrok/mt_manager/src/config"
)

var (
	app newrelic.Application
)

func Init(cnf config.NewRelicConfig) {
	log.WithField("NRName", cnf.AppName)

	cfg := newrelic.NewConfig(cnf.AppName, cnf.License)

	var err error
	app, err = newrelic.NewApplication(cfg)
	if err != nil {
		log.WithField("NR", err.Error()).Fatal("Init NewRerlic")
	}
}

func NewRelicHandler(c *gin.Context) {
	txn := app.StartTransaction("mt", c.Writer, c.Request)
	defer txn.End()
	c.Next()
}

func RecordInitApp() {
	app.RecordCustomEvent("Init", map[string]interface{}{"time": time.Now().String()})
}
