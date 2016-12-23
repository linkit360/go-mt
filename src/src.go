package src

// server has metrics, config, newrelic app
// and handles rpc method to get content url by campaign hash
// and another method to update cache on demand (CQR)
// anyway, there is a http method to catch metrics
import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/mt_manager/src/config"
	"github.com/vostrok/mt_manager/src/service"
	"github.com/vostrok/utils/metrics"
)

func RunServer() {
	appConfig := config.LoadConfig()
	metrics.Init(appConfig.MetricInstancePrefix)

	service.Init(
		appConfig.AppName,
		appConfig.Service,
		appConfig.InMemClientConfig,
		appConfig.DbConf,
		appConfig.Publisher,
		appConfig.Consumer,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()
	metrics.AddHandler(r)

	r.Run(":" + appConfig.Server.Port)
	log.WithField("port", appConfig.Server.Port).Info("mt init")
}

func OnExit() {
	service.SaveState()
}
