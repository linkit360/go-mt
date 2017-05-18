package src

// server has metrics, config, newrelic app
// and handles rpc method to get content url by campaign hash
// and another method to update cache on demand (CQR)
// anyway, there is a http method to catch metrics
import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/linkit360/go-mt/src/config"
	"github.com/linkit360/go-mt/src/service"
	"github.com/linkit360/go-utils/metrics"
)

func RunServer() {
	appConfig := config.LoadConfig()

	service.Init(
		appConfig.AppName,
		appConfig.Service,
		appConfig.InMemClientConfig,
		appConfig.ReporterConfig,
		appConfig.DbConf,
		appConfig.Publisher,
		appConfig.Consumer,
		appConfig.ContentdClientConfig,
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
