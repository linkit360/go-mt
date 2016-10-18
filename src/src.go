package src

// server has metrics, config, newrelic app
// and handles rpc method to get content url by campaign hash
// and another method to update cache on demand (CQR)
// anyway, there is a http method to catch metrics
import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/contrib/expvar"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/mt/src/config"
	"github.com/vostrok/mt/src/metrics"
	"github.com/vostrok/mt/src/newrelic"
	"github.com/vostrok/mt/src/service"
)

func RunServer() {
	appConfig := config.LoadConfig()
	metrics.Init()
	newrelic.Init(appConfig.NewRelic)
	service.InitService(appConfig.Service)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()
	rg := r.Group("/debug")
	rg.GET("/vars", expvar.Handler())
	r.Run(":" + appConfig.Server.Port)
	log.WithField("port", appConfig.Server.Port).Info("mt init")
}
