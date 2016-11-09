package src

// server has metrics, config, newrelic app
// and handles rpc method to get content url by campaign hash
// and another method to update cache on demand (CQR)
// anyway, there is a http method to catch metrics
import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/metrics"
	"github.com/vostrok/mt_manager/src/config"
	"github.com/vostrok/mt_manager/src/service"
	"github.com/vostrok/mt_manager/src/service/mobilink"
)

func RunServer() {
	appConfig := config.LoadConfig()
	metrics.Init(appConfig.Name)

	service.Init(
		appConfig.Service,
		appConfig.DbConf,
		appConfig.Notifier,
		appConfig.Mobilink,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()

	service.AddCQRHandlers(r)
	metrics.AddHandler(r)

	rgMobilink := r.Group("/mobilink_handler")
	rgMobilink.POST("", mobilink.MobilinkHandler)

	r.Run(":" + appConfig.Server.Port)
	log.WithField("port", appConfig.Server.Port).Info("mt init")
}
