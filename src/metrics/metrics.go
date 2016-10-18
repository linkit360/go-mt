package metrics

import (
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
)

var M AppMetrics

type AppMetrics struct {
	RetryCount metrics.Counter
}

func Init() AppMetrics {
	M = AppMetrics{
		RetryCount: expvar.NewCounter("retry_count"),
	}
	return M
}
