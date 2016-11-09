package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/metrics"
)

var (
	metrics            metric
	SubscriptionsCount prometheus.Gauge
	RetriesCount       prometheus.Gauge
	SinceSuccessPaid   prometheus.Gauge
)

type metric struct {
	Errors      prometheus.Gauge
	PostPaid    prometheus.Gauge
	Rejected    prometheus.Gauge
	BlackListed prometheus.Gauge
	Pixel       prometheus.Gauge
}

var (
	Errors      int64
	PostPaid    int64
	Rejected    int64
	BlackListed int64
	Pixel       int64
)

func Init() {
	// visible gauges
	SubscriptionsCount = m.NewGauge(
		"",
		"",
		"subscriptions_count",
		"Subscriptions got from database",
	)
	RetriesCount = m.NewGauge(
		"",
		"",
		"retries_count",
		"Retries got from database",
	)
	SinceSuccessPaid = m.NewGauge(
		"",
		"",
		"since_success_paid_seconds",
		"Seconds ago from successful payment from any operator",
	)

	metrics = metric{

		Errors: m.NewGauge(
			"",
			"",
			"service_errors",
			"Errors during count processing",
		),
		PostPaid: m.NewGauge(
			"",
			"",
			"postpaid",
			"Postpaid count",
		),
		Rejected: m.NewGauge(
			"",
			"",
			"rejected",
			"Rejected count",
		),
		BlackListed: m.NewGauge(
			"",
			"",
			"blacklisted",
			"Blacklisted count",
		),
		Pixel: m.NewGauge(
			"",
			"",
			"pixel",
			"Number of new subscriptions with pixel",
		),
	}
	go func() {
		// metrics in prometheus as for 15s (default)
		// so make for minute interval
		for range time.Tick(time.Minute) {

			metrics.Errors.Set(float64(Errors))
			metrics.PostPaid.Set(float64(PostPaid))
			metrics.Rejected.Set(float64(Rejected))
			metrics.BlackListed.Set(float64(BlackListed))
			metrics.Pixel.Set(float64(Pixel))

			Errors = 0
			PostPaid = 0
			Rejected = 0
			BlackListed = 0
			Pixel = 0
		}
	}()
}
