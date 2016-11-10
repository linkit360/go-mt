package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/metrics"
)

var (
	SubscriptionsCount prometheus.Gauge
	RetriesCount       prometheus.Gauge
	SinceSuccessPaid   prometheus.Gauge
	Errors             m.Gauge
	PostPaid           m.Gauge
	Rejected           m.Gauge
	BlackListed        m.Gauge
	Pixel              m.Gauge
)

func newGaugeCycle(name, help string) prometheus.Gauge {
	return m.PrometheusGauge("", "cycle", name, "Cycle "+help)
}
func newGaugeNotPaid(name, help string) m.Gauge {
	return m.NewCustomMetric("not_paid_status", name, "not paid status "+help)
}

func Init() {
	// visible gauges
	SubscriptionsCount = newGaugeCycle("subscriptions", "Subscriptions got from database")
	RetriesCount = newGaugeCycle("retries", "Retries got from database")
	SinceSuccessPaid = m.PrometheusGauge(
		"",
		"payment",
		"since_success_paid_seconds",
		"Seconds ago from successful payment from any operator",
	)
	Errors = newGaugeNotPaid("errors", "Errors during processing")
	PostPaid = newGaugeNotPaid("postpaid", "Postpaid count")
	Rejected = newGaugeNotPaid("rejected", "Rejected count")
	BlackListed = newGaugeNotPaid("blacklisted", "Blacklisted count")
	Pixel = newGaugeNotPaid("pixel", "Number of new subscriptions with pixel")

	go func() {
		// metrics in prometheus as for 15s (default)
		// so make for minute interval
		for range time.Tick(time.Minute) {

			Errors.Update()
			PostPaid.Update()
			Rejected.Update()
			BlackListed.Update()
			Pixel.Update()
		}
	}()
}
