package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/metrics"
)

var (
	SinceSuccessPaid            prometheus.Gauge
	MobilinkRequestQueue        prometheus.Gauge
	MobilinkResponsesQueue      prometheus.Gauge
	BalanceCheckRequestsOverall m.Gauge
	TarificateRequestsOverall   m.Gauge
	Errors                      m.Gauge
	BalanceCheckFailed          m.Gauge
	TarificateFailed            m.Gauge
	PostPaid                    m.Gauge
	Rejected                    m.Gauge
	BlackListed                 m.Gauge
	Pixel                       m.Gauge
)

func newGaugeMobilinkQueues(name, help string) prometheus.Gauge {
	return m.PrometheusGauge("mobilink", "queue", name, "mobilink queue "+help)
}
func newGaugeCycle(name, help string) prometheus.Gauge {
	return m.PrometheusGauge("", "cycle", name, "Cycle "+help)
}
func newGaugeNotPaid(name, help string) m.Gauge {
	return m.NewGaugeMetric("not_paid_status", name, "not paid status "+help)
}
func newGaugeOperaor(name, help string) m.Gauge {
	return m.NewGaugeMetric("operator", name, "operator "+help)
}
func Init() {

	// visible gauges
	MobilinkRequestQueue = newGaugeMobilinkQueues("requests", "request")
	MobilinkResponsesQueue = newGaugeMobilinkQueues("responses", "response")

	SubscriptionsCount = newGaugeCycle("subscriptions", "Subscriptions got from database")
	RetriesCount = newGaugeCycle("retries", "Retries got from database")
	SinceSuccessPaid = m.PrometheusGauge(
		"",
		"payment",
		"since_success_paid_seconds",
		"Seconds ago from successful payment from any operator",
	)
	Errors = newGaugeNotPaid("errors", "Errors during processing")
	BalanceCheckFailed = newGaugeNotPaid("balance_check_falied", "Balance check errors")
	TarificateFailed = newGaugeNotPaid("tarificate_falied", "Tariffication attempt errors")
	BalanceCheckRequestsOverall = newGaugeOperaor("balance_check_request", "balance check")
	TarificateRequestsOverall = newGaugeOperaor("tarifficate_request", "tarifficate request")
	PostPaid = newGaugeNotPaid("postpaid", "Postpaid count")
	Rejected = newGaugeNotPaid("rejected", "Rejected count")
	BlackListed = newGaugeNotPaid("blacklisted", "Blacklisted count")
	Pixel = newGaugeNotPaid("pixel", "Number of new subscriptions with pixel")

	go func() {
		// metrics in prometheus as for 15s (default)
		// so make for minute interval
		for range time.Tick(time.Minute) {
			TarificateFailed.Update()
			BalanceCheckFailed.Update()
			TarificateRequestsOverall.Update()
			BalanceCheckRequestsOverall.Update()
			Errors.Update()
			PostPaid.Update()
			Rejected.Update()
			BlackListed.Update()
			Pixel.Update()
		}
	}()
}
