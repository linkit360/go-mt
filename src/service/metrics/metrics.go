package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/metrics"
)

var (
	SinceSuccessPaid          prometheus.Gauge
	TarificateRequestsOverall m.Gauge
	Errors                    m.Gauge
	DBErrors                  m.Gauge
	TarificateFailed          m.Gauge
	PostPaid                  m.Gauge
	Rejected                  m.Gauge
	BlackListed               m.Gauge
	Pixel                     m.Gauge
	SubscritpionsDropped      m.Gauge
	SubscritpionsErrors       m.Gauge
	SubscritpionsSent         m.Gauge
	ResponseDropped           m.Gauge
	ResponseErrors            m.Gauge
	ResponseSuccess           m.Gauge
)

func newGaugeResponse(name, help string) prometheus.Gauge {
	return m.NewGaugeMetric("response", name, "response "+help)
}
func newGaugeNotPaid(name, help string) m.Gauge {
	return m.NewGaugeMetric("not_paid_status", name, "not paid status "+help)
}
func newGaugeOperaor(name, help string) m.Gauge {
	return m.NewGaugeMetric("operator", name, "operator "+help)
}

func newGaugeSubscritpions(name, help string) m.Gauge {
	return m.NewGaugeMetric("subscritpions", name, "subscritpions "+help)
}

func Init() {

	SinceSuccessPaid = m.PrometheusGauge(
		"",
		"payment",
		"since_success_paid_seconds",
		"Seconds ago from successful payment from any operator",
	)
	Errors = newGaugeNotPaid("errors", "Errors during processing")
	DBErrors = newGaugeNotPaid("db_errors", "DB errors pverall mt_manager")

	TarificateFailed = newGaugeNotPaid("tarificate_falied", "Tariffication attempt errors")

	TarificateRequestsOverall = newGaugeOperaor("tarifficate_request", "tarifficate request")
	PostPaid = newGaugeNotPaid("postpaid", "Postpaid count")
	Rejected = newGaugeNotPaid("rejected", "Rejected count")
	BlackListed = newGaugeNotPaid("blacklisted", "Blacklisted count")
	Pixel = newGaugeNotPaid("pixel", "Number of new subscriptions with pixel")

	SubscritpionsDropped = newGaugeSubscritpions("dropped", "dropped")
	SubscritpionsErrors = newGaugeSubscritpions("errors", "errors")
	SubscritpionsSent = newGaugeSubscritpions("sent", "sent")

	ResponseDropped = newGaugeResponse("dropped", "dropped")
	ResponseErrors = newGaugeResponse("errors", "errors")
	ResponseSuccess = newGaugeResponse("success", "success")

	go func() {
		// metrics in prometheus as for 15s (default)
		// so make for minute interval
		for range time.Tick(time.Minute) {
			TarificateFailed.Update()
			TarificateRequestsOverall.Update()
			Errors.Update()
			DBErrors.Update()
			PostPaid.Update()
			Rejected.Update()
			BlackListed.Update()
			Pixel.Update()
			ResponseDropped.Update()
			ResponseErrors.Update()
			ResponseSuccess.Update()
		}
	}()
}
