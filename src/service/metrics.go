package service

import (
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/utils/metrics"
)

var (
	SinceSuccessPaid          prometheus.Gauge
	TarificateRequestsOverall m.Gauge
	OperatorNotEnabled        m.Gauge
	OperatorNotApplicable     m.Gauge
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

func newGaugeResponse(name, help string) m.Gauge {
	return m.NewGauge("", "response", name, "response "+help)
}
func newGaugeNotPaid(name, help string) m.Gauge {
	return m.NewGauge("", "not_paid_status", name, "not paid status "+help)
}
func newGaugeOperaor(name, help string) m.Gauge {
	return m.NewGauge("", "operator", name, "operator "+help)
}
func newGaugeSubscritpions(name, help string) m.Gauge {
	return m.NewGauge("", "subscritpions", name, "subscritpions "+help)
}

func initMetrics() {

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
	OperatorNotEnabled = newGaugeOperaor("not_enabled", "operator is not enabled in config")
	OperatorNotApplicable = newGaugeOperaor("not_applicable", "there is no such operator in database")

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

}
