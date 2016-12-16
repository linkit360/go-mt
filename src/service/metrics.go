package service

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/utils/metrics"
	rec "github.com/vostrok/utils/rec"
)

var (
	SinceSuccessPaid            prometheus.Gauge
	SetPendingStatusDuration    prometheus.Summary
	SetPendingStatusErrors      m.Gauge
	OperatorNotEnabled          m.Gauge
	OperatorNotApplicable       m.Gauge
	NotifyErrors                m.Gauge
	Errors                      m.Gauge
	PostPaid                    m.Gauge
	Rejected                    m.Gauge
	BlackListed                 m.Gauge
	Paid                        m.Gauge
	Pixel                       m.Gauge
	SubscritpionsDropped        m.Gauge
	SubscritpionsErrors         m.Gauge
	SubscritpionsSent           m.Gauge
	RetriesSent                 m.Gauge
	TarificateFailed            m.Gauge
	TarificateResponsesReceived m.Gauge
	ResponseDropped             m.Gauge
	ResponseErrors              m.Gauge
	ResponseSuccess             m.Gauge
	ResponseSMSErrors           m.Gauge
	ResponseSMSSuccess          m.Gauge
	PendingRetriesCount         prometheus.Gauge
	PendingSubscriptionsCount   prometheus.Gauge
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
	go func() {
		for range time.Tick(time.Second) {
			SinceSuccessPaid.Inc()
		}
	}()
	Errors = newGaugeNotPaid("errors", "Errors during processing")

	TarificateFailed = newGaugeNotPaid("tarificate_falied", "Tariffication attempt errors")
	TarificateResponsesReceived = newGaugeOperaor("tarifficate_request", "tarifficate request")
	OperatorNotEnabled = newGaugeOperaor("not_enabled", "operator is not enabled in config")
	OperatorNotApplicable = newGaugeOperaor("not_applicable", "there is no such operator in database")
	NotifyErrors = m.NewGauge("", "", "notify_errors", "notify errors")

	PostPaid = newGaugeNotPaid("postpaid", "Postpaid count")
	Rejected = newGaugeNotPaid("rejected", "Rejected count")
	BlackListed = newGaugeNotPaid("blacklisted", "Blacklisted count")

	Pixel = newGaugeNotPaid("pixel", "Number of new subscriptions with pixel")
	SetPendingStatusErrors = m.NewGauge("", "", "set_pending_status_errors", "set_pending status")
	SubscritpionsDropped = newGaugeSubscritpions("dropped", "dropped")
	SubscritpionsErrors = newGaugeSubscritpions("errors", "errors")
	SubscritpionsSent = newGaugeSubscritpions("sent", "sent")

	ResponseDropped = newGaugeResponse("dropped", "dropped")
	ResponseErrors = newGaugeResponse("errors", "errors")
	ResponseSuccess = newGaugeResponse("success", "success")
	ResponseSMSErrors = newGaugeResponse("sms_errors", "errors")
	ResponseSMSSuccess = newGaugeResponse("sms_success", "success")

	PendingRetriesCount = m.PrometheusGauge("pending", "retries", "count", "pending retries count")
	PendingSubscriptionsCount = m.PrometheusGauge("pending", "subscriptions", "count", "pending subscriptions count")

	Paid = m.NewGauge("", "", "paid", "paid")
	RetriesSent = m.NewGauge("", "retries", "sent", "paid")
	SetPendingStatusDuration = m.NewSummary("set_pending_status_db_duration_seconds", "set pending status duration seconds")

	go func() {
		for range time.Tick(time.Minute) {
			Errors.Update()

			TarificateFailed.Update()
			TarificateResponsesReceived.Update()
			OperatorNotEnabled.Update()
			OperatorNotApplicable.Update()
			NotifyErrors.Update()

			PostPaid.Update()
			Rejected.Update()
			BlackListed.Update()
			Paid.Update()
			RetriesSent.Update()
			Pixel.Update()
			SetPendingStatusErrors.Update()

			SubscritpionsDropped.Update()
			SubscritpionsErrors.Update()
			SubscritpionsSent.Update()

			ResponseDropped.Update()
			ResponseErrors.Update()
			ResponseSuccess.Update()
			ResponseSMSErrors.Update()
			ResponseSMSSuccess.Update()
		}
	}()

	go func() {
		for range time.Tick(time.Minute) {
			retriesCount, err := rec.GetPendingRetriesCount()
			if err != nil {
				err = fmt.Errorf("rec.GetPendingRetriesCount: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get pending retries")
				PendingRetriesCount.Set(float64(10000000))
			} else {
				PendingRetriesCount.Set(float64(retriesCount))
			}

			moCount, err := rec.GetPendingSubscriptionsCount()
			if err != nil {
				err = fmt.Errorf("rec.GetPendingSubscriptionsCount: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get mo")
				PendingSubscriptionsCount.Set(float64(100000000))
			} else {
				PendingSubscriptionsCount.Set(float64(moCount))
			}
		}
	}()

}
