package service

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/utils/metrics"
)

var (
	RetryProcessDuration             prometheus.Summary
	SetRetryPendingStatusDuration    prometheus.Summary
	SetPeriodicPendingStatusDuration prometheus.Summary
	SetPendingStatusErrors           m.Gauge

	DelayHoursArentPassed m.Gauge
	NotifyErrors          m.Gauge
	Errors                m.Gauge
	PostPaid              m.Gauge
	Rejected              m.Gauge
	BlackListed           m.Gauge
	ContentdRPCDialError  m.Gauge
)

func newGaugeNotPaid(name, help string) m.Gauge {
	return m.NewGauge(appName, "not_paid_status", name, "not paid status "+help)
}

func initMetrics(appName string) {

	RetryProcessDuration = m.NewSummary(appName+"_retry_process_duration_seconds", "retries after fetch processing duration in seconds")
	SetRetryPendingStatusDuration = m.NewSummary(appName+"_set_retries_pending_status_db_duration_seconds", "set retries pending status duration seconds")
	SetPeriodicPendingStatusDuration = m.NewSummary(appName+"_set_periodic_pending_status_db_duration_seconds", "set periodic pending status duration seconds")

	NotifyErrors = m.NewGauge("", "", "notify_errors", "notify errors")
	Errors = newGaugeNotPaid("errors", "Errors during processing")
	PostPaid = newGaugeNotPaid(appName+"_postpaid", "Postpaid count")
	Rejected = newGaugeNotPaid(appName+"_rejected", "Rejected count")
	BlackListed = newGaugeNotPaid(appName+"_blacklisted", "Blacklisted count")
	SetPendingStatusErrors = m.NewGauge(appName, "", "set_pending_status_errors", "set_pending status")
	DelayHoursArentPassed = m.NewGauge(appName, "", "delay_hours_arent_passed", "delay_hours_arent_passed")
	ContentdRPCDialError = m.NewGauge(appName, "contentd", "errors", "contentd errors")

	go func() {
		for range time.Tick(time.Minute) {
			Errors.Update()
			NotifyErrors.Update()
			PostPaid.Update()
			Rejected.Update()
			BlackListed.Update()
			SetPendingStatusErrors.Update()
			DelayHoursArentPassed.Update()
			ContentdRPCDialError.Update()
		}
	}()
}
