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
	RetryProcessDuration             prometheus.Summary
	SetRetryPendingStatusDuration    prometheus.Summary
	SetPeriodicPendingStatusDuration prometheus.Summary
	PendingSubscriptionsCount        prometheus.Gauge
	PendingRetriesCount              prometheus.Gauge
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
	PendingSubscriptionsCount = m.PrometheusGauge(appName+"_pending", "subscriptions", "count", "pending subscriptions count")
	PendingRetriesCount = m.PrometheusGauge(appName+"_pending", "retries", "count", "pending retries count")

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

	go func() {
		for range time.Tick(time.Minute) {
			retriesCount, err := rec.GetSuspendedRetriesCount()
			if err != nil {
				err = fmt.Errorf("rec.GetSuspendedRetriesCount: %s", err.Error())
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("get suspended retries")
				PendingRetriesCount.Set(float64(10000000))
			} else {
				PendingRetriesCount.Set(float64(retriesCount))
			}

			moCount, err := rec.GetSuspendedSubscriptionsCount()
			if err != nil {
				err = fmt.Errorf("rec.GetSuspendedSubscriptionsCount: %s", err.Error())
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
