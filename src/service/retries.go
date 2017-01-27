package service

import (
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	rec "github.com/vostrok/utils/rec"
)

type EventNotifyTarifficate struct {
	EventName string     `json:"event_name"`
	EventData rec.Record `json:"event_data"`
}

// get records from db
// check them
// and send to telco
func ProcessRetries(operatorCode int64, retryCount int, paidOnceHours int, notifyFnSendChargeRequest func(uint8, rec.Record) error) {
	begin := time.Now()
	retries, err := rec.GetRetryTransactions(operatorCode, retryCount, paidOnceHours)
	if err != nil {
		log.WithFields(log.Fields{
			"code":  operatorCode,
			"count": retryCount,
			"error": err.Error(),
		}).Error("get retries")
		return
	}

	log.WithFields(log.Fields{
		"code":      operatorCode,
		"count":     retryCount,
		"gotFromDB": len(retries),
		"took":      time.Since(begin),
	}).Info("retries")

	if len(retries) == 0 {
		return
	}

	begin = time.Now()

	if svc.retriesWg[operatorCode] == nil {
		svc.retriesWg[operatorCode] = &sync.WaitGroup{}
	}
	for _, r := range retries {
		svc.retriesWg[operatorCode].Add(1)
		go handleRetry(r, notifyFnSendChargeRequest)
	}
	svc.retriesWg[operatorCode].Wait()

	log.WithFields(log.Fields{
		"code":  operatorCode,
		"count": len(retries),
		"took":  time.Since(begin),
	}).Debug("done process retries")
	RetryProcessDuration.Observe(time.Since(begin).Seconds())
}

// for retries: set price
// set paid hours
func handleRetry(record rec.Record, notifyFnSendChargeRequest func(uint8, rec.Record) error) error {

	defer svc.retriesWg[record.OperatorCode].Done()

	logCtx := log.WithFields(log.Fields{
		"tid":            record.Tid,
		"attempts_count": record.AttemptsCount,
	})

	sincePreviousAttempt := time.Now().Sub(record.LastPayAttemptAt).Hours()
	delayHours := (time.Duration(record.DelayHours) * time.Hour).Hours()
	if sincePreviousAttempt < delayHours {
		DelayHoursArentPassed.Inc()

		logCtx.WithFields(log.Fields{
			"prev":   record.LastPayAttemptAt,
			"now":    time.Now().UTC(),
			"passed": sincePreviousAttempt,
			"delay":  delayHours,
		}).Debug("delay hours were not passed")
		return nil
	}

	logCtx.Debug("start processsing")
	if err := checkBlackListedPostpaid(&record); err != nil {
		err = fmt.Errorf("checkBlackListedPostpaid: %s", err.Error())
		return err
	}

	begin := time.Now()
	if err := rec.SetRetryStatus("pending", record.RetryId); err != nil {
		Errors.Inc()
		return err
	}
	SetRetryPendingStatusDuration.Observe(time.Since(begin).Seconds())

	begin = time.Now()
	priority := uint8(0)
	if err := notifyFnSendChargeRequest(priority, record); err != nil {
		Errors.Inc()
		NotifyErrors.Inc()

		err = fmt.Errorf("notifyFnSendChargeRequest: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cannot send to queue")
		return err
	}
	logCtx.WithField("took", time.Since(begin).Seconds()).Debug("notify operator")

	return nil
}
