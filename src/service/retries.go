package service

// does following:
// listen to *_mo_subscription queues
// send them to operator request queue

// check operator requests queue, if free:
// get some retries from database
// send to operator requesus queue
// if everything is ok, then remove item
// if not, "touch" item == renew attempts count and last attempt date

// send sms request to operator queue if necessary

// listen to operator responses queue
// make all needed operations with retries, subscritpions to mark records as processed
import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/utils/config"
	rec "github.com/vostrok/utils/rec"
)

// here are functions to sent tarifficate requests to operator
// retries and subscritpions

type EventNotifyTarifficate struct {
	EventName string     `json:"event_name"`
	EventData rec.Record `json:"event_data"`
}

func processRetries(operatorCode int64, retryCount int) {
	begin := time.Now()
	retries, err := rec.GetRetryTransactions(operatorCode, retryCount)
	if err != nil {
		log.WithFields(log.Fields{
			"code":  operatorCode,
			"count": retryCount,
			"error": err.Error(),
		}).Error("get retries")
		return
	}
	SinceRetryStartProcessed.Set(.0)
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

	svc.retriesWg[operatorCode] = &sync.WaitGroup{}
	for _, r := range retries {
		svc.retriesWg[operatorCode].Add(1)
		go handleRetry(r)
	}
	svc.retriesWg[operatorCode].Wait()
	log.WithFields(log.Fields{
		"code":  operatorCode,
		"count": len(retries),
		"took":  time.Since(begin),
	}).Debug("done process retries")

}

// for retries: set price
// set paid hours
func handleRetry(record rec.Record) error {

	defer svc.retriesWg[record.OperatorCode].Done()

	logCtx := log.WithFields(log.Fields{
		"tid":            record.Tid,
		"attempts_count": record.AttemptsCount,
	})
	logCtx.Debug("start processsing")

	begin := time.Now()
	logCtx.Debug("blacklist checks..")
	blackListed, err := inmem_client.IsBlackListed(record.Msisdn)
	if err != nil {
		Errors.Inc()

		err := fmt.Errorf("inmem_client.IsBlackListed: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cann't get is blacklisted")
		return err
	}
	logCtx.WithField("took", time.Since(begin).Seconds()).Debug("blacklist inmem call")
	if blackListed {
		BlackListed.Inc()

		logCtx.Info("blacklisted")
		record.SubscriptionStatus = "blacklisted"

		if err := writeSubscriptionStatus(record); err != nil {
			Errors.Inc()
			return err
		}

		if err := removeRetry(record); err != nil {
			Errors.Inc()
			return err
		}

		return nil
	} else {
		logCtx.Debug("not blacklisted, start postpaid checks..")
	}

	begin = time.Now()
	postPaid, err := inmem_client.IsPostPaid(record.Msisdn)
	if err != nil {
		Errors.Inc()

		err := fmt.Errorf("inmem_client.IsPostPaid: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cann't get postpaid")
		return err
	}
	logCtx.WithField("took", time.Since(begin).Seconds()).Debug("postpaid inmem call")
	if postPaid {
		PostPaid.Inc()

		logCtx.Info("number is postpaid")
		record.Result = "postpaid"
		record.SubscriptionStatus = "postpaid"

		if err := writeSubscriptionStatus(record); err != nil {
			Errors.Inc()
			return err
		}
		if err := removeRetry(record); err != nil {
			Errors.Inc()
			return err
		}
		return nil
	} else {
		logCtx.Debug("not postpaid, send to operator..")
	}

	begin = time.Now()
	operator, err := inmem_client.GetOperatorByCode(record.OperatorCode)
	if err != nil {
		Errors.Inc()
		OperatorNotApplicable.Inc()

		err := fmt.Errorf("inmem_client.GetOperatorByCode %s: %s", record.OperatorCode, err.Error())
		logCtx.WithField("error", err.Error()).Error("can't process")
		return err
	}
	logCtx.WithField("took", time.Since(begin).Seconds()).Debug("operator by code inmem call")

	operatorName := strings.ToLower(operator.Name)
	queue := config.RequestQueue(operatorName)
	priority := uint8(0)

	begin = time.Now()
	if err := rec.SetRetryStatus("pending", record.RetryId); err != nil {
		Errors.Inc()
		return err
	}
	SetPendingStatusDuration.Observe(time.Since(begin).Seconds())

	begin = time.Now()
	if err := notifyOperatorRequest(queue, priority, "charge", record); err != nil {
		Errors.Inc()
		NotifyErrors.Inc()

		err = fmt.Errorf("notifyOperatorRequest: %s, queue: %s", err.Error(), queue)
		logCtx.WithField("error", err.Error()).Error("Cannot send to operator queue")
		return err
	}
	logCtx.WithField("took", time.Since(begin).Seconds()).Debug("notify operator call")
	RetriesSent.Inc()

	return nil
}
