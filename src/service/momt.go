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
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	rec "github.com/vostrok/utils/rec"
)

// here are functions to sent tarifficate requests to operator
// retries and subscritpions

type EventNotifyTarifficate struct {
	EventName string     `json:"event_name"`
	EventData rec.Record `json:"event_data"`
}

func processSubscriptions(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyTarifficate
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			SubscritpionsDropped.Inc()

			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"tarifficate": string(msg.Body),
			}).Error("consume")
			goto ack
		}

		if err := handle(e.EventData); err != nil {
			SubscritpionsErrors.Inc()
		nack:
			if err := msg.Nack(false, true); err != nil {
				log.WithFields(log.Fields{
					"tid":   e.EventData.Tid,
					"error": err.Error(),
				}).Error("cannot nack")
				time.Sleep(time.Second)
				goto nack
			}
			continue
		}
		SubscritpionsSent.Inc()
	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"tid":   e.EventData.Tid,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}

	}
}

func processRetries(operatorCode int64, retryCount int) {
	retries, err := rec.GetRetryTransactions(operatorCode, retryCount)
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
	}).Info("retries")

	if len(retries) == 0 {
		return
	}

	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"code":      operatorCode,
			"gotFromDB": len(retries),
			"took":      time.Since(begin),
		}).Debug("done process retries")
	}()
	for _, r := range retries {
		now := time.Now()
		makeAttempt := false

		// new!
		log.WithFields(log.Fields{
			"tid":      r.Tid,
			"keepDays": r.KeepDays,
		}).Debug("keep days check..")
		if r.CreatedAt.Sub(now).Hours() > (time.Duration(24*r.KeepDays) * time.Hour).Hours() {
			log.WithField("subscription", r).Debug("")
			log.WithFields(log.Fields{
				"subscription": r,
				"CreatedAt":    r.CreatedAt.String(),
				"KeepDays":     r.KeepDays,
			}).Debug("must remove, but first, try tarifficate")
			makeAttempt = true
		}

		hoursSinceLastAttempt := now.Sub(r.LastPayAttemptAt).Hours()
		delay := (time.Duration(r.DelayHours) * time.Hour).Hours()
		log.WithFields(log.Fields{
			"tid":              r.Tid,
			"sinceLastAttempt": hoursSinceLastAttempt,
			"delay":            delay,
		}).Debug("delay hours check..")
		if hoursSinceLastAttempt > delay {
			log.WithFields(log.Fields{
				"subscription":     r,
				"LastPayAttemptAt": r.LastPayAttemptAt.String(),
				"delayHours":       r.DelayHours,
			}).Debug("time to tarifficate")
			makeAttempt = true
		} else {
			log.WithFields(log.Fields{
				"tid": r.Tid,
			}).Debug("dalay check was not passed")
		}

		if makeAttempt {
			handle(r)
		} else {
			log.WithFields(log.Fields{
				"tid": r.Tid,
			}).Debug("won't make attempt")
		}
	}
}

// for retries: set price
// set paid hours
func handle(record rec.Record) error {

	logCtx := log.WithFields(log.Fields{
		"tid":            record.Tid,
		"attempts_count": record.AttemptsCount,
	})
	logCtx.Debug("start processsing")

	mService, err := inmem_client.GetServiceById(record.ServiceId)
	if err != nil {
		Errors.Inc()

		err := fmt.Errorf("inmem_client.GetServiceById %d: %s", record.ServiceId, err.Error())
		logCtx.WithField("error", err.Error()).Error("cann't process")
		return err
	}
	// misconfigured price
	if mService.Price <= 0 {
		Errors.Inc()

		err := fmt.Errorf("mService.Price %d is zero or less", mService.Price)
		logCtx.WithField("price", mService.Price).Error("price is not set")
		return err
	}
	record.Price = 100 * int(mService.Price)

	// if msisdn already was subscribed on this subscription in paid hours time
	// give them content, and skip tariffication
	if record.AttemptsCount == 0 && mService.PaidHours > 0 {
		logCtx.WithField("paidHours", mService.PaidHours).Debug("service paid hours > 0")

		hasPrevious := getPrevSubscriptionCache(
			record.Msisdn, record.ServiceId, record.Tid)
		if hasPrevious {
			Rejected.Inc()

			logCtx.WithFields(log.Fields{}).Info("paid hours aren't passed")
			record.Result = "rejected"
			record.SubscriptionStatus = "rejected"

			if err := writeSubscriptionStatus(record); err != nil {
				Errors.Inc()
				return err
			}
			if err := writeTransaction(record); err != nil {
				Errors.Inc()
				return err
			}
			return nil
		} else {
			logCtx.Debug("no previous subscription found")
		}
	}

	logCtx.Debug("blacklist checks..")
	blackListed, err := inmem_client.IsBlackListed(record.Msisdn)
	if err != nil {
		Errors.Inc()

		err := fmt.Errorf("inmem_client.IsBlackListed: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cann't get is blacklisted")
		return err
	}
	if blackListed {
		BlackListed.Inc()

		logCtx.Info("blacklisted")
		record.SubscriptionStatus = "blacklisted"

		if err := writeSubscriptionStatus(record); err != nil {
			Errors.Inc()
			return err
		}

		if record.AttemptsCount >= 1 {
			if err := removeRetry(record); err != nil {
				Errors.Inc()
				return err
			} else {
			}
		}
		return nil
	} else {
		logCtx.Debug("not blacklisted, start postpaid checks..")
	}

	postPaid, err := inmem_client.IsPostPaid(record.Msisdn)
	if err != nil {
		Errors.Inc()

		err := fmt.Errorf("inmem_client.IsPostPaid: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cann't get postpaid")
		return err
	}
	if postPaid {
		PostPaid.Inc()

		logCtx.Info("postpaid")
		record.SubscriptionStatus = "postpaid"

		if err := writeSubscriptionStatus(record); err != nil {
			Errors.Inc()
			return err
		}
		if record.AttemptsCount >= 1 {
			if err := removeRetry(record); err != nil {
				Errors.Inc()
				return err
			} else {
				logCtx.Info("remove retry: postpaid")
			}
		}
		return nil
	} else {
		logCtx.Debug("not postpaid, send to operator..")
	}

	operator, err := inmem_client.GetOperatorByCode(record.OperatorCode)
	if err != nil {
		OperatorNotApplicable.Inc()

		err := fmt.Errorf("inmem_client.GetOperatorByCode %s: %s", record.OperatorCode, err.Error())
		logCtx.WithField("error", err.Error()).Error("can't process")
		return err
	}
	operatorName := strings.ToLower(operator.Name)
	queue, ok := svc.conf.QueueOperators[operatorName]
	if !ok {
		OperatorNotEnabled.Inc()
		err := fmt.Errorf("Name %s is not enabled", operatorName)
		logCtx.WithField("error", err.Error()).Error("can't process")
		return err
	}

	priority := uint8(0)
	if record.AttemptsCount == 0 {
		setPrevSubscriptionCache(record.Msisdn, record.ServiceId, record.Tid)
		priority = 1
	}

	if record.AttemptsCount > 0 {
		if err := rec.SetRetryStatus("pending", record.RetryId); err != nil {
			Errors.Inc()
			return err
		}
	}

	if err := notifyOperatorRequest(queue.Requests, priority, "charge", record); err != nil {
		Errors.Inc()
		NotifyErrors.Inc()

		err = fmt.Errorf("notifyOperatorRequest: %s, queue: %s", err.Error(), queue.Requests)
		logCtx.WithField("error", err.Error()).Error("Cannot send to operator queue")
		return err
	}

	return nil
}
