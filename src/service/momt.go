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

		msg.Ack(false)

		var e EventNotifyTarifficate
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			SubscritpionsDropped.Inc()

			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"tarifficate": string(msg.Body),
			}).Error("consume")
			continue
		}

		if err := handle(e.EventData); err != nil {
			SubscritpionsErrors.Inc()
		} else {
			SubscritpionsSent.Inc()
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

		// check it is time!
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

func handle(subscription rec.Record) error {

	logCtx := log.WithFields(log.Fields{
		"tid":            subscription.Tid,
		"attempts_count": subscription.AttemptsCount,
	})
	logCtx.Debug("start processsing")

	mService, err := inmem_client.GetServiceById(subscription.ServiceId)
	if err != nil {
		Errors.Inc()

		err := fmt.Errorf("Service id %d: %s", subscription.ServiceId, err.Error())
		logCtx.WithField("error", err.Error()).Error("cann't process")
		return err
	}
	// misconfigured price
	if mService.Price <= 0 {
		Errors.Inc()

		logCtx.WithField("price", mService.Price).Error("price is not set")
		err := fmt.Errorf("Service price %d is zero or less", mService.Price)
		return err
	}
	subscription.Price = 100 * int(mService.Price)

	// if msisdn already was subscribed on this subscription in paid hours time
	// give them content, and skip tariffication
	if mService.PaidHours > 0 && subscription.AttemptsCount == 0 {
		logCtx.WithField("paidHours", mService.PaidHours).Debug("service paid hours > 0")
		hasPrevious := getPrevSubscriptionCache(
			subscription.Msisdn, subscription.ServiceId, subscription.Tid)
		if hasPrevious {
			Rejected.Inc()
			logCtx.WithFields(log.Fields{}).Info("paid hours aren't passed")
			subscription.Result = "rejected"
			subscription.SubscriptionStatus = "rejected"
			subscription.WriteSubscriptionStatus()
			subscription.WriteTransaction()
			return nil
		} else {
			logCtx.Debug("no previous subscription found")
		}
	}

	logCtx.Debug("blacklist checks..")
	blackListed, err := inmem_client.IsBlackListed(subscription.Msisdn)
	if err != nil {
		Errors.Inc()

		err := fmt.Errorf("inmem_client.IsBlackListed: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cann't get is blacklisted")
		return err
	}
	if blackListed {
		BlackListed.Inc()
		logCtx.Info("blacklisted")
		subscription.SubscriptionStatus = "blacklisted"
		subscription.WriteSubscriptionStatus()

		if subscription.AttemptsCount >= 1 {
			if err := subscription.RemoveRetry(); err != nil {
				Errors.Inc()

				err = fmt.Errorf("subscription.RemoveRetry :%s", err.Error())
				logCtx.WithField("error", err.Error()).Error("remove from retries failed")
			} else {
				logCtx.Info("remove retry: blacklisted")
			}
		}
		return nil
	} else {
		logCtx.Debug("not blacklisted, start postpaid checks..")
	}

	postPaid, err := inmem_client.IsPostPaid(subscription.Msisdn)
	if err != nil {
		Errors.Inc()

		err := fmt.Errorf("inmem_client.IsPostPaid: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cann't get is postpaid")
		return err
	}
	if postPaid {
		PostPaid.Inc()

		logCtx.Info("postpaid")
		subscription.SubscriptionStatus = "postpaid"
		subscription.WriteSubscriptionStatus()
		return nil
	} else {
		logCtx.Debug("not postpaid, send to operator..")
	}

	operator, err := inmem_client.GetOperatorByCode(subscription.OperatorCode)
	if err != nil {
		OperatorNotApplicable.Inc()

		err := fmt.Errorf("inmem_client.GetOperatorByCode %s: %s", subscription.OperatorCode, err.Error())
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
	if subscription.AttemptsCount == 0 {
		priority = 1
	}
	if err := notifyOperatorRequest(queue.Requests, priority, "charge", subscription); err != nil {
		NotifyErrors.Inc()

		err = fmt.Errorf("notifyOperatorRequest: %s, queue: %s", err.Error(), queue.Requests)
		logCtx.WithField("error", err.Error()).Error("Cannot send to operator queue")
		return err
	}
	return nil
}
