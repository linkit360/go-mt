package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/streadway/amqp"

	pixels "github.com/vostrok/pixels/src/notifier"
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
			msg.Ack(false)
			continue
		}

		go func(r rec.Record) {
			if err := handle(r); err != nil {
				SubscritpionsErrors.Inc()
			} else {
				SubscritpionsSent.Inc()
			}
			msg.Ack(false)
		}(e.EventData)
	}
}

func processRetries(operatorCode int64) {
	retries, err := rec.GetRetryTransactions(operatorCode, svc.conf.RetryCount)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("get retries")
		return
	}
	log.WithFields(log.Fields{
		"count": len(retries),
	}).Info("retries")

	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"took": time.Since(begin),
		}).Debug("process retries")
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
			go func(rr rec.Record) {
				handle(rr)
			}(r)
		} else {
			log.WithFields(log.Fields{
				"tid": r.Tid,
			}).Debug("won't make attempt")
		}
	}
}

func handle(subscription rec.Record) error {

	logCtx := log.WithFields(log.Fields{"tid": subscription.Tid})
	logCtx.Debug("start processsing")

	mService, ok := memServices.Map[subscription.ServiceId]
	if !ok {
		logCtx.Error("service not found")
		err := fmt.Errorf("Service id %d not found", subscription.ServiceId)
		Errors.Inc()
		return err
	}
	logCtx.WithField("service_id", mService.Id).Debug("found service")

	// misconfigured price
	if mService.Price <= 0 {
		logCtx.WithField("price", mService.Price).Error("price is not set")
		err := fmt.Errorf("Service price %d is zero or less", mService.Price)
		Errors.Inc()
		return err
	}
	subscription.Price = 100 * int(mService.Price)

	// if msisdn already was subscribed on this subscription in paid hours time
	// give them content, and skip tariffication
	if mService.PaidHours > 0 && subscription.AttemptsCount == 0 {
		logCtx.WithField("paidHours", mService.PaidHours).Debug("service paid hours > 0")
		previous, err := subscription.GetPreviousSubscription(mService.PaidHours)
		if err == sql.ErrNoRows {
			logCtx.Debug("no previous subscription found")
			err = nil
		} else if err != nil {
			logCtx.WithField("error", err.Error()).Error("get previous subscription error")
			err = fmt.Errorf("Get previous subscription: %s", err.Error())
			Errors.Inc()
			return err
		} else {
			sincePrevious := time.Now().Sub(previous.CreatedAt).Hours()
			paidHours := (time.Duration(mService.PaidHours) * time.Hour).Hours()
			if sincePrevious < paidHours {
				logCtx.WithFields(log.Fields{
					"sincePrevious": sincePrevious,
					"paidHours":     paidHours,
				}).Info("paid hours aren't passed")
				Rejected.Inc()
				subscription.Result = "rejected"
				subscription.SubscriptionStatus = "rejected"
				subscription.WriteSubscriptionStatus()
				subscription.WriteTransaction()
				return nil
			} else {
				logCtx.WithFields(log.Fields{
					"sincePrevious": sincePrevious,
					"paidHours":     paidHours,
				}).Debug("previous subscription time elapsed, proceed")
			}
		}
	} else {
		logCtx.Debug("service paid hours == 0")
	}

	logCtx.Debug("blacklist checks..")
	if _, ok := memBlackListed.Map[subscription.Msisdn]; ok {
		BlackListed.Inc()
		logCtx.Info("immemory blacklisted")
		subscription.SubscriptionStatus = "blacklisted"
		subscription.WriteSubscriptionStatus()
		return nil
	}

	logCtx.Debug("postpaid checks..")
	if _, ok := memPostPaid.Map[subscription.Msisdn]; ok {
		logCtx.Info("immemory postpaid")
		PostPaid.Inc()
		subscription.SubscriptionStatus = "postpaid"
		subscription.WriteSubscriptionStatus()
		return nil
	}
	// send everything, pixels module will decide to send pixel, or not to send
	if subscription.Pixel != "" && subscription.AttemptsCount == 0 {
		Pixel.Inc()
		logCtx.WithField("pixel", subscription.Pixel).Debug("enqueue pixel")
		notifyPixel(pixels.Pixel{
			Tid:            subscription.Tid,
			Msisdn:         subscription.Msisdn,
			CampaignId:     subscription.CampaignId,
			SubscriptionId: subscription.SubscriptionId,
			OperatorCode:   subscription.OperatorCode,
			CountryCode:    subscription.CountryCode,
			Pixel:          subscription.Pixel,
			Publisher:      subscription.Publisher,
		})
	} else {
		logCtx.Debug("pixel is empty")
	}

	logCtx.Debug("send to operator")
	operator, ok := memOperators.ByCode[subscription.OperatorCode]
	if !ok {
		err := fmt.Errorf("Code %s is not applicable to any operator", subscription.OperatorCode)
		logCtx.WithField("error", err.Error()).Error("Not applicable to any operator")
		return err
	}
	operatorName := strings.ToLower(operator.Name)
	queue, ok := svc.conf.QueueOperators[operatorName]
	if !ok {
		err := fmt.Errorf("Name %s is not enabled", operatorName)
		logCtx.WithField("error", err.Error()).Error("not enabled in config")
		return err
	}

	if err := notifyOperatorRequest(queue.Requests, "charge", subscription); err != nil {
		err = fmt.Errorf("notifyOperatorRequest: %s, queue: %s", err.Error(), queue.Requests)
		logCtx.WithField("error", err.Error()).Error("Cannot send to operator queue")
		return err
	}
	return nil
}
