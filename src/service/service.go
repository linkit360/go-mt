// does following:
// get subscription records that are in "" status (not tries to pay) or in "failed" status
// tries to charge via operator
// if not, set up retries

// get all retries
// retry transaction to the operator
// if everything is ok, then remove item
// if not, "touch" item == renew attempts count and last attempt date

// todo batches add - retry
// todo: order by created_at - first in first out
// todo: cursor

package service

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"

	"database/sql"
	"github.com/vostrok/db"
	rec "github.com/vostrok/mt/src/service/instance"
	"github.com/vostrok/mt/src/service/mobilink"
)

var svc MTService

func Init(sConf MTServiceConfig) {
	log.SetLevel(log.DebugLevel)

	svc.sConfig = sConf
	svc.operatorResponses = make(chan rec.Record)
	rec.Init(sConf.DbConf)

	if err := initInMem(sConf.DbConf); err != nil {
		log.WithField("error", err.Error()).Fatal("init in memory tables")
	}
	log.Info("inmemory tables init ok")

	mobilinkDb, ok := memOperators.Map["Mobilink"]
	if !ok {
		log.WithField("error", "no db record for mobilink").Fatal("get mobilink from db")
	}
	svc.mobilink = mobilink.Init(mobilinkDb.Rps, sConf.Mobilink)
	log.Info("mt service init ok")

	go func() {
		for range time.Tick(1 * time.Second) {
			processSubscriptions()
		}
	}()

	go func() {
		for range time.Tick(1 * time.Second) {
			processRetries()
		}
	}()

	go func() {
		getResponses()
	}()
}

type MTService struct {
	sConfig           MTServiceConfig
	mobilink          mobilink.Mobilink
	operatorResponses chan rec.Record
}
type MTServiceConfig struct {
	DbConf   db.DataBaseConfig `yaml:"db"`
	Mobilink mobilink.Config   `yaml:"mobilink"`
}

func processRetries() {
	retries, err := rec.GetRetryTransactions()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("get retries")
		return
	}
	log.WithFields(log.Fields{
		"count": len(retries),
	}).Info("retries")

	for _, r := range retries {
		now := time.Now()
		makeAttempt := false

		// new!
		if r.CreatedAt.Sub(now).Hours() > (time.Duration(24*r.KeepDays) * time.Hour).Hours() {
			log.WithField("subscription", r).Debug("")
			log.WithFields(log.Fields{
				"subscription": r,
				"CreatedAt":    r.CreatedAt.String(),
				"KeepDays":     r.KeepDays,
			}).Debug("must remove, but first, try tarifficate")
			makeAttempt = true
		}
		if r.LastPayAttemptAt.Sub(now).Hours() > (time.Duration(r.DelayHours) * time.Hour).Hours() {
			log.WithFields(log.Fields{
				"subscription":     r,
				"LastPayAttemptAt": r.LastPayAttemptAt.String(),
				"delayHours":       r.DelayHours,
			}).Debug("time to tarifficate")
			makeAttempt = true
		}
		if makeAttempt {
			handle(r)
		}
	}
}

func processSubscriptions() {
	records, err := rec.GetNotPaidSubscriptions()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("get subscriptions")
		return
	}
	log.WithFields(log.Fields{
		"count": len(records),
	}).Info("subscriptions")
	for _, record := range records {
		if err := handle(record); err != nil {
			continue
		}
	}
	return
}

func smsSend(subscription rec.Record, msg string) error {
	switch {
	case mobilink.Belongs(subscription.Msisdn):
		return svc.mobilink.SMS(subscription.Msisdn, msg)
	default:
		log.WithFields(log.Fields{
			"subscription": subscription,
		}).Debug("SMS send: not applicable to any operator")
		return fmt.Errorf("Msisdn %s is not applicable to any operator", subscription.Msisdn)
	}
	return nil
}

func handle(subscription rec.Record) error {
	logCtx := log.WithFields(log.Fields{"subscription": subscription})

	mService, ok := memServices.Map[subscription.ServiceId]
	if !ok {
		logCtx.Error("service not found")
		return fmt.Errorf("Service id %d not found", subscription.ServiceId)
	}
	// misconfigured price
	if mService.Price <= 0 {
		logCtx.WithField("price", mService.Price).Error("price is not set")
		return fmt.Errorf("Service price %d is zero or less", mService.Price)
	}
	subscription.Price = 100 * int(mService.Price)

	// if msisdn already was subscribed on this subscription in paid hours time
	// give them content, and skip tariffication
	if mService.PaidHours > 0 {
		previous, err := subscription.GetPreviousSubscription()
		if err == sql.ErrNoRows {
			// ok
		} else if err != nil {
			logCtx.WithField("error", err.Error()).Error("")
			return fmt.Errorf("Get previous subscription: %s", err.Error())
		} else {
			if time.Now().Sub(previous.CreatedAt).Hours() >
				(time.Duration(subscription.DelayHours) * time.Hour).Hours() {
				logCtx.Info("paid hours aren't passed")
				subscription.Result = "rejected"
				subscription.SubscriptionStatus = "rejected"
				subscription.WriteSubscriptionStatus()
				subscription.WriteTransaction()
				return nil
			}
		}
	}

	if _, ok := memBlackListed.Map[subscription.Msisdn]; ok {
		logCtx.Info("blacklisted")
		subscription.SubscriptionStatus = "blacklisted"
		subscription.WriteSubscriptionStatus()
		return nil
	}

	switch {
	case mobilink.Belongs(subscription.Msisdn):
		svc.mobilink.Publish(subscription)
	default:
		log.WithField("subscription", subscription).Error("Not applicable to any operator")
		return fmt.Errorf("Msisdn %s is not applicable to any operator", subscription.Msisdn)
	}
	return nil
}

func getResponses() {

	for {
		select {
		case subscription := <-svc.mobilink.Response:
			svc.operatorResponses <- subscription
		}
	}

	for record := range svc.operatorResponses {
		logCtx := log.WithField("subscription", record)

		if len(record.OperatorToken) > 0 && record.OperatorErr == nil {
			record.SubscriptionStatus = "paid"
			if record.AttemptsCount >= 1 {
				record.Result = "retry_paid"
			} else {
				record.Result = "paid"
			}
		} else {
			record.SubscriptionStatus = "failed"
			if record.AttemptsCount >= 1 {
				record.Result = "retry_failed"
			} else {
				record.Result = "failed"
			}
		}
		logCtx.WithField("status", record.Result).Info("got response")

		if err := record.WriteSubscriptionStatus(); err != nil {
			logCtx.WithField("error", err.Error()).Error("Write Subscription Status failed")
		}
		if err := record.WriteTransaction(); err != nil {
			logCtx.WithField("error", err.Error()).Error("Write Transaction failed")
		}

		// add retries
		if record.AttemptsCount == 0 && record.SubscriptionStatus == "failed" {
			mService, ok := memServices.Map[record.ServiceId]
			if !ok {
				logCtx.WithField("error", "Service not found").Error(record.ServiceId)
				continue
			}
			record.DelayHours = mService.DelayHours
			record.KeepDays = mService.KeepDays

			if mService.SMSSend == 1 {
				logCtx.WithField("sms", "send").Info(mService.SMSNotPaidText)
				smsSend(record, mService.SMSNotPaidText)
			}

			logCtx.Info("start retry..")
			if err := record.StartRetry(); err != nil {
				logCtx.WithField("error", err.Error()).Error("start retry failed")
				continue
			}
		}
		// retry
		if record.AttemptsCount >= 1 {
			now := time.Now()

			remove := false
			if record.CreatedAt.Sub(now).Hours() >
				(time.Duration(24*record.KeepDays) * time.Hour).Hours() {
				remove = true
			}
			if record.Result == "retry_paid" {
				remove = true
			}
			if remove {
				logCtx.Info("remove retry..")
				if err := record.RemoveRetry(); err != nil {
					logCtx.WithField("error", err.Error()).Error("remove retry failed")
					continue
				}
			} else {
				logCtx.Info("touch retry..")
				if err := record.TouchRetry(); err != nil {
					logCtx.WithField("error", err.Error()).Error("touch retry failed")
				}
			}

		}

	}

}
