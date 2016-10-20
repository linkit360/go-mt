// does following:
// get subscription records that are in "" status (not tries to pay) or in "failed" status
// tries to charge via operator
// if not, set up retries

// get all retries
// retry transaction to the operator
// if everything is ok, then remove item
// if not, "touch" item == renew attempts count and last attempt date

package service

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/db"
	rec "github.com/vostrok/mt/src/service/instance"
	"github.com/vostrok/mt/src/service/mobilink"
)

var svc MTService

func Init(sConf MTServiceConfig) {

	svc.sConfig = sConf
	svc.operatorResponses = make(chan rec.Record)
	rec.Init(sConf.DbConf)

	if err := initInMem(sConf.DbConf); err != nil {
		log.WithField("error", err.Error()).Fatal("init in memory tables")
	}
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
	for _, r := range retries {
		now := time.Now()
		makeAttempt := false
		if r.CreatedAt.Sub(now).Hours() > time.Duration(24*r.KeepDays)*time.Hour {
			makeAttempt = true
		}
		if r.LastPayAttemptAt.Sub(now).Hours() > time.Duration(r.DelayHours)*time.Hour {
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
	for _, record := range records {
		if err := handle(record); err != nil {
			continue
		}
	}

	return nil
}

func smsSend(subscription rec.Record, msg string) error {
	switch {
	case mobilink.Belongs(subscription.Msisdn):
		return svc.mobilink.SMS(subscription.Msisdn, msg)
	default:
		log.WithFields(log.Fields{{"subscription": subscription}}).
			Debug("SMS send: not applicable to any operator")
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
	if mService.Price <= 0 {
		logCtx.WithField("price", mService.Price).Error("price is not set")
		return fmt.Errorf("Service price %d is zero or less", mService.Price)
	}
	if mService.PaidHours > 0 {
		// if msisdn already was subscribed on this subscription in paid hours time
		// give them content, and skip tariffication
		if false {
			logCtx.Info("paid hours aren't passed")
			subscription.Status = "rejected"
			subscription.WriteSubscriptionStatus()
			return nil
		}
	}
	if _, ok := memBlackListed.Map[subscription.Msisdn]; ok {
		logCtx.Info("blacklisted")
		subscription.Status = "blacklisted"
		subscription.WriteSubscriptionStatus()
		return nil
	}

	switch {
	case mobilink.Belongs(subscription.Msisdn):
		svc.mobilink.Publish(subscription)
	default:
		log.WithField("subscription", subscription).Error("Not applicable to any operator")
		return false, fmt.Errorf("Msisdn %s is not applicable to any operator", subscription.Msisdn)
	}
	return nil
}

func getResponses() {

	for {
		var subscription rec.Record
		select {
		case subscription <- svc.mobilink.Response:
			svc.operatorResponses <- subscription
		}
	}

	for record := range svc.operatorResponses {
		logCtx := log.WithField("subscription", record)

		if len(record.OperatorToken) > 0 && record.OperatorErr == nil {
			record.Status = "paid"
		} else {
			record.Status = "failed"
		}
		record.WriteSubscriptionStatus()
		record.WriteTransaction()

		// add retries
		if record.AttemptsCount == 0 && record.Status == "failed" {
			mService, ok := memServices.Map[record.ServiceId]
			if !ok {
				logCtx.Error("service not found")
				continue
			}
			record.DelayHours = mService.DelayHours
			record.KeepDays = mService.KeepDays

			if mService.SMSSend == 1 {
				smsSend(record, mService.SMSNotPaidText)
			}
			record.StartRetry()
		}

		if record.AttemptsCount >= 1 {
			record.TouchRetry()
			now := time.Now()

			remove := false
			if record.CreatedAt.Sub(now).Hours() > time.Duration(24*record.KeepDays)*time.Hour {
				remove = true
			}
			if record.Status == "paid" {
				remove = true
			}
			if remove {
				record.RemoveRetry()
			}

		}

	}

}
