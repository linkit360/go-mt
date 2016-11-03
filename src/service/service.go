// does following:
// get subscription records that are in "" status (not tries to pay) or in "failed" status
// tries to charge via operator
// if not, set up retries

// get all retries
// retry transaction to the operator
// if everything is ok, then remove item
// if not, "touch" item == renew attempts count and last attempt date

// todo: order by created_at - first in first out

package service

import (
	"database/sql"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"

	"github.com/vostrok/db"
	rec "github.com/vostrok/mt_manager/src/service/instance"
	"github.com/vostrok/mt_manager/src/service/mobilink"
	"github.com/vostrok/mt_manager/src/service/notifier"
	//github.com/vostrok/pixels/src/service
)

var svc MTService

func Init(
	sConf MTServiceConfig,
	dbConf db.DataBaseConfig,
	notifConf notifier.NotifierConfig,
	mobConf mobilink.Config,
) {
	log.SetLevel(log.DebugLevel)

	svc.sConfig = sConf
	svc.dbConf = dbConf
	svc.m = initMetrics()
	rec.Init(dbConf)

	svc.n = notifier.NewNotifierService(notifConf)

	if err := initInMem(dbConf); err != nil {
		log.WithField("error", err.Error()).Fatal("init in memory tables")
	}
	log.Info("inmemory tables init ok")

	mobilinkDb, ok := memOperators.Map["Mobilink"]
	if !ok {
		log.WithField("error", "no db record for mobilink").Fatal("get mobilink from db")
	}

	svc.mobilink = mobilink.Init(mobilinkDb.Rps, mobConf)
	log.Info("mt service init ok")

	go func() {
		for range time.Tick(time.Second) {
			svc.m.SinceSuccessPaid.Set(svc.m.SinceSuccessPaid.Get() + 1.0)
			svc.mobilink.M.ResponseLen.Set(float64(len(svc.mobilink.Response)))
			svc.mobilink.M.PendingRequests.Set(float64(svc.mobilink.GetMTChanGap()))
		}
	}()

	go func() {
		for range time.Tick(time.Duration(sConf.SubscriptionsSec) * time.Second) {
			processSubscriptions()
		}
	}()

	go func() {
		for range time.Tick(time.Duration(sConf.RetrySec) * time.Second) {
			processRetries()
		}
	}()

	go func() {
		for {
			begin := time.Now()
			log.Debug("process all responses")
			for record := range svc.mobilink.Response {
				go func(r rec.Record) {
					handleResponse(record)
				}(record)
			}
			log.WithFields(log.Fields{
				"took": time.Since(begin),
			}).Debug("process all responses")
		}
	}()
}

type MTService struct {
	sConfig  MTServiceConfig
	dbConf   db.DataBaseConfig
	mobilink *mobilink.Mobilink
	m        Metrics
	n        notifier.Notifier
}
type MTServiceConfig struct {
	SubscriptionsSec   int `default:"600" yaml:"subscriptions_period"`
	SubscriptionsCount int `default:"600" yaml:"subscriptions_count"`
	RetrySec           int `default:"600" yaml:"retry_period"`
	RetryCount         int `default:"600" yaml:"retry_count"`
}

type Metrics struct {
	SubscriptionsCount metrics.Gauge
	RetryCount         metrics.Gauge
	SinceSuccessPaid   metrics.Gauge
}

func initMetrics() Metrics {
	return Metrics{
		SubscriptionsCount: expvar.NewGauge("subscriptions_count"),
		RetryCount:         expvar.NewGauge("retry_count"),
		SinceSuccessPaid:   expvar.NewGauge("since_success_paid_sec"),
	}
}
func processRetries() {
	if buzyCheck() {
		return
	}
	retries, err := rec.GetRetryTransactions(svc.sConfig.RetryCount)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("get retries")
		return
	}
	log.WithFields(log.Fields{
		"count": len(retries),
	}).Info("retries")
	svc.m.RetryCount.Set(float64(len(retries)))

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
func buzyCheck() bool {
	log.Info("buzy check")
	if len(svc.mobilink.Response) > 0 {
		log.WithFields(log.Fields{
			"responses": "mobilink",
		}).Debug("have to process all responses")
	}
	if svc.mobilink.GetMTChanGap() > 0 {
		log.WithFields(log.Fields{
			"tarifficate": "mobilink",
		}).Debug("tariffications requests are still not empty")
		return true
	}
	return false
}
func processSubscriptions() {
	if buzyCheck() {
		return
	}

	records, err := rec.GetNotPaidSubscriptions(svc.sConfig.SubscriptionsCount)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("get subscriptions")
		return
	}
	log.WithFields(log.Fields{
		"count": len(records),
	}).Info("subscriptions")

	svc.m.SubscriptionsCount.Set(float64(len(records)))
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"took": time.Since(begin),
		}).Debug("process subscriptions")
	}()
	for _, record := range records {
		go func(r rec.Record) {
			handle(r)
		}(record)
	}
	return
}
func smsSend(subscription rec.Record, msg string) error {
	switch {
	case mobilink.Belongs(subscription.Msisdn):
		return svc.mobilink.SMS(subscription.Tid, subscription.Msisdn, msg)
	default:
		log.WithFields(log.Fields{
			"subscription": subscription,
		}).Debug("SMS send: not applicable to any operator")
		return fmt.Errorf("Msisdn %s is not applicable to any operator", subscription.Msisdn)
	}
	return nil
}
func handle(subscription rec.Record) error {
	//logCtx := log.WithFields(log.Fields{"subscription": subscription})
	logCtx := log.WithFields(log.Fields{"tid": subscription.Tid})
	logCtx.Debug("start processsing")

	mService, ok := memServices.Map[subscription.ServiceId]
	if !ok {
		logCtx.Error("service not found")
		return fmt.Errorf("Service id %d not found", subscription.ServiceId)
	}
	logCtx.WithField("service", mService).Debug("found service")

	// misconfigured price
	if mService.Price <= 0 {
		logCtx.WithField("price", mService.Price).Error("price is not set")
		return fmt.Errorf("Service price %d is zero or less", mService.Price)
	}
	subscription.Price = 100 * int(mService.Price)

	// if msisdn already was subscribed on this subscription in paid hours time
	// give them content, and skip tariffication
	if mService.PaidHours > 0 && subscription.AttemptsCount == 0 {
		logCtx.WithField("paidHours", mService.PaidHours).Debug("service paid hours > 0")
		previous, err := subscription.GetPreviousSubscription()
		if err == sql.ErrNoRows {
			logCtx.Debug("no previous subscription found")
			err = nil
		} else if err != nil {
			logCtx.WithField("error", err.Error()).Error("get previous subscription error")
			return fmt.Errorf("Get previous subscription: %s", err.Error())
		} else {
			sincePrevious := time.Now().Sub(previous.CreatedAt).Hours()
			paidHours := (time.Duration(mService.PaidHours) * time.Hour).Hours()
			if sincePrevious < paidHours {
				log.WithFields(log.Fields{
					"sincePrevious": sincePrevious,
					"paidHours":     paidHours,
				}).Info("paid hours aren't passed")
				subscription.Result = "rejected"
				subscription.SubscriptionStatus = "rejected"
				subscription.WriteSubscriptionStatus()
				subscription.WriteTransaction()
				return nil
			} else {
				log.WithFields(log.Fields{
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
		logCtx.Info("immemory blacklisted")
		subscription.SubscriptionStatus = "blacklisted"
		subscription.WriteSubscriptionStatus()
		return nil
	}

	if mobilink.Belongs(subscription.Msisdn) {
		begin := time.Now()
		postPaid, err := svc.mobilink.BalanceCheck(subscription.Tid, subscription.Msisdn)
		if err != nil {
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"took":  time.Since(begin),
			}).Debug("check blacklisted error")
			return err
		}

		if postPaid {
			logCtx.Debug("number is blacklisted by operator")
			if err = subscription.AddBlacklistedNumber(); err != nil {
				logCtx.WithField("error", err.Error()).Debug("add blacklist error")
				return err
			}
			logCtx.Info("new blacklist number added")
			memBlackListed.Reload()
			subscription.SubscriptionStatus = "blacklisted"
			subscription.WriteSubscriptionStatus()
			return nil
		}
	}

	logCtx.Debug("send to operator")
	switch {
	case mobilink.Belongs(subscription.Msisdn):
		logCtx.Debug("sent to mobilink channel")
		svc.mobilink.Publish(subscription)
	default:
		log.WithField("subscription", subscription).Error("Not applicable to any operator")
		return fmt.Errorf("Msisdn %s is not applicable to any operator", subscription.Msisdn)
	}
	return nil
}
func handleResponse(record rec.Record) {
	//logCtx := log.WithField("subscription", record)
	logCtx := log.WithFields(log.Fields{})
	logCtx.Info("start processing response")

	if len(record.OperatorToken) > 0 && record.OperatorErr == "" {
		svc.m.SinceSuccessPaid.Set(.0)
		record.SubscriptionStatus = "paid"
		if record.AttemptsCount >= 1 {
			record.Result = "retry_paid"
		} else {
			record.Result = "paid"
		}
		// TODO: pixels
		//svc.n.PaidNotify(service.Pixel{
		//	Tid:            record.Tid,
		//	Pixel:          record.Pixel,
		//	Publisher:      record.Publisher,
		//	SubscriptionId: record.SubscriptionId,
		//	Msisdn:         record.Msisdn,
		//})
	} else {
		record.SubscriptionStatus = "failed"
		if record.AttemptsCount >= 1 {
			record.Result = "retry_failed"
		} else {
			record.Result = "failed"
		}
	}

	logCtx.WithFields(log.Fields{
		"result": record.Result,
		"status": record.SubscriptionStatus,
	}).Info("got response")

	if err := record.WriteSubscriptionStatus(); err != nil {
		// already logged inside, wuth query
	}
	if err := record.WriteTransaction(); err != nil {
		// already logged inside, wuth query
	}

	// add retries
	if record.AttemptsCount == 0 && record.SubscriptionStatus == "failed" {
		logCtx.WithFields(log.Fields{
			"action": "move to retry",
		}).Debug("subscription")

		mService, ok := memServices.Map[record.ServiceId]
		if !ok {
			logCtx.WithFields(log.Fields{
				"error": "Service not found",
			}).Error("cannot process subscription")
			return
		}
		record.DelayHours = mService.DelayHours
		record.KeepDays = mService.KeepDays

		if mService.SMSSend == 1 {
			logCtx.WithField("sms", "send").Info(mService.SMSNotPaidText)

			smsTranasction := record
			smsTranasction.Result = "sms"
			err := smsSend(record, mService.SMSNotPaidText)
			if err != nil {
				smsTranasction.OperatorErr = fmt.Errorf("SMS Send: %s", err.Error()).Error()
				logCtx.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("sms send")
			}
			if err := smsTranasction.WriteTransaction(); err != nil {
				logCtx.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("failed to write sms transaction")
			}

		}

		logCtx.Info("add to retries")
		if err := record.StartRetry(); err != nil {
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"retry": fmt.Sprintf("%#v", record),
			}).Error("add to retries failed")
		}
	}
	// retry
	if record.AttemptsCount >= 1 {
		logCtx.WithFields(log.Fields{
			"attemptsCount": record.AttemptsCount,
		}).Debug("process retry")

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
			logCtx.Info("remove from retries")
			if err := record.RemoveRetry(); err != nil {
				logCtx.WithField("error", err.Error()).Error("remove from retries failed")
				return
			}
		} else {
			logCtx.Info("touch retry..")
			if err := record.TouchRetry(); err != nil {
				logCtx.WithField("error", err.Error()).Error("touch retry failed")
			}
		}

	}

}
