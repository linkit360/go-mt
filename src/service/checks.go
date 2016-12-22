package service

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	rec "github.com/vostrok/utils/rec"
)

// chech functions for MO, retries, responses
func checkMO(record *rec.Record, getPreviousSubscriptionFn, setPreviousSubscriptionFn func(r rec.Record)) error {
	logCtx := log.WithFields(log.Fields{
		"tid":            record.Tid,
		"msisdn":         record.Msisdn,
		"attempts_count": record.AttemptsCount,
	})
	logCtx.Debug("start processsing")

	// if msisdn already was subscribed on this subscription in paid hours time
	// give them content, and skip tariffication
	if record.PaidHours > 0 {
		logCtx.WithField("paidHours", record.PaidHours).Debug("paid hours > 0")

		hasPrevious := getPreviousSubscriptionFn(record)
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
	if err := checkBlackListedPostpaid(record); err != nil {
		return err
	}
	if record.PaidHours > 0 {
		setPreviousSubscriptionFn(record)
	}
	return nil
}

func checkBlackListedPostpaid(record *rec.Record) error {
	logCtx := log.WithFields(log.Fields{
		"tid":            record.Tid,
		"msisdn":         record.Msisdn,
		"attempts_count": record.AttemptsCount,
	})

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
		record.Result = "blacklisted"
		record.SubscriptionStatus = "blacklisted"

		if err := writeSubscriptionStatus(record); err != nil {
			Errors.Inc()
			return err
		}
		if record.AttemptsCount >= 1 {
			if err := removeRetry(record); err != nil {
				Errors.Inc()
				return err
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
		logCtx.Debug("number is postpaid")
		PostPaid.Inc()

		record.Result = "postpaid"
		record.SubscriptionStatus = "postpaid"
		if err := writeSubscriptionStatus(record); err != nil {
			Errors.Inc()
			return err
		}
		if record.AttemptsCount >= 1 {
			if err := removeRetry(record); err != nil {
				Errors.Inc()
				return err
			}
		}
		return nil
	} else {
		logCtx.Debug("not postpaid")
	}
	return nil
}

func checkResponse(r *rec.Record) error {
	logCtx := log.WithFields(log.Fields{
		"tid":    r.Tid,
		"msisdn": r.Msisdn,
	})
	logCtx.Info("got response")

	if r.SubscriptionStatus == "postpaid" {
		PostPaid.Inc()
		logCtx.Debug("number is postpaid")

		// order is important
		// if we ad postpaid below in the table (first)
		// and after that record transaction, then
		// we do not notice it in inmem and
		// subscription redirects again to operator request
		r.SubscriptionStatus = "postpaid"
		if err := writeSubscriptionStatus(r); err != nil {
			Errors.Inc()
			return err
		}
		// update postpaid status only if it wasnt already added
		postPaid, _ := inmem_client.IsPostPaid(r.Msisdn)
		if !postPaid {
			PostPaid.Inc()
			if err := inmem_client.PostPaidPush(r.Msisdn); err != nil {
				Errors.Inc()

				err = fmt.Errorf("inmem_client.PostPaidPush: %s", err.Error())
				logCtx.WithFields(log.Fields{
					"msisdn": r.Msisdn,
					"error":  err.Error(),
				}).Error("add inmem postpaid error")
				return err
			}
			if err := addPostPaidNumber(r); err != nil {
				Errors.Inc()

				logCtx.WithFields(log.Fields{
					"msisdn": r.Msisdn,
					"error":  err.Error(),
				}).Error("add postpaid error")

				// sometimes duplicates found despite the check
				return nil
			}
			logCtx.Info("new postpaid number added")
		} else {
			logCtx.Info("already in postpaid inmem")
		}
		if r.AttemptsCount >= 1 {
			if err := removeRetry(r); err != nil {
				Errors.Inc()
				return err
			}
		}
		return nil
	}

	if r.Paid {
		r.SubscriptionStatus = "paid"
		if r.AttemptsCount >= 1 {
			r.Result = "retry_paid"
		} else {
			r.Result = "paid"
		}
	} else {
		r.SubscriptionStatus = "failed"
		if r.AttemptsCount >= 1 {
			r.Result = "retry_failed"
		} else {
			r.Result = "failed"
		}
	}

	logCtx.WithFields(log.Fields{
		"paid":   r.Paid,
		"result": r.Result,
		"status": r.SubscriptionStatus,
	}).Info("statuses")

	if err := writeSubscriptionStatus(r); err != nil {
		Errors.Inc()
		return err
	}
	if err := writeTransaction(r); err != nil {
		Errors.Inc()
		return err
	}

	if r.AttemptsCount == 0 && r.SubscriptionStatus == "failed" {
		logCtx.WithFields(log.Fields{
			"action": "move to retry",
		}).Debug("mo")
		if err := startRetry(r); err != nil {
			Errors.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"retry": fmt.Sprintf("%#v", r),
			}).Error("add to retries failed")
			return err
		}
	}

	if r.AttemptsCount >= 1 {
		now := time.Now()

		logCtx.WithFields(log.Fields{
			"createdAt":      r.CreatedAt,
			"KeepDays":       r.KeepDays,
			"spentInRetries": now.Sub(r.CreatedAt).Hours(),
			"KeepHours":      (time.Duration(24*r.KeepDays) * time.Hour).Hours(),
		}).Debug("process retry")

		remove := false
		if now.Sub(r.CreatedAt).Hours() >
			(time.Duration(24*r.KeepDays) * time.Hour).Hours() {
			logCtx.Info("remove retry: keep days expired")
			remove = true
		}
		if r.Result == "retry_paid" {
			remove = true
		}
		if remove {
			if err := removeRetry(r); err != nil {
				Errors.Inc()
				return err
			}
		} else {
			if err := touchRetry(r); err != nil {
				Errors.Inc()
				return err
			}
		}
	}
	return nil
}
