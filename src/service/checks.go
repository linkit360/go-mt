package service

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	mid_client "github.com/linkit360/go-mid/rpcclient"
	rec "github.com/linkit360/go-utils/rec"
)

// chech functions for MO, retries, responses
func checkMO(record *rec.Record, isRejectedFn func(r rec.Record) bool, setActiveSubscriptionFn func(r rec.Record)) error {
	logCtx := log.WithFields(log.Fields{
		"tid": record.Tid,
	})
	logCtx.Debug("mo")

	// if msisdn already was subscribed on this subscription in paid hours time
	// give them content, and skip tariffication
	if record.PaidHours > 0 {
		hasPrevious := isRejectedFn(*record)
		if hasPrevious {
			Rejected.Inc()

			logCtx.WithFields(log.Fields{}).Info("paid hours aren't passed")
			record.Result = "rejected"
			record.SubscriptionStatus = "rejected"

			if err := writeSubscriptionStatus(*record); err != nil {
				return err
			}
			if err := writeTransaction(*record); err != nil {
				return err
			}
			return nil
		} else {
			logCtx.Debug("no active subscription found")
		}
	}
	if err := checkBlackListedPostpaid(record); err != nil {
		return err
	}
	if record.PaidHours > 0 {
		setActiveSubscriptionFn(*record)
	}
	return nil
}

func checkBlackListedPostpaid(record *rec.Record) error {
	logCtx := log.WithFields(log.Fields{
		"tid":            record.Tid,
		"attempts_count": record.AttemptsCount,
	})

	logCtx.Debug("blacklist checks..")
	blackListed, err := mid_client.IsBlackListed(record.Msisdn)
	if err != nil {
		Errors.Inc()

		err := fmt.Errorf("mid_client.IsBlackListed: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cann't get is blacklisted")
		return err
	}
	if blackListed {
		BlackListed.Inc()

		logCtx.Info("blacklisted")
		record.Result = "blacklisted"
		record.SubscriptionStatus = "blacklisted"

		if err := writeSubscriptionStatus(*record); err != nil {
			Errors.Inc()
			return err
		}
		if record.AttemptsCount >= 1 {
			if err := removeRetry(*record); err != nil {
				Errors.Inc()
				return err
			}
		}
		return nil
	} else {
		logCtx.Debug("not blacklisted, start postpaid checks..")
	}

	postPaid, err := mid_client.IsPostPaid(record.Msisdn)
	if err != nil {
		Errors.Inc()

		err := fmt.Errorf("mid_client.IsPostPaid: %s", err.Error())
		logCtx.WithField("error", err.Error()).Error("cann't get postpaid")
		return err
	}
	if postPaid {
		logCtx.Debug("number is postpaid")
		PostPaid.Inc()

		record.Result = "postpaid"
		record.SubscriptionStatus = "postpaid"
		if err := writeSubscriptionStatus(*record); err != nil {
			Errors.Inc()
			return err
		}
		if record.AttemptsCount >= 1 {
			if err := removeRetry(*record); err != nil {
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

func processResponse(r *rec.Record, retriesEnabled bool) error {
	logCtx := log.WithFields(log.Fields{
		"tid": r.Tid,
	})

	if r.SubscriptionStatus == "postpaid" {
		PostPaid.Inc()
		logCtx.Debug("number is postpaid")

		// order is important
		// if we ad postpaid below in the table (first)
		// and after that record transaction, then
		// we do not notice it in mid and
		// subscription redirects again to operator request
		r.SubscriptionStatus = "postpaid"
		if err := writeSubscriptionStatus(*r); err != nil {
			Errors.Inc()
			return err
		}
		// update postpaid status only if it wasnt already added
		postPaid, _ := mid_client.IsPostPaid(r.Msisdn)
		if !postPaid {
			PostPaid.Inc()
			if err := mid_client.PostPaidPush(r.Msisdn); err != nil {
				Errors.Inc()

				err = fmt.Errorf("mid_client.PostPaidPush: %s", err.Error())
				logCtx.WithFields(log.Fields{
					"msisdn": r.Msisdn,
					"error":  err.Error(),
				}).Error("add mid postpaid error")
				return err
			}

			if err := addPostPaidNumber(*r); err != nil {
				Errors.Inc()

				logCtx.WithFields(log.Fields{
					"msisdn": r.Msisdn,
					"error":  err.Error(),
				}).Error("add postpaid error")

				// sometimes duplicates found despite the check
				return nil
			}
		} else {
			logCtx.Info("already in postpaid mid")
		}
		if retriesEnabled && r.AttemptsCount >= 1 {
			if err := removeRetry(*r); err != nil {
				Errors.Inc()
				return err
			}
		}
		return nil
	}
	if r.SubscriptionStatus == "blacklisted" {
		if err := writeSubscriptionStatus(*r); err != nil {
			Errors.Inc()
			return err
		}
		logCtx.Info("blacklisted")
		return nil
	}

	if r.Paid {
		SinceLastSuccessPay.Set(.0)
		r.SubscriptionStatus = "paid"
		if r.AttemptsCount >= 1 {
			r.Result = "retry_paid"
		} else {
			r.Result = "paid"
		}
		if r.Type == "expired" {
			r.Result = "expired_paid"
		}
		if r.Type == "injection" {
			r.Result = "injection_paid"
		}
	} else {
		r.SubscriptionStatus = "failed"
		if r.AttemptsCount >= 1 {
			r.Result = "retry_failed"
		} else {
			r.Result = "failed"
		}
		if r.Type == "expired" {
			r.Result = "expired_failed"
		}
		if r.Type == "injection" {
			r.Result = "injection_failed"
		}
	}

	logCtx.WithFields(log.Fields{
		"paid":           r.Paid,
		"result":         r.Result,
		"status":         r.SubscriptionStatus,
		"attempts_count": r.AttemptsCount,
	}).Info("response")

	if err := writeSubscriptionStatus(*r); err != nil {
		Errors.Inc()
		return err
	}
	if err := writeTransaction(*r); err != nil {
		Errors.Inc()
		return err
	}
	return nil
}
