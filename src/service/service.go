package service

// does following:
// get subscription records that are in "" status (not tries to pay) or in "failed" status
// tries to charge via operator
// if not, set up retries

// get all retries
// retry transaction to the operator
// if everything is ok, then remove item
// if not, "touch" item == renew attempts count and last attempt date

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	rec "github.com/vostrok/mt_manager/src/service/instance"
	m "github.com/vostrok/mt_manager/src/service/metrics"
)

func processResponses(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyTarifficate
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			m.ResponseDropped.Inc()

			log.WithFields(log.Fields{
				"error":    err.Error(),
				"msg":      "dropped",
				"response": string(msg.Body),
			}).Error("consume failed")
			msg.Ack(false)
			continue
		}

		go func(r rec.Record) {
			if err := handleResponse(e.EventData); err != nil {
				m.ResponseErrors.Inc()
			} else {
				m.ResponseSuccess.Inc()
			}
		}(e.EventData)
	}
}

func smsSend(record rec.Record, msg string) error {
	operatorName, ok := memOperators.ByCode[record.OperatorCode]
	if !ok {
		log.WithFields(log.Fields{
			"tid":    record.Tid,
			"msisdn": record.Msisdn,
		}).Debug("SMS send: not applicable to any operator")
		return fmt.Errorf("Code %s is not applicable to any operator", record.OperatorCode)
	}
	operatorName = strings.ToLower(operatorName)
	queue, ok := svc.conf.Queues.Operator[operatorName]
	if !ok {
		log.WithFields(log.Fields{
			"tid":    record.Tid,
			"msisdn": record.Msisdn,
		}).Debug("SMS send: not enabled in mt_manager")
		return fmt.Errorf("Name %s is not enabled", operatorName)
	}
	if err := notifyOperatorRequest(queue.SMS, "sms_send", record); err != nil {
		err = fmt.Errorf("notifyOperatorRequest: %s, queue: %s", err.Error(), queue)
		log.WithFields(log.Fields{
			"tid":    record.Tid,
			"msisdn": record.Msisdn,
			"error", err.Error(),
		}).Error("Cannot send to operator SMS queue")
		return err
	}
	return nil
}

func handleResponse(record rec.Record) error {
	logCtx := log.WithFields(log.Fields{
		"tid": record.Tid,
	})
	logCtx.Info("start processing response")

	if record.SubscriptionStatus == "postpaid" {
		m.PostPaid.Inc()
		logCtx.Debug("number is postpaid")
		if err := record.AddPostPaidNumber(); err != nil {
			err = fmt.Errorf("record.AddPostPaidNumber: %s", err.Error())
			logCtx.WithField("error", err.Error()).Error("add postpaid error")
			m.Errors.Inc()
			return err
		}
		logCtx.Info("new postpaid number added")
		memPostPaid.Reload()
		record.SubscriptionStatus = "postpaid"
		record.WriteSubscriptionStatus()
		return nil
	}

	if len(record.OperatorToken) > 0 && record.Paid {
		m.SinceSuccessPaid.Set(.0)
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

	logCtx.WithFields(log.Fields{
		"result": record.Result,
		"status": record.SubscriptionStatus,
	}).Info("got response")

	if err := record.WriteSubscriptionStatus(); err != nil {
		// already logged inside, wuth query
		err = fmt.Errorf("record.WriteSubscriptionStatus : %s", err.Error())
		return err
	}
	if err := record.WriteTransaction(); err != nil {
		m.Errors.Inc()
		// already logged inside, wuth query
		err = fmt.Errorf("record.WriteTransaction :%s", err.Error())
		return err
	}

	// add retries
	if record.AttemptsCount == 0 && record.SubscriptionStatus == "failed" {
		logCtx.WithFields(log.Fields{
			"action": "move to retry",
		}).Debug("subscription")

		mService, ok := memServices.Map[record.ServiceId]
		if !ok {
			m.Errors.Inc()
			err := fmt.Errorf("memServices.Map: %d", record.ServiceId)
			logCtx.WithFields(log.Fields{
				"error": "Service not found",
			}).Error("cannot process subscription")
			return err
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

		} else {
			logCtx.WithField("service id", mService.Id).Info("send sms disabled on service")
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
			logCtx.Info("remove retry: keep days expired")
			remove = true
		}
		if record.Result == "retry_paid" {
			logCtx.Info("remove retry: retry_paid")
			remove = true
		}
		if remove {
			if err := record.RemoveRetry(); err != nil {
				m.Errors.Inc()

				err = fmt.Errorf("record.RemoveRetry :%s", err.Error())
				logCtx.WithField("error", err.Error()).Error("remove from retries failed")
				return err
			}
		} else {
			if err := record.TouchRetry(); err != nil {
				m.Errors.Inc()

				err = fmt.Errorf("record.TouchRetry: %s", err.Error())
				logCtx.WithField("error", err.Error()).Error("touch retry failed")
				return err
			}
		}

	}

	return nil
}
