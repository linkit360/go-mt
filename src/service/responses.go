package service

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

func processResponses(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyTarifficate
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			ResponseDropped.Inc()

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
				ResponseErrors.Inc()
			} else {
				ResponseSuccess.Inc()
			}
			msg.Ack(false)
		}(e.EventData)
	}
}

func processSMSResponses(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyTarifficate
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			ResponseDropped.Inc()

			log.WithFields(log.Fields{
				"error":    err.Error(),
				"msg":      "dropped",
				"response": string(msg.Body),
			}).Error("consume failed")
			msg.Ack(false)
			continue
		}

		go func(r rec.Record) {
			if err := handleSMSResponse(e.EventData); err != nil {
				ResponseSMSErrors.Inc()
			} else {
				ResponseSMSSuccess.Inc()
			}
			msg.Ack(false)
		}(e.EventData)
	}
}

func smsSend(record rec.Record, msg string) error {
	operator, err := inmem_client.GetOperatorByCode(record.OperatorCode)
	if err != nil {
		OperatorNotApplicable.Inc()
		log.WithFields(log.Fields{
			"tid":    record.Tid,
			"msisdn": record.Msisdn,
		}).Debug("SMS send: not applicable to any operator")
		return fmt.Errorf("Code %s is not applicable to any operator", record.OperatorCode)
	}
	operatorName := strings.ToLower(operator.Name)
	queue, ok := svc.conf.QueueOperators[operatorName]
	if !ok {
		OperatorNotEnabled.Inc()

		log.WithFields(log.Fields{
			"tid":    record.Tid,
			"msisdn": record.Msisdn,
		}).Debug("SMS send: not enabled in mt_manager")
		return fmt.Errorf("Name %s is not enabled", operatorName)
	}
	record.SMSText = msg
	if err := notifyOperatorRequest(queue.SMSRequest, "send_sms", record); err != nil {
		err = fmt.Errorf("notifyOperatorRequest: %s, queue: %s", err.Error(), queue)
		log.WithFields(log.Fields{
			"tid":    record.Tid,
			"msisdn": record.Msisdn,
			"error":  err.Error(),
		}).Error("Cannot send to operator SMS queue")
		return err
	}
	return nil
}

func handleSMSResponse(record rec.Record) error {
	smsTranasction := record
	smsTranasction.Result = "sms"
	if smsTranasction.OperatorErr != "" {
		smsTranasction.Result = "sms failed"
	} else {
		smsTranasction.Result = "sms sent"
	}
	if err := smsTranasction.WriteTransaction(); err != nil {
		log.WithFields(log.Fields{
			"tid":    smsTranasction.Tid,
			"msisdn": smsTranasction.Msisdn,
			"error":  err.Error(),
		}).Error("failed to write sms transaction")
		return err
	}
	return nil
}

func handleResponse(record rec.Record) error {
	TarificateResponsesReceived.Inc()
	logCtx := log.WithFields(log.Fields{
		"tid": record.Tid,
	})
	logCtx.Info("start processing response")

	if record.OperatorErr != "" {
		TarificateFailed.Inc()
	}
	if record.SubscriptionStatus == "postpaid" {
		PostPaid.Inc()
		logCtx.Debug("number is postpaid")
		if err := record.AddPostPaidNumber(); err != nil {
			err = fmt.Errorf("record.AddPostPaidNumber: %s", err.Error())
			logCtx.WithField("error", err.Error()).Error("add postpaid error")
			Errors.Inc()
			return err
		}
		logCtx.Info("new postpaid number added")
		inmem_client.PostPaidPush(record.Msisdn)
		record.SubscriptionStatus = "postpaid"
		record.WriteSubscriptionStatus()
		return nil
	}

	if len(record.OperatorToken) > 0 && record.Paid {
		SinceSuccessPaid.Set(.0)
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
		Errors.Inc()
		// already logged inside, wuth query
		err = fmt.Errorf("record.WriteTransaction :%s", err.Error())
		return err
	}

	// add retries
	if record.AttemptsCount == 0 && record.SubscriptionStatus == "failed" {
		logCtx.WithFields(log.Fields{
			"action": "move to retry",
		}).Debug("subscription")

		mService, err := inmem_client.GetServiceById(record.ServiceId)
		if err != nil {
			Errors.Inc()
			err := fmt.Errorf("GetServiceById: %d", record.ServiceId)
			logCtx.WithFields(log.Fields{
				"error": err.Error(),
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
			logCtx.WithField("serviceId", mService.Id).Debug("send sms disabled on service")
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
				Errors.Inc()

				err = fmt.Errorf("record.RemoveRetry :%s", err.Error())
				logCtx.WithField("error", err.Error()).Error("remove from retries failed")
				return err
			}
		} else {
			if err := record.TouchRetry(); err != nil {
				Errors.Inc()

				err = fmt.Errorf("record.TouchRetry: %s", err.Error())
				logCtx.WithField("error", err.Error()).Error("touch retry failed")
				return err
			}
		}

	}

	return nil
}
