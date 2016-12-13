package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	pixels "github.com/vostrok/pixels/src/notifier"
	rec "github.com/vostrok/utils/rec"
)

func processResponses(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process response")

		var e EventNotifyTarifficate
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			ResponseDropped.Inc()

			log.WithFields(log.Fields{
				"error":    err.Error(),
				"msg":      "dropped",
				"response": string(msg.Body),
			}).Error("consume failed")
			goto ack
		}
		if err := handleResponse(e.EventData); err != nil {
			ResponseErrors.Inc()
		nack:
			if err := msg.Nack(false, true); err != nil {
				log.WithFields(log.Fields{
					"tid":   e.EventData.Tid,
					"error": err.Error(),
				}).Error("cannot nack")
				time.Sleep(time.Second)
				goto nack
			}
		}
		ResponseSuccess.Inc()
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

func handleResponse(record rec.Record) error {
	TarificateResponsesReceived.Inc()
	logCtx := log.WithFields(log.Fields{
		"tid": record.Tid,
	})
	logCtx.Info("start response")

	if record.OperatorErr != "" {
		TarificateFailed.Inc()
	}
	if record.SubscriptionStatus == "postpaid" {
		PostPaid.Inc()
		logCtx.Debug("number is postpaid")

		// order is important
		// if we ad postpaid below in the table (first)
		// and after that record transaction, then
		// we do not notice it in inmem and
		// subscription redirects again to operator request
		record.SubscriptionStatus = "postpaid"
		if err := writeSubscriptionStatus(record); err != nil {
			Errors.Inc()
			return err
		}

		// update postpaid status only if it wasnt already added
		postPaid, _ := inmem_client.IsPostPaid(record.Msisdn)
		if !postPaid {
			if err := inmem_client.PostPaidPush(record.Msisdn); err != nil {
				Errors.Inc()

				err = fmt.Errorf("inmem_client.PostPaidPush: %s", err.Error())
				logCtx.WithFields(log.Fields{
					"msisdn": record.Msisdn,
					"error":  err.Error(),
				}).Error("add inmem postpaid error")
				return err
			}
			if err := addPostPaidNumber(record); err != nil {
				Errors.Inc()

				logCtx.WithFields(log.Fields{
					"msisdn": record.Msisdn,
					"error":  err.Error(),
				}).Error("add postpaid error")

				// sometimes duplicates found despite the check
				return nil
			}
			logCtx.Info("new postpaid number added")
		} else {
			logCtx.Info("already in postpaid inmem")
		}

		if record.AttemptsCount >= 1 {
			if err := removeRetry(record); err != nil {
				Errors.Inc()
				return err
			}
		}
		return nil
	}

	if len(record.OperatorToken) > 0 && record.Paid {
		Paid.Inc()
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
		"paid":   record.Paid,
		"result": record.Result,
		"status": record.SubscriptionStatus,
	}).Info("statuses")

	if err := writeSubscriptionStatus(record); err != nil {
		Errors.Inc()
		return err
	}
	if err := writeTransaction(record); err != nil {
		Errors.Inc()
		return err
	}

	// add to retries
	if record.AttemptsCount == 0 && record.SubscriptionStatus == "failed" {
		logCtx.WithFields(log.Fields{
			"action": "move to retry",
		}).Debug("mo")

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
			if err := smsSend(record, mService.SMSNotPaidText); err != nil {
				Errors.Inc()

				logCtx.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("sms send")
				return err
			}
			if err := writeTransaction(record); err != nil {
				Errors.Inc()

				logCtx.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("failed to write sms transaction")
				return err
			}
		} else {
			logCtx.WithField("serviceId", mService.Id).Debug("send sms disabled on service")
		}
		if err := startRetry(record); err != nil {
			Errors.Inc()

			logCtx.WithFields(log.Fields{
				"error": err.Error(),
				"retry": fmt.Sprintf("%#v", record),
			}).Error("add to retries failed")
			return err
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
			remove = true
		}
		if remove {
			if err := removeRetry(record); err != nil {
				Errors.Inc()
				return err
			}
		} else {
			if err := touchRetry(record); err != nil {
				Errors.Inc()
				return err
			}
		}
	}

	// send everything, pixels module will decide to send pixel, or not to send
	if record.Pixel != "" &&
		record.AttemptsCount == 0 &&
		record.SubscriptionStatus != "postpaid" {
		Pixel.Inc()

		if err := notifyPixel(pixels.Pixel{
			Tid:            record.Tid,
			Msisdn:         record.Msisdn,
			CampaignId:     record.CampaignId,
			SubscriptionId: record.SubscriptionId,
			OperatorCode:   record.OperatorCode,
			CountryCode:    record.CountryCode,
			Pixel:          record.Pixel,
			Publisher:      record.Publisher,
		}); err != nil {
			Errors.Inc()

			err = fmt.Errorf("notifyPixel: %s", err.Error())
			return err
		}
	} else {
		logCtx.WithFields(log.Fields{
			"attemptsCount": record.AttemptsCount,
		}).Debug("skip send pixel")
	}

	return nil
}
