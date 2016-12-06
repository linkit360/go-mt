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
			goto ack
		}

		if err := handleSMSResponse(e.EventData); err != nil {
			ResponseSMSErrors.Inc()
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
		ResponseSMSSuccess.Inc()
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

func smsSend(record rec.Record, msg string) error {
	operator, err := inmem_client.GetOperatorByCode(record.OperatorCode)
	if err != nil {
		OperatorNotApplicable.Inc()

		log.WithFields(log.Fields{
			"tid":    record.Tid,
			"msisdn": record.Msisdn,
		}).Error("SMS send: not applicable to any operator")
		return fmt.Errorf("Code %s is not applicable to any operator", record.OperatorCode)
	}
	operatorName := strings.ToLower(operator.Name)
	queue, ok := svc.conf.QueueOperators[operatorName]
	if !ok {
		OperatorNotEnabled.Inc()

		log.WithFields(log.Fields{
			"tid":    record.Tid,
			"msisdn": record.Msisdn,
		}).Error("SMS send: not enabled in mt_manager")
		return fmt.Errorf("Name %s is not enabled", operatorName)
	}
	record.SMSText = msg
	if err := notifyOperatorRequest(queue.SMSRequest, 0, "send_sms", record); err != nil {

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
	if err := writeTransaction(smsTranasction); err != nil {
		Errors.Inc()
		return err
	}
	return nil
}
