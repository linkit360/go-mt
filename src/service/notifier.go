package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"

	pixels "github.com/vostrok/pixels/src/notifier"
	transaction_log_service "github.com/vostrok/qlistener/src/service"
	"github.com/vostrok/utils/amqp"
	rec "github.com/vostrok/utils/rec"
)

func startRetry(msg rec.Record) error {
	return _notifyDBAction("StartRetry", msg)
}
func addPostPaidNumber(msg rec.Record) error {
	return _notifyDBAction("AddPostPaidNumber", msg)
}
func touchRetry(msg rec.Record) error {
	return _notifyDBAction("TouchRetry", msg)
}
func removeRetry(msg rec.Record) error {
	return _notifyDBAction("RemoveRetry", msg)
}
func writeSubscriptionStatus(msg rec.Record) error {
	return _notifyDBAction("WriteSubscriptionStatus", msg)
}
func writeTransaction(msg rec.Record) error {
	return _notifyDBAction("WriteTransaction", msg)
}
func _notifyDBAction(eventName string, msg rec.Record) (err error) {
	msg.SentAt = time.Now().UTC()
	defer func() {
		if err != nil {
			NotifyErrors.Inc()
			fields := log.Fields{
				"data":  fmt.Sprintf("%#v", msg),
				"queue": svc.conf.Queues.DBActions,
				"event": eventName,
			}
			fields["error"] = fmt.Errorf(eventName+": %s", err.Error())
			fields["rec"] = fmt.Sprintf("%#v", msg)
			log.WithFields(fields).Error("cannot send")
		}
	}()

	if eventName == "" {
		err = fmt.Errorf("QueueSend: %s", "Empty event name")
		return
	}

	event := amqp.EventNotify{
		EventName: eventName,
		EventData: msg,
	}
	var body []byte
	body, err = json.Marshal(event)

	if err != nil {
		err = fmt.Errorf(eventName+" json.Marshal: %s", err.Error())
		return
	}
	svc.notifier.Publish(amqp.AMQPMessage{
		QueueName: svc.conf.Queues.DBActions,
		Body:      body,
	})
	return nil
}

func notifyPixel(r rec.Record) (err error) {
	msg := pixels.Pixel{
		Tid:            r.Tid,
		Msisdn:         r.Msisdn,
		CampaignId:     r.CampaignId,
		SubscriptionId: r.SubscriptionId,
		OperatorCode:   r.OperatorCode,
		CountryCode:    r.CountryCode,
		Pixel:          r.Pixel,
		Publisher:      r.Publisher,
	}

	defer func() {
		fields := log.Fields{
			"tid":   msg.Tid,
			"queue": svc.conf.Queues.Pixels,
		}
		if err != nil {
			fields["errors"] = err.Error()
			fields["pixel"] = fmt.Sprintf("%#v", msg)
			log.WithFields(fields).Error("cannot enqueue")
		} else {
			log.WithFields(fields).Debug("sent")
		}
	}()
	eventName := "pixels"
	event := amqp.EventNotify{
		EventName: eventName,
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return err
	}
	svc.notifier.Publish(amqp.AMQPMessage{svc.conf.Queues.Pixels, uint8(0), body})
	return nil
}

func publishTransactionLog(eventName string,
	transactionMsg transaction_log_service.OperatorTransactionLog) error {
	transactionMsg.SentAt = time.Now().UTC()
	event := amqp.EventNotify{
		EventName: eventName,
		EventData: transactionMsg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		NotifyErrors.Inc()

		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	svc.notifier.Publish(amqp.AMQPMessage{svc.conf.Queues.TransactionLog, 0, body})
	return nil
}