package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"

	pixels "github.com/linkit360/go-pixel/src/notifier"
	transaction_log_service "github.com/linkit360/go-qlistener/src/service"
	"github.com/linkit360/go-utils/amqp"
	rec "github.com/linkit360/go-utils/rec"
)

func startRetry(msg rec.Record, retriesEnabled bool) error {
	if !retriesEnabled {
		return nil
	}
	if msg.TransactionOnly() {
		return nil
	}
	return _notifyDBAction("StartRetry", msg)
}
func addPostPaidNumber(msg rec.Record) error {
	return _notifyDBAction("AddPostPaidNumber", msg)
}
func touchRetry(msg rec.Record) error {
	if msg.TransactionOnly() {
		return nil
	}
	return _notifyDBAction("TouchRetry", msg)
}
func removeRetry(msg rec.Record) error {
	if msg.TransactionOnly() {
		return nil
	}
	return _notifyDBAction("RemoveRetry", msg)
}
func writeSubscriptionStatus(msg rec.Record) error {
	if msg.TransactionOnly() {
		return nil
	}
	return _notifyDBAction("WriteSubscriptionStatus", msg)
}
func writeSubscriptionPeriodic(msg rec.Record) error {
	if msg.TransactionOnly() {
		return nil
	}
	return _notifyDBAction("WriteSubscriptionPeriodic", msg)
}
func unsubscribe(msg rec.Record) error {
	return _notifyDBAction("Unsubscribe", msg)
}
func unsubscribeAll(msg rec.Record) error {
	return _notifyDBAction("UnsubscribeAll", msg)
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
				"q":     svc.conf.Queues.DBActions,
				"event": eventName,
				"error": fmt.Errorf(eventName+": %s", err.Error()),
			}
			log.WithFields(fields).Error("cannot send")
		} else {
			fields := log.Fields{
				"event":    eventName,
				"tid":      msg.Tid,
				"status":   msg.SubscriptionStatus,
				"periodic": msg.Periodic,
				"q":        svc.conf.Queues.DBActions,
			}
			log.WithFields(fields).Info("sent")
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
			"tid": msg.Tid,
			"q":   svc.conf.Queues.Pixels,
		}
		if err != nil {
			NotifyErrors.Inc()

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
	svc.notifier.Publish(amqp.AMQPMessage{svc.conf.Queues.Pixels, uint8(0), body, event.EventName})
	return nil
}

func notifyRestorePixel(r rec.Record) (err error) {
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
			"tid": msg.Tid,
			"q":   svc.conf.Queues.RestorePixels,
		}
		if err != nil {
			NotifyErrors.Inc()

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
	svc.notifier.Publish(amqp.AMQPMessage{svc.conf.Queues.RestorePixels, uint8(0), body, event.EventName})
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
	svc.notifier.Publish(amqp.AMQPMessage{svc.conf.Queues.TransactionLog, 0, body, event.EventName})
	return nil
}
