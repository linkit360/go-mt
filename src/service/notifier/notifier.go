package notifier

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"

	pixels "github.com/vostrok/pixels/src/notifier"
	transactions "github.com/vostrok/qlistener/src/service"
	"github.com/vostrok/rabbit"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

type Notifier interface {
	OperatorTransactionNotify(msg transactions.OperatorTransactionLog) error
	PixelNotify(msg pixels.Pixel) error
}

type NotifierConfig struct {
	Queue queues            `yaml:"queue"`
	Rbmq  rabbit.RBMQConfig `yaml:"rbmq"`
}
type queues struct {
	OperatorTransactions string `default:"operator_transactions" yaml:"operator_transactions"`
	PixelsQueue          string `default:"pixels" yaml:"pixels"`
}

type notifier struct {
	q  queues
	mq rabbit.AMQPService
}

type EventNotify struct {
	EventName string      `json:"event_name,omitempty"`
	EventData interface{} `json:"event_data,omitempty"`
}

func init() {
	log.SetLevel(log.DebugLevel)
}

func NewNotifierService(conf NotifierConfig) Notifier {
	var n Notifier
	{
		rabbit := rabbit.NewPublisher(conf.Rbmq, rabbit.InitPublisherMetrics())
		n = &notifier{
			q: queues{
				OperatorTransactions: conf.Queue.OperatorTransactions,
				PixelsQueue:          conf.Queue.PixelsQueue,
			},
			mq: rabbit,
		}
	}
	return n
}

func (service notifier) OperatorTransactionNotify(msg transactions.OperatorTransactionLog) error {
	log.WithField("operator_transaction", fmt.Sprintf("%#v", msg)).Debug("got operator transaction")

	event := EventNotify{
		EventName: "operator_transaction",
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	log.WithField("body", string(body)).Debug("sent operator transaction")
	service.mq.Publish(rabbit.AMQPMessage{service.q.OperatorTransactions, body})
	return nil
}

// XXX
func (service notifier) PixelNotify(msg pixels.Pixel) error {
	log.WithField("pixel", fmt.Sprintf("%#v", msg)).Debug("got pixel")

	event := EventNotify{
		EventName: "pixels",
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	log.WithField("body", string(body)).Debug("sent pixels")
	service.mq.Publish(rabbit.AMQPMessage{service.q.PixelsQueue, body})
	return nil
}
