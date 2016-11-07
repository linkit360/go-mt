package notifier

import (
	"encoding/json"
	"fmt"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/pixels/src/service"
	"github.com/vostrok/rabbit"
)

type Notifier interface {
	PaidNotify(msg service.Pixel) error
}

type NotifierConfig struct {
	Queue queues            `yaml:"queue"`
	Rbmq  rabbit.RBMQConfig `yaml:"rbmq"`
}
type queues struct {
	PixelsQueue string `default:"pixels" yaml:"pixels"`
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
		rabbit := rabbit.NewPublisher(conf.Rbmq, rabbit.InitMetrics("mt_manager"))
		n = &notifier{
			q: queues{
				PixelsQueue: conf.Queue.PixelsQueue,
			},
			mq: rabbit,
		}
	}
	return n
}

func (service notifier) PaidNotify(msg service.Pixel) error {
	log.WithField("pixel", fmt.Sprintf("%#v", msg)).Debug("got pixel")

	event := EventNotify{
		EventName: "pixels",
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	log.WithField("body", string(body)).Debug("sent body")
	service.mq.Publish(rabbit.AMQPMessage{service.q.PixelsQueue, body})
	return nil
}
