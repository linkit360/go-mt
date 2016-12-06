package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/utils/amqp"
	dbconn "github.com/vostrok/utils/db"
	"github.com/vostrok/utils/rec"
)

// get retries from json file and put them into database
type conf struct {
	Db             dbconn.DataBaseConfig        `yaml:"db"`
	InmemConf      inmem_client.RPCClientConfig `yaml:"inmem_client"`
	PuublisherConf amqp.NotifierConfig          `yaml:"publisher"`
}

var postPaid = []string{"<value><i4>11</i4></value>",
	"<value><i4>27</i4></value>",
	"<value><i4>70</i4></value>",
}
var paidMarker = "<value><i4>0</i4></value>"
var publisher *amqp.Notifier

var responseLog *string
var mtLog *string

func main() {
	process()
	//log.SetLevel(log.DebugLevel)
	//row := getRow(`1480784486-259758e6-04c5-4527-59f4-dad6e38ebbd5`)
	//t := getTime(row)
	//token := getOperatorToken(row)
	//log.WithFields(log.Fields{
	//	"row":   row,
	//	"token": token,
	//	"time":  t.String(),
	//}).Debug("parsed")
}

func process() {
	cfg := flag.String("config", "mt_manager.yml", "configuration yml file")
	hours := flag.Int("hours", 1, "hours")
	limit := flag.Int("limit", 1, "limit of retries to process")
	responseLog = flag.String("response", "/var/log/linkit/response_mobilink.log", "response log")
	mtLog = flag.String("mt", "/var/log/linkit/mt.log", "mt log")

	flag.Parse()

	var appConfig conf

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}
	if err := inmem_client.Init(appConfig.InmemConf); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init inmem client")
	}

	publisher = amqp.NewNotifier(appConfig.PuublisherConf)

	rec.Init(appConfig.Db)
	records, err := rec.LoadScriptRetries(*hours, 41001, *limit)
	if err != nil {
		err = fmt.Errorf("rec.LoadPendingRetries: %s", err.Error())
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("cannot get pending retries")
		return
	}
	log.WithFields(log.Fields{
		"count": len(records),
	}).Debug("got retries")

	count := 0
	for _, v := range records {
		log.WithFields(log.Fields{
			"count": count,
		}).Debug("processing")
		count++
		processRetry(v)
	}
}
func processRetry(v rec.Record) {

	cmdOut := getRowResponse(v.Tid)
	if cmdOut == "" {
		log.WithFields(log.Fields{
			"tid": v.Tid,
			"log": *responseLog,
		}).Error("nothing found in response log")

		cmdX := getRowMT(v.Tid)
		if cmdX == "" {
			log.WithFields(log.Fields{
				"tid": v.Tid,
				"log": *mtLog,
			}).Error("nothing found in mt log")
			return
		}
		log.WithFields(log.Fields{
			"tid": v.Tid,
			"mt":  cmdX,
		}).Debug("processing")

		if err := notifyOperatorRequest("mobilink_requests", 0, "charge", v); err != nil {
			err = fmt.Errorf("notifyOperatorRequest: %s, queue: %s", err.Error(), "mobilink_requests")
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Fatal("Cannot send to operator queue")
		}

		return
	}

	v.LastPayAttemptAt = getTime(cmdOut)
	v.CreatedAt = v.LastPayAttemptAt
	v.OperatorToken = getOperatorToken(cmdOut)
	v.Price = getPrice(v)

	log.WithFields(log.Fields{
		"tid":              v.Tid,
		"token":            v.OperatorToken,
		"createdat":        v.CreatedAt.String(),
		"lastPayAttemptAt": v.LastPayAttemptAt.String(),
		"response":         cmdOut,
	}).Debug("processing")

	if v.OperatorToken == "" {
		if !(strings.Contains(cmdOut, "Data out of bounds") ||
			strings.Contains(cmdOut, "client.Do: Post")) {
			log.WithFields(log.Fields{
				"responnse": cmdOut,
			}).Fatal("cannot find operator token")
		} else {
			log.WithFields(log.Fields{
				"responnse": cmdOut,
			}).Error("cannot find operator token")
		}
	}

	if strings.Contains(cmdOut, paidMarker) {
		log.WithFields(log.Fields{
			"tid": v.Tid,
		}).Debug("paid")

		v.SubscriptionStatus = "paid"
		if v.AttemptsCount >= 1 {
			v.Result = "retry_paid"
		} else {
			v.Result = "paid"
		}

		if err := rec.WriteSubscriptionStatus(v); err != nil {
			err = fmt.Errorf("record.WriteSubscriptionStatus : %s", err.Error())
			os.Exit(1)
		}
		if err := rec.WriteTransaction(v); err != nil {
			// already logged inside, wuth query
			err = fmt.Errorf("record.WriteTransaction :%s", err.Error())
			os.Exit(1)
		}
		if err := v.RemoveRetry(); err != nil {
			err = fmt.Errorf("RemoveRetry :%s", err.Error())
			log.WithFields(log.Fields{
				"responnse": cmdOut,
				"error":     err.Error(),
				"tid":       v.Tid,
			}).Fatal("remove from retries failed")
		}
		return
	}

	for _, postpaidStr := range postPaid {
		if strings.Contains(string(cmdOut), postpaidStr) {
			log.WithFields(log.Fields{
				"tid": v.Tid,
			}).Debug("postpaid")

			v.SubscriptionStatus = "postpaid"
			v.Result = "postpaid"
			rec.WriteSubscriptionStatus(v)
			if err := v.RemoveRetry(); err != nil {

				err = fmt.Errorf("RemoveRetry :%s", err.Error())
				log.WithFields(log.Fields{
					"responnse": cmdOut,
					"error":     err.Error(),
					"tid":       v.Tid,
				}).Fatal("remove from retries failed")
			}
			return
		}
	}

	log.WithFields(log.Fields{
		"tid": v.Tid,
	}).Debug("pay failed")

	v.SubscriptionStatus = "failed"
	if v.AttemptsCount >= 1 {
		v.Result = "retry_failed"
	} else {
		v.Result = "failed"
	}
	if err := rec.WriteSubscriptionStatus(v); err != nil {
		// already logged inside, wuth query
		err = fmt.Errorf("record.WriteSubscriptionStatus : %s", err.Error())
		os.Exit(1)
	}
	if err := rec.WriteTransaction(v); err != nil {
		// already logged inside, wuth query
		err = fmt.Errorf("record.WriteTransaction :%s", err.Error())
		os.Exit(1)
	}

	if err := rec.TouchRetry(v); err != nil {
		err = fmt.Errorf("record.TouchRetry: %s", err.Error())
		os.Exit(1)
	}
}

func getTime(row string) time.Time {
	parseString := row[6:26]
	t1, err := time.Parse(
		"2006-01-02T15:04:05Z",
		parseString,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"time":  parseString,
			"error": err.Error(),
		}).Fatal("cannot parse time")
	}
	return t1
}

var r = regexp.MustCompile(`originTransactionID</name>\\n<value><string>(\d+)</string>`)

func getOperatorToken(row string) string {
	//originTransactionID</name>\\n<value><string>92302647243404004837</string>
	arr := r.FindAllString(row, 1)
	if len(arr) == 0 {
		return ""
	}
	return arr[0][43:63]
}
func getRowResponse(tid string) string {
	// zgrep 1480784486-259758e6-04c5-4527-59f4-dad6e38ebbd5 response_mobilink.log-20161205.gz
	// | egrep -o
	cmdName := "/usr/bin/grep"
	if strings.Contains(*responseLog, ".gz") {
		cmdName = "/usr/bin/zgrep"
	}
	cmdArgs := []string{tid, *responseLog}

	cmdOut, err := exec.Command(cmdName, cmdArgs...).Output()
	if err != nil {
		log.WithFields(log.Fields{
			"tid":   tid,
			"error": err.Error(),
		}).Error("cannot get row")
		return ""
	}
	return string(cmdOut)
}

func getRowMT(tid string) string {
	// zgrep 1480784486-259758e6-04c5-4527-59f4-dad6e38ebbd5 response_mobilink.log-20161205.gz
	// | egrep -o
	cmdName := "/usr/bin/grep"
	if strings.Contains(*mtLog, ".gz") {
		cmdName = "/usr/bin/zgrep"
	}
	cmdArgs := []string{tid, *mtLog}
	cmdOut, err := exec.Command(cmdName, cmdArgs...).Output()
	if err != nil {
		log.WithFields(log.Fields{
			"tid":   tid,
			"error": err.Error(),
		}).Error("cannot get row")
		return ""
	}
	return string(cmdOut)
}

func getPrice(v rec.Record) int {
	mService, err := inmem_client.GetServiceById(v.ServiceId)
	if err != nil {
		err := fmt.Errorf("inmem_client.GetServiceById %d: %s", v.ServiceId, err.Error())
		log.WithFields(log.Fields{
			"error": err.Error(),
			"tid":   v.Tid,
		}).Fatal("cannot get service by id")
	}
	if mService.Price <= 0 {
		err := fmt.Errorf("Service price %d is zero or less", mService.Price)
		log.WithFields(log.Fields{
			"error": err.Error(),
			"tid":   v.Tid,
		}).Fatal("cannot continue with zero price")
	}
	return 100 * int(mService.Price)
}

func notifyOperatorRequest(queue string, priority uint8, eventName string, msg rec.Record) error {
	if eventName == "" {
		return fmt.Errorf("QueueSend: %s", "empty event name")
	}
	if queue == "" {
		return fmt.Errorf("QueueSend: %s", "empty queue name")
	}

	event := amqp.EventNotify{
		EventName: eventName,
		EventData: msg,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	log.WithFields(log.Fields{
		"tid":   msg.Tid,
		"queue": queue,
		"event": eventName,
	}).Debug("sent")

	publisher.Publish(amqp.AMQPMessage{
		QueueName: queue,
		Priority:  priority,
		Body:      body,
	})
	return nil
}
