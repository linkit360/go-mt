package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/utils/amqp"
	dbconn "github.com/vostrok/utils/db"
	m "github.com/vostrok/utils/metrics"
	"github.com/vostrok/utils/rec"
)

// get retries from json file and put them into database
type conf struct {
	Db             dbconn.DataBaseConfig `yaml:"db"`
	PuublisherConf amqp.NotifierConfig   `yaml:"publisher"`
}

var publisher *amqp.Notifier

var responseLog *string

func main() {
	process()
}

var paidCount = 0

func process() {
	cfg := flag.String("config", "mt_manager.yml", "configuration yml file")
	hours := flag.Int("hours", 1, "hours")
	limit := flag.Int("limit", 1, "limit of retries to process")
	responseLog = flag.String("response", "/var/log/linkit/response_mobilink.log", "response log")

	flag.Parse()

	var appConfig conf

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
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
		}).Debug("")
		count++
		processRetry(v)
	}
	log.Infof("paid: %d", paidCount)
}

func processRetry(v rec.Record) {

	cmdOut := getRowResponse(v.Tid)
	if cmdOut == "" {
		rec.SetRetryStatus("", v.RetryId)
		return
	}
	v.LastPayAttemptAt = getTime(cmdOut)
	v.SentAt = v.LastPayAttemptAt

	if strings.Contains(cmdOut, "<value><i4>0</i4></value>") {
		log.WithFields(log.Fields{
			"tid": v.Tid,
		}).Debug("paid!")

		v.Paid = true
		paidCount++
	} else {
		log.WithFields(log.Fields{
			"tid": v.Tid,
		}).Debug("not paid")
	}

	if err := publishResponse("script", v); err != nil {
		err = fmt.Errorf("notify failed: %s", err.Error())
		os.Exit(1)
	}
	log.WithFields(log.Fields{
		"tid": v.Tid,
	}).Debug("sent")
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

func getRowResponse(tid string) string {
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

func publishResponse(eventName string, data interface{}) error {
	event := amqp.EventNotify{
		EventName: eventName,
		EventData: data,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("json.Marshal: %s", err.Error())
	}
	publisher.Publish(amqp.AMQPMessage{"mobilink_responses", 0, body})
	return nil
}
