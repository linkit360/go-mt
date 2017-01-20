package main

import (
	"encoding/json"
	"flag"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"
	"github.com/vostrok/utils/amqp"

	"github.com/vostrok/utils/db"
	dbconn "github.com/vostrok/utils/db"
	m "github.com/vostrok/utils/metrics"
	"github.com/vostrok/utils/rec"
	//"strconv"
)

// get retries from json file and put them into database
type conf struct {
	Db            dbconn.DataBaseConfig
	PublisherConf amqp.NotifierConfig `yaml:"publisher"`
}

var notifier *amqp.Notifier

func main() {
	cfg := flag.String("config", "mt_manager.yml", "configuration yml file")
	//count := flag.Int("count", 500, "count of retries")
	flag.Parse()

	var appConfig conf

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}

	notifier = amqp.NewNotifier(appConfig.PublisherConf)

	dbConn := db.Init(appConfig.Db)

	var retries []rec.Record
	var err error
	var query string

	query = fmt.Sprintf("SELECT "+
		"id, "+
		"tid, "+
		"created_at, "+
		"last_pay_attempt_at, "+
		"attempts_count, "+
		"keep_days, "+
		"delay_hours, "+
		"msisdn, "+
		"price, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign "+
		"FROM %sretries "+
		"WHERE (CURRENT_TIMESTAMP - delay_hours * INTERVAL '1 hour' ) > last_pay_attempt_at AND "+
		"operator_code = 41001 AND "+
		" status = '' "+
		" ORDER BY last_pay_attempt_at DESC "+
		" LIMIT 500", // get the last touched
		appConfig.Db.TablePrefix,
	)

	rows, err := dbConn.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		log.Fatal(err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		record := rec.Record{}
		if err := rows.Scan(
			&record.RetryId,
			&record.Tid,
			&record.CreatedAt,
			&record.LastPayAttemptAt,
			&record.AttemptsCount,
			&record.KeepDays,
			&record.DelayHours,
			&record.Msisdn,
			&record.Price,
			&record.OperatorCode,
			&record.CountryCode,
			&record.ServiceId,
			&record.SubscriptionId,
			&record.CampaignId,
		); err != nil {
			err = fmt.Errorf("db.Scan: %s, query: %s", err.Error(), query)
			log.Fatal(err.Error())
		}
		retries = append(retries, record)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("GetRetries RowsError: %s", err.Error())
		log.Fatal(err.Error())
	}

	if err != nil {
		log.WithField("error", err.Error()).Fatal(" error")
	}
	log.WithField("count", len(retries)).Info("done")
}

func publishToTelcoAPI(priority uint8, r rec.Record) (err error) {
	event := amqp.EventNotify{
		EventName: "charge",
		EventData: r,
	}
	body, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return
	}
	notifier.Publish(amqp.AMQPMessage{QueueName: "mobilink_requests", Priority: priority, Body: body})
	return nil
}
