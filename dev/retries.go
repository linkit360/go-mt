package main

import (
	"database/sql"
	"flag"
	"fmt"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	dbconn "github.com/vostrok/utils/db"
	"github.com/vostrok/utils/rec"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

// get retries from json file and put them into database
type conf struct {
	Db dbconn.DataBaseConfig
}

var dbConn *sql.DB

func main() {
	cfg := flag.String("config", "mt_manager.yml", "configuration yml file")
	count := flag.Int("count", 1, "count")
	flag.Parse()

	var appConfig conf

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}

	dbConn = dbconn.Init(appConfig.Db)
	getRetry(*count)
}

func getRetry(limit int) {
	query := "select msisdn, count(*) from xmp_retries GROUP BY msisdn HAVING count(*) >= 2"
	if limit > 0 {
		query = query + " LIMIT " + strconv.Itoa(limit)
	}

	var rows *sql.Rows
	rows, err := dbConn.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		log.WithField("error", err.Error()).Fatal("query")
		return
	}
	defer rows.Close()

	msisdns := []string{}
	for rows.Next() {
		var msisdn string
		var count int
		if err = rows.Scan(
			&msisdn,
			&count,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			log.WithField("error", err.Error()).Fatal("query")
			return
		}
		log.WithFields(log.Fields{
			"msisdn": msisdn,
			"count":  count,
		}).Debug("got")
		msisdns = append(msisdns, msisdn)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		log.WithField("error", err.Error()).Fatal("query")
		return
	}
	log.WithField("count", len(msisdns)).Info("through tids")
	for _, msisdn := range msisdns {
		log.WithField("msisdn", msisdn).Info("get retries")
		query = "SELECT id, created_at FROM xmp_retries WHERE msisdn = $1 ORDER BY created_at DESC"
		rows, err := dbConn.Query(query, msisdn)
		if err != nil {
			err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
			log.WithField("error", err.Error()).Fatal("query")
			return
		}
		log.WithField("msisdn", msisdn).Debug("got retries")
		defer rows.Close()
		count := 0
		for rows.Next() {
			log.WithFields(log.Fields{
				"count":  count,
				"msisdn": msisdn,
			}).Debug("start")
			var r rec.Record
			if err = rows.Scan(
				&r.RetryId,
				&r.CreatedAt,
			); err != nil {
				err = fmt.Errorf("rows.Scan: %s", err.Error())
				log.WithField("error", err.Error()).Fatal("query")

			}
			log.WithFields(log.Fields{
				"created_at": r.CreatedAt,
				"id":         r.RetryId,
				"tid":        msisdn,
			}).Debug("process")

			if count == 0 {
				log.WithFields(log.Fields{
					"created_at": r.CreatedAt,
					"id":         r.RetryId,
					"tid":        msisdn,
				}).Debug("skip")
				count++
				continue
			}
			query = `INSERT INTO
			xmp_retries_expired(
				  status,
				  tid,
				  created_at ,
				  price,
				  last_pay_attempt_at ,
				  attempts_count ,
				  keep_days ,
				  delay_hours ,
				  msisdn ,
				  operator_code ,
				  country_code ,
				  id_service ,
				  id_subscription ,
				  id_campaign
			)
			SELECT
				  status,
				  tid ,
				  created_at ,
				  price,
				  last_pay_attempt_at ,
				  attempts_count ,
				  keep_days ,
				  delay_hours ,
				  msisdn ,
				  operator_code ,
				  country_code ,
				  id_service ,
				  id_subscription ,
				  id_campaign
			FROM xmp_retries WHERE id = $1`
			if _, err = dbConn.Exec(query, r.RetryId); err != nil {
				err = fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
				log.WithField("error", err.Error()).Fatal("query")
			}
			log.WithFields(log.Fields{
				"created_at": r.CreatedAt,
				"id":         r.RetryId,
				"tid":        msisdn,
			}).Debug("added to expired")

			query = "delete from xmp_retries WHERE id = $1 "
			_, err = dbConn.Exec(query, r.RetryId)
			if err != nil {
				err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
				log.WithField("error", err.Error()).Fatal("query")

			}
			log.WithFields(log.Fields{
				"id":  r.RetryId,
				"tid": msisdn,
			}).Debug("removed from retries")
		}
		if rows.Err() != nil {
			err = fmt.Errorf("rows.Err: %s", err.Error())
			log.WithField("error", err.Error()).Fatal("query")
			return
		}
		log.WithFields(log.Fields{
			"tid": msisdn,
		}).Info("done")
	}

	return
}
