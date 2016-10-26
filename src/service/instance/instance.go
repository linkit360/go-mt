package instance

import (
	"database/sql"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"

	db_conn "github.com/vostrok/db"
	"sync"
)

var mutSubscriptions sync.RWMutex
var mutTransactions sync.RWMutex

type Record struct {
	Msisdn             string
	Result             string
	SubscriptionStatus string
	OperatorCode       int64
	CountryCode        int64
	ServiceId          int64
	SubscriptionId     int64
	CampaignId         int64
	RetryId            int64
	CreatedAt          time.Time
	LastPayAttemptAt   time.Time
	AttemptsCount      int
	KeepDays           int
	DelayHours         int
	OperatorName       string
	OperatorToken      string
	OperatorErr        error
	Price              int
}

var db *sql.DB
var dbConf db_conn.DataBaseConfig

func Init(dbC db_conn.DataBaseConfig) {
	log.SetLevel(log.DebugLevel)

	db = db_conn.Init(dbC)
	dbConf = dbC
	fmt.Println(dbC)
}

func GetNotPaidSubscriptions() ([]Record, error) {
	var subscr []Record
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"msisdn, "+
		"id_service, "+
		"id_campaign, "+
		"operator_code, "+
		"country_code "+
		" FROM %ssubscriptions WHERE"+
		" result = '' OR result = 'failed' ",
		dbConf.TablePrefix)
	rows, err := db.Query(query)
	if err != nil {
		return subscr, fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	for rows.Next() {
		record := Record{}

		if err := rows.Scan(
			&record.SubscriptionId,
			&record.Msisdn,
			&record.ServiceId,
			&record.CampaignId,
			&record.OperatorCode,
			&record.CountryCode,
		); err != nil {
			return subscr, err
		}
		subscr = append(subscr, record)
	}
	if rows.Err() != nil {
		return subscr, fmt.Errorf("GetSubscriptions RowsError: %s", err.Error())
	}

	return subscr, nil
}

func GetRetryTransactions() ([]Record, error) {
	var retries []Record
	query := fmt.Sprintf("SELECT id, "+
		"id, "+
		"created_at, "+
		"last_pay_attempt_at, "+
		"attempts_count, "+
		"keep_days, "+
		"msisdn, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign "+
		" from %sretries",
		dbConf.TablePrefix)
	rows, err := db.Query(query)
	if err != nil {
		return retries, fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	for rows.Next() {
		record := Record{}
		if err := rows.Scan(
			&record.RetryId,
			&record.CreatedAt,
			&record.LastPayAttemptAt,
			&record.AttemptsCount,
			&record.KeepDays,
			&record.Msisdn,
			&record.OperatorCode,
			&record.CountryCode,
			&record.ServiceId,
			&record.SubscriptionId,
			&record.CampaignId,
		); err != nil {
			return retries, fmt.Errorf("Rows.Next: %s", err.Error())
		}

		retries = append(retries, record)
	}
	if rows.Err() != nil {
		return retries, fmt.Errorf("GetRetries RowsError: %s", err.Error())
	}
	return retries, nil
}

func (t Record) GetPreviousSubscription() (Record, error) {

	query := fmt.Sprintf("SELECT id, "+
		"id, "+
		"created_at, "+
		"last_pay_attempt_at, "+
		"attempts_count, "+
		"keep_days, "+
		"msisdn, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign "+
		"FROM %ssubscriptions "+
		"WHERE id != $1 ORDER BY created_at DESC LIMIT 1",
		dbConf.TablePrefix)

	var subscription Record

	mutSubscriptions.Lock()
	defer mutSubscriptions.Unlock()
	if err := db.QueryRow(query, t.SubscriptionId).Scan(
		&subscription.RetryId,
		&subscription.CreatedAt,
		&subscription.LastPayAttemptAt,
		&subscription.AttemptsCount,
		&subscription.KeepDays,
		&subscription.Msisdn,
		&subscription.OperatorCode,
		&subscription.CountryCode,
		&subscription.ServiceId,
		&subscription.SubscriptionId,
		&subscription.CampaignId,
	); err != nil {
		if err == sql.ErrNoRows {
			return subscription, err
		}
		return subscription, fmt.Errorf("db.QueryRow: %s, query: %s", err.Error(), query)
	}
	return subscription, nil
}
func (t Record) WriteTransaction() error {
	query := fmt.Sprintf("INSERT INTO %stransactions ("+
		"msisdn, "+
		"result, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign, "+
		"operator_token, "+
		"price, "+
		") VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)",
		dbConf.TablePrefix)

	mutTransactions.Lock()
	defer mutTransactions.Unlock()
	_, err := db.Exec(
		query,
		t.Msisdn,
		t.Result,
		t.OperatorCode,
		t.CountryCode,
		t.ServiceId,
		t.SubscriptionId,
		t.CampaignId,
		t.OperatorToken,
		100*int(t.Price),
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error ":      err.Error(),
			"query":       query,
			"transaction": t}).
			Error("record transaction failed")
		return fmt.Errorf("QueryExec: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"query":       query,
		"transaction": t,
	}).Info("record transaction done")
	return nil
}

func (subscription Record) WriteSubscriptionStatus() error {
	query := fmt.Sprintf("UPDATE %ssubscriptions "+
		" set result = $1, attempts_count = attempts_count + 1 where id = $2", dbConf.TablePrefix)

	mutSubscriptions.Lock()
	defer mutSubscriptions.Unlock()

	_, err := db.Exec(query,
		subscription.SubscriptionStatus,
		subscription.SubscriptionId,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error ":       err.Error(),
			"query":        query,
			"subscription": subscription,
		}).Error("notify paid subscription failed")
		return fmt.Errorf("QueryExec: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"query":        query,
		"subscription": subscription,
	}).Info("notify paid subscription done")
	return nil
}

func (r Record) RemoveRetry() error {
	query := fmt.Sprintf("DELETE FROM %sretries WHERE id = $1", dbConf.TablePrefix)

	_, err := db.Exec(query, r.RetryId)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"retry":  r}).
			Error("delete retry failed")
		return fmt.Errorf("QueryExec: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"query": query,
		"retry": r}).
		Info("retry deleted")
	return nil
}

func (r Record) TouchRetry() error {
	query := fmt.Sprintf("UPDATE %sretries SET "+
		"last_pay_attempt_at = $1 "+
		"attempts_count = attempts_count + 1 "+
		"WHERE id = $2", dbConf.TablePrefix)

	lastPayAttemptAt := time.Now().UTC().Unix()

	_, err := db.Exec(query, lastPayAttemptAt, r.RetryId)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"retry":  r}).
			Error("update retry failed")
		return fmt.Errorf("QueryExec: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"query": query,
		"retry": r,
	}).Info("retry touch")
	return nil
}

func (r Record) StartRetry() error {
	if r.KeepDays == 0 {
		return fmt.Errorf("Retry Keep Days required, service id: %s", r.ServiceId)
	}
	if r.DelayHours == 0 {
		return fmt.Errorf("Retry Delay Hours required, service id: %s", r.ServiceId)
	}

	query := fmt.Sprintf("INSERT INTO  %sretries ("+
		"keep_days, "+
		"delay_hours, "+
		"msisdn, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign "+
		") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		dbConf.TablePrefix)
	_, err := db.Exec(query,
		&r.KeepDays,
		&r.DelayHours,
		&r.Msisdn,
		&r.OperatorCode,
		&r.CountryCode,
		&r.ServiceId,
		&r.SubscriptionId,
		&r.CampaignId)
	if err != nil {
		return fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
	}

	return nil
}
