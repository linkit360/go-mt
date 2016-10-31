package instance

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	db_conn "github.com/vostrok/db"
)

var mutSubscriptions sync.RWMutex
var mutTransactions sync.RWMutex

type Record struct {
	Msisdn             string    `json:",omitempty"`
	Tid                string    `json:",omitempty"`
	Result             string    `json:",omitempty"`
	SubscriptionStatus string    `json:",omitempty"`
	OperatorCode       int64     `json:",omitempty"`
	CountryCode        int64     `json:",omitempty"`
	ServiceId          int64     `json:",omitempty"`
	SubscriptionId     int64     `json:",omitempty"`
	CampaignId         int64     `json:",omitempty"`
	RetryId            int64     `json:",omitempty"`
	CreatedAt          time.Time `json:",omitempty"`
	LastPayAttemptAt   time.Time `json:",omitempty"`
	AttemptsCount      int       `json:",omitempty"`
	KeepDays           int       `json:",omitempty"`
	DelayHours         int       `json:",omitempty"`
	OperatorName       string    `json:",omitempty"`
	OperatorToken      string    `json:",omitempty"`
	OperatorErr        string    `json:",omitempty"`
	Price              int       `json:",omitempty"`
}

var db *sql.DB
var dbConf db_conn.DataBaseConfig

func Init(dbC db_conn.DataBaseConfig) {
	log.SetLevel(log.DebugLevel)

	db = db_conn.Init(dbC)
	dbConf = dbC
	fmt.Println(dbC)
}

func GetNotPaidSubscriptions(batchLimit int) ([]Record, error) {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"took": time.Since(begin),
		}).Debug("get notpaid subscriptions")
	}()
	var subscr []Record
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"tid, "+
		"msisdn, "+
		"id_service, "+
		"id_campaign, "+
		"operator_code, "+
		"country_code, "+
		"attempts_count "+
		" FROM %ssubscriptions "+
		" WHERE result = '' "+
		" ORDER BY id DESC LIMIT %s",
		dbConf.TablePrefix,
		strconv.Itoa(batchLimit),
	)
	rows, err := db.Query(query)
	if err != nil {
		return subscr, fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	for rows.Next() {
		record := Record{}

		if err := rows.Scan(
			&record.SubscriptionId,
			&record.Tid,
			&record.Msisdn,
			&record.ServiceId,
			&record.CampaignId,
			&record.OperatorCode,
			&record.CountryCode,
			&record.AttemptsCount,
		); err != nil {
			return subscr, err
		}
		subscr = append(subscr, record)
	}
	if rows.Err() != nil {
		return subscr, fmt.Errorf("row.Err: %s", err.Error())
	}

	return subscr, nil
}

func GetRetryTransactions(batchLimit int) ([]Record, error) {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"took": time.Since(begin),
		}).Debug("get retry transactions")
	}()
	var retries []Record
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"tid, "+
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
		"FROM %sretries "+
		"WHERE (CURRENT_TIMESTAMP - delay_hours * INTERVAL '1 hour' ) > last_pay_attempt_at "+
		"ORDER BY last_pay_attempt_at DESC LIMIT %s", // get the last touched
		dbConf.TablePrefix,
		strconv.Itoa(batchLimit),
	)
	rows, err := db.Query(query)
	if err != nil {
		return retries, fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	for rows.Next() {
		record := Record{}
		if err := rows.Scan(
			&record.RetryId,
			&record.Tid,
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

type PreviuosSubscription struct {
	Id        int64
	CreatedAt time.Time
}

func (t Record) GetPreviousSubscription() (PreviuosSubscription, error) {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"tid":  t.Tid,
			"took": time.Since(begin),
		}).Debug("get previous subscription")
	}()
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"created_at "+
		"FROM %ssubscriptions "+
		"WHERE id != $1 AND "+
		"( msisdn = $2 AND id_service = $3 )"+
		"ORDER BY created_at DESC LIMIT 1",
		dbConf.TablePrefix)

	mutSubscriptions.Lock()
	defer mutSubscriptions.Unlock()

	var p PreviuosSubscription
	if err := db.QueryRow(query,
		t.SubscriptionId,
		t.Msisdn,
		t.ServiceId,
	).Scan(
		&p.Id,
		&p.CreatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return p, err
		}
		return p, fmt.Errorf("db.QueryRow: %s, query: %s", err.Error(), query)
	}
	return p, nil
}
func (t Record) WriteTransaction() error {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"tid":  t.Tid,
			"took": time.Since(begin),
		}).Debug("write transaction")
	}()
	query := fmt.Sprintf("INSERT INTO %stransactions ("+
		"msisdn, "+
		"result, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign, "+
		"operator_token, "+
		"price "+
		") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
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
		int(t.Price),
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"tid":    t.Tid,
		}).Error("record transaction failed")
		return fmt.Errorf("db.Exec: %s, Query: %s", err.Error(), query)
	}

	log.WithFields(log.Fields{
		"tid": t.Tid,
	}).Info("write transaction done")
	return nil
}

func (s Record) WriteSubscriptionStatus() error {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"tid":  s.Tid,
			"took": time.Since(begin),
		}).Debug("write subscription status")
	}()
	query := fmt.Sprintf("UPDATE %ssubscriptions SET "+
		"result = $1, "+
		"attempts_count = attempts_count + 1, "+
		"last_pay_attempt_at = $2 "+
		"where id = $3", dbConf.TablePrefix)

	mutSubscriptions.Lock()
	defer mutSubscriptions.Unlock()

	lastPayAttemptAt := time.Now()
	_, err := db.Exec(query,
		s.SubscriptionStatus,
		lastPayAttemptAt,
		s.SubscriptionId,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"tid":    s.Tid,
		}).Error("notify paid subscription failed")
		return fmt.Errorf("db.Exec: %s, Query: %s", err.Error(), query)
	}

	log.WithFields(log.Fields{
		"tid": s.Tid,
	}).Info("write subscription done")
	return nil
}

func (r Record) RemoveRetry() error {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"tid":  r.Tid,
			"took": time.Since(begin),
		}).Debug("remove retry")
	}()
	query := fmt.Sprintf("DELETE FROM %sretries WHERE id = $1", dbConf.TablePrefix)

	_, err := db.Exec(query, r.RetryId)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"tid":    r.Tid,
		}).Error("delete retry failed")
		return fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
	}

	log.WithFields(log.Fields{
		"retry": fmt.Sprintf("%#v", r),
	}).Info("retry deleted")
	return nil
}

func (r Record) TouchRetry() error {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"tid":  r.Tid,
			"took": time.Since(begin),
		}).Debug("touch retry")
	}()
	query := fmt.Sprintf("UPDATE %sretries SET "+
		"last_pay_attempt_at = $1, "+
		"attempts_count = attempts_count + 1 "+
		"WHERE id = $2", dbConf.TablePrefix)

	lastPayAttemptAt := time.Now()

	_, err := db.Exec(query, lastPayAttemptAt, r.RetryId)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"retry":  fmt.Sprintf("%#v", r),
		}).Error("update retry failed")
		return fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
	}

	log.WithFields(log.Fields{
		"tid": r.Tid,
	}).Info("retry touch")
	return nil
}

func (r Record) StartRetry() error {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"tid":  r.Tid,
			"took": time.Since(begin),
		}).Debug("add retry")
	}()
	if r.KeepDays == 0 {
		return fmt.Errorf("Retry Keep Days required, service id: %s", r.ServiceId)
	}
	if r.DelayHours == 0 {
		return fmt.Errorf("Retry Delay Hours required, service id: %s", r.ServiceId)
	}

	query := fmt.Sprintf("INSERT INTO  %sretries ("+
		"tid, "+
		"keep_days, "+
		"delay_hours, "+
		"msisdn, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign "+
		") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
		dbConf.TablePrefix)
	_, err := db.Exec(query,
		&r.Tid,
		&r.KeepDays,
		&r.DelayHours,
		&r.Msisdn,
		&r.OperatorCode,
		&r.CountryCode,
		&r.ServiceId,
		&r.SubscriptionId,
		&r.CampaignId)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"retry":  fmt.Sprintf("%#v", r),
		}).Error("start retry failed")
		return fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
	}
	log.WithFields(log.Fields{
		"tid": r.Tid,
	}).Info("retry start")
	return nil
}
