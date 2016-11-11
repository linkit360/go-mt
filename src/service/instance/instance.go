package instance

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/db"
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
	Paid               bool      `json:"paid"`
	Price              int       `json:",omitempty"`
	Pixel              string    `json:",omitempty"`
	Publisher          string    `json:",omitempty"`
}

var dbConn *sql.DB
var conf db.DataBaseConfig

func Init(dbC db.DataBaseConfig) {
	log.SetLevel(log.DebugLevel)
	dbConn = db.Init(dbC)
	conf = dbC
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
		"pixel, "+
		"publisher, "+
		"id_service, "+
		"id_campaign, "+
		"operator_code, "+
		"country_code, "+
		"attempts_count "+
		" FROM %ssubscriptions "+
		" WHERE result = '' "+
		" ORDER BY id DESC LIMIT %s",
		conf.TablePrefix,
		strconv.Itoa(batchLimit),
	)
	rows, err := dbConn.Query(query)
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
			&record.Pixel,
			&record.Publisher,
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
		"pixel, "+
		"publisher, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign "+
		"FROM %sretries "+
		"WHERE (CURRENT_TIMESTAMP - delay_hours * INTERVAL '1 hour' ) > last_pay_attempt_at "+
		"ORDER BY last_pay_attempt_at DESC LIMIT %s", // get the last touched
		conf.TablePrefix,
		strconv.Itoa(batchLimit),
	)
	rows, err := dbConn.Query(query)
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
			&record.Pixel,
			&record.Publisher,
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

	// if the user get's content twice in a second period
	// then we could handle both of them in sequesnce
	// then to process, we should get really previous - before this one
	query := fmt.Sprintf("SELECT "+
		"id, "+
		"created_at "+
		"FROM %ssubscriptions "+
		"WHERE id < $1 AND "+
		" msisdn = $2 AND id_service = $3 "+
		"ORDER BY created_at DESC LIMIT 1",
		conf.TablePrefix)

	mutSubscriptions.Lock()
	defer mutSubscriptions.Unlock()

	var p PreviuosSubscription
	if err := dbConn.QueryRow(query,
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
		conf.TablePrefix)

	mutTransactions.Lock()
	defer mutTransactions.Unlock()
	_, err := dbConn.Exec(
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
		"where id = $3", conf.TablePrefix)

	mutSubscriptions.Lock()
	defer mutSubscriptions.Unlock()

	lastPayAttemptAt := time.Now()
	_, err := dbConn.Exec(query,
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
	query := fmt.Sprintf("DELETE FROM %sretries WHERE id = $1", conf.TablePrefix)

	_, err := dbConn.Exec(query, r.RetryId)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"tid":    r.Tid,
		}).Error("delete retry failed")
		return fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
	}
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
		"WHERE id = $2", conf.TablePrefix)

	lastPayAttemptAt := time.Now()

	_, err := dbConn.Exec(query, lastPayAttemptAt, r.RetryId)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"retry":  fmt.Sprintf("%#v", r),
		}).Error("update retry failed")
		return fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
	}
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
		"pixel, "+
		"publisher, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign "+
		") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
		conf.TablePrefix)
	_, err := dbConn.Exec(query,
		&r.Tid,
		&r.KeepDays,
		&r.DelayHours,
		&r.Msisdn,
		&r.OperatorCode,
		&r.CountryCode,
		&r.Pixel,
		&r.Publisher,
		&r.ServiceId,
		&r.SubscriptionId,
		&r.CampaignId)
	if err != nil {
		return fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
	}
	return nil
}

func (r Record) AddBlacklistedNumber() error {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"took":   time.Since(begin),
		}).Debug("add blacklisted")
	}()

	query := fmt.Sprintf("INSERT INTO  %smsisdn_blacklist ( msisdn ) VALUES ($1)",
		conf.TablePrefix)
	if _, err := dbConn.Exec(query, &r.Msisdn); err != nil {
		return fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
	}
	return nil
}

func (r Record) AddPostPaidNumber() error {
	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"tid":    r.Tid,
			"msisdn": r.Msisdn,
			"took":   time.Since(begin),
		}).Debug("add postpaid")
	}()

	query := fmt.Sprintf("INSERT INTO  %smsisdn_postpaid ( msisdn ) VALUES ($1)",
		conf.TablePrefix)
	if _, err := dbConn.Exec(query, &r.Msisdn); err != nil {
		return fmt.Errorf("db.Exec: %s, query: %s", err.Error(), query)
	}
	return nil
}
