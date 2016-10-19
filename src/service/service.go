package service

import (
	"database/sql"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/db"
	"github.com/vostrok/mt/src/service/mobilink"
)

var svc MTService

func Init(sConf MTServiceConfig) {
	svc.db = db.Init(sConf.DbConf)
	svc.sConfig = sConf
	svc.mobilink = mobilink.Init(sConf.Mobilink)
	log.Info("mt service init ok")
}

type MTService struct {
	db       *sql.DB
	sConfig  MTServiceConfig
	mobilink mobilink.Mobilink
}
type MTServiceConfig struct {
	DbConf   db.DataBaseConfig `yaml:"db"`
	Mobilink mobilink.Config   `yaml:"mobilink"`
}

type Record struct {
	Msisdn           string
	Status           string
	OperatorCode     int64
	CountryCode      int64
	ServiceId        int64
	SubscriptionId   int64
	CampaignId       int64
	RetryId          int64
	CreatedAt        time.Time
	LastPayAttemptAt time.Time
	AttemptsCount    int
	KeepDays         int
	DelayHours       int
}

func process() {
	go func() {
		for range time.Tick(1 * time.Second) {
			processSubscriptions()
		}
	}()

	go func() {
		for range time.Tick(1 * time.Second) {
			processRetries()
		}
	}()
}

func processRetries() {
	retries, err := getRetryTransactions()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("get retries")
		return
	}
	for _, r := range retries {
		now := time.Now()
		remove := false
		makeAttempt := false
		touch := false
		if r.CreatedAt.Sub(now).Hours() > time.Duration(24*r.KeepDays)*time.Hour {
			remove = true
			makeAttempt = true
		}
		if r.LastPayAttemptAt.Sub(now).Hours() > time.Duration(r.DelayHours)*time.Hour {
			makeAttempt = true
		}

		if makeAttempt {
			success, err := handle(r)
			if success == true && err == nil {
				remove = true
			} else {
				touch = true
			}
		}
		if remove {
			removeRetry(r)
		}
		if touch {
			touchRetry(r)
		}
	}
}
func processSubscriptions() {
	records, err := getNotPaidSubscriptions()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("get subscriptions")
		return
	}
	for _, record := range records {
		success, err := handle(record)
		if err != nil {
			continue
		}
		if !success {
			mService, ok := memServices.Map[record.ServiceId]
			if !ok {
				log.Error("service not found")
				continue
			}
			record.DelayHours = mService.DelayHours
			record.KeepDays = mService.KeepDays

			startRetry(record)
		}
	}
	return nil
}

func handle(subscription Record) (bool, error) {
	logCtx := log.WithFields(log.Fields{"subscription": subscription})

	//CREATE TYPE transaction_statuses AS ENUM ('', 'failed', 'paid', 'blacklisted', 'recurly', 'rejected', 'past');

	mService, ok := memServices.Map[subscription.ServiceId]
	if !ok {
		logCtx.Error("service not found")
		return false, fmt.Errorf("Service id %d not found", subscription.ServiceId)
	}
	if mService.Price <= 0 {
		logCtx.WithField("price", mService.Price).Error("price is not set")
		return false, fmt.Errorf("Service price %d is zero or less", mService.Price)
	}
	if mService.PaidHours > 0 {
		// if msisdn already was subscribed on this subscription in paid hours time
		// give them content, and skip tariffication
		if false {
			logCtx.Debug("paid hours aren't passed")
			subscription.Status = "rejected"
			writeSubscriptionStatus(subscription)
			return true, nil
		}
	}

	var operatorToken string
	var err error
	switch {
	case mobilink.Belongs(subscription.Msisdn):
		operatorToken, err = svc.mobilink.MT(subscription.Msisdn, mService.Price)
	default:
		logCtx.Debug("Not applicable to any operator")
		return false, fmt.Errorf("Msisdn %s is not applicable to any operator", subscription.Msisdn)
	}
	if err != nil {
		logCtx.WithFields(log.Fields{
			"error": err.Error(),
		}).Info("tariffication has failed")
	}
	if len(operatorToken) > 0 && err == nil {
		subscription.Status = "paid"
	} else {
		subscription.Status = "failed"
	}
	writeSubscriptionStatus(subscription)
	writeTransaction(subscription)
	return subscription.Status == "paid", nil
}

func writeTransaction(t Record) error {
	query := fmt.Sprintf("INSERT INTO %stransaction ("+
		"msisdn, "+
		"status, "+
		"operator_code, "+
		"country_code, "+
		"id_service, "+
		"id_subscription, "+
		"id_campaign"+
		") VALUES($1, $2, $3, $4, $5, $6, $7)", svc.sConfig.DbConf.TablePrefix)

	res, err := svc.db.Exec(
		query,
		t.Msisdn,
		t.Status,
		t.OperatorCode,
		t.CountryCode,
		t.ServiceId,
		t.SubscriptionId,
		t.CampaignId,
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
		"query":        query,
		"rowsAffected": res.LastInsertId(),
		"transaction":  t}).
		Info("record transaction done")
	return nil
}

func writeSubscriptionStatus(subscription Record) error {
	query := fmt.Sprintf("update %ssubscriptions set paid = $1 where id = $2", svc.sConfig.DbConf.TablePrefix)
	res, err := svc.db.Exec(query, subscription.Status, subscription.SubscriptionId)
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
		"rowsAffected": res.RowsAffected(),
		"subscription": subscription,
	}).Info("notify paid subscription done")
	return nil
}

func getNotPaidSubscriptions() ([]Record, error) {
	var subscr []Record
	query := fmt.Sprintf("SELECT id, msisdn, id_service, id_campaign, operator_code, country_code"+
		" from %ssubscriptions where paid = ''", svc.sConfig.DbConf.TablePrefix)
	rows, err := svc.db.Query(query)
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

func getRetryTransactions() ([]Record, error) {
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
		" from %sretries", svc.sConfig.DbConf.TablePrefix)
	rows, err := svc.db.Query(query)
	if err != nil {
		return retries, fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var lastPayAttemptAt, createdAt *time.Time
	for rows.Next() {
		record := Record{}
		if err := rows.Scan(
			&record.RetryId,
			&createdAt,
			&lastPayAttemptAt,
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
		record.LastPayAttemptAt = lastPayAttemptAt.UTC().Unix()
		record.CreatedAt = createdAt.UTC().Unix()
		retries = append(retries, record)
	}
	if rows.Err() != nil {
		return retries, fmt.Errorf("GetRetries RowsError: %s", err.Error())
	}
	return retries, nil
}

func removeRetry(r Record) error {
	query := fmt.Sprintf("DELETE FROM %sretries WHERE id = $1", svc.sConfig.DbConf.TablePrefix)

	res, err := svc.db.Exec(query, r.RetryId)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"retry":  r}).
			Error("delete retry failed")
		return fmt.Errorf("QueryExec: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"query":        query,
		"rowsAffected": res.LastInsertId(),
		"retry":        r}).
		Info("retry deleted")
	return nil
}

type Record struct {
	Msisdn           string
	Status           string
	OperatorCode     int64
	CountryCode      int64
	ServiceId        int64
	SubscriptionId   int64
	CampaignId       int64
	RetryId          int64
	CreatedAt        time.Time
	LastPayAttemptAt time.Time
	AttemptsCount    int
	KeepDays         int
	DelayHours       int
}

func touchRetry(r Record) error {
	query := fmt.Sprintf("UPDATE %sretries SET "+
		"last_pay_attempt_at = $1 "+
		"attempts_count = attempts_count + 1 "+
		"WHERE id = $2", svc.sConfig.DbConf.TablePrefix)

	lastPayAttemptAt := time.Now().UTC().Unix()

	res, err := svc.db.Exec(query, lastPayAttemptAt, r.RetryId)
	if err != nil {
		log.WithFields(log.Fields{
			"error ": err.Error(),
			"query":  query,
			"retry":  r}).
			Error("update retry failed")
		return fmt.Errorf("QueryExec: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"query":        query,
		"rowsAffected": res.LastInsertId(),
		"retry":        r}).
		Info("retry touch")
	return nil
}

func startRetry(r Record) error {
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
		"id_campaign ) VALUES ( $1, $2, $3, $4, $5, $6, $7)",
		svc.sConfig.DbConf.TablePrefix)
	_, err := svc.db.Exec(query,
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
