package service

import (
	"database/sql"
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/db"
	"github.com/vostrok/mt/src/service/mobilink"
)

var svc MTService

const ACTIVE_STATUS = 1

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

type Operator interface {
	MT(msisdn string, price int) (string, error)
}

func process() {
	processSubscriptions()

	// in transaction
	// get from retry database all records
	// mt them
	// record

}

func processSubscriptions() error {
	records, err := getNotPaidSubscriptions()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("get subscriptions")
		return fmt.Errorf("Get Subscriptions: %s", err.Error())
	}
	handle(records)
	return nil
}

func handle(records []Record) {
	for _, record := range records {

		//CREATE TYPE transaction_statuses AS ENUM ('', 'failed', 'paid', 'blacklisted', 'recurly', 'rejected', 'past');
		t := Record{
			Msisdn:         record.Msisdn,
			Status:         "failed",
			OperatorCode:   record.OperatorCode,
			CountryCode:    record.CountryCode,
			ServiceId:      record.ServiceId,
			SubscriptionId: record.Id,
			CampaignId:     record.CampaignId,
		}

		mService, ok := memServices.Map[record.ServiceId]
		if !ok {
			log.WithFields(log.Fields{
				"serviceId":    record.ServiceId,
				"subscription": record,
			}).Error("service not found")
			continue
		}
		if mService.Price <= 0 {
			log.WithFields(log.Fields{
				"serviceId":    record.ServiceId,
				"subscription": record,
			}).Error("price is not set")
			continue
		}
		if mService.PaidHours > 0 {
			// see previous bills for this phone
			// do not send tarifficate request if he was already billed
			// on the same service before paidHours passed
			if false {
				t.Status = "rejected"
				writeTransaction(t)
				continue
			}
		}

		var operatorToken string
		var err error
		switch {
		case mobilink.Belongs(record.Msisdn):
			operatorToken, err = svc.mobilink.MT(record.Msisdn, mService.Price)
		default:
			log.WithFields(log.Fields{
				"msisdn": record.Msisdn,
			}).Debug("Not applicable to any operator")
			continue
		}

		if err != nil {
			log.WithFields(log.Fields{
				"subscription": record,
				"error":        err.Error(),
			}).Info("tariffication has failed")
		}

		if len(operatorToken) > 0 {
			t.Status = "paid"
			writeSuccessSubscription(record.Id)
		}
		writeTransaction(t)
	}
}

type Record struct {
	Id             int64
	Msisdn         string
	Status         string
	OperatorCode   int64
	CountryCode    int64
	ServiceId      int64
	SubscriptionId int64
	CampaignId     int64
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

func writeSuccessSubscription(subscriptionId int64) error {
	query := fmt.Sprintf("update %ssubscriptions set paid = true where id = $1", svc.sConfig.DbConf.TablePrefix)
	res, err := svc.db.Exec(query, subscriptionId)
	if err != nil {
		log.WithFields(log.Fields{
			"error ":         err.Error(),
			"query":          query,
			"subscriptionId": subscriptionId}).
			Error("notify paid subscription failed")
		return fmt.Errorf("QueryExec: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"query":          query,
		"rowsAffected":   res.RowsAffected(),
		"subscriptionId": subscriptionId}).
		Info("notify paid subscription done")
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
			&record.Id,
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

// Tasks:
// Keep in memory all active service to content mapping
// Allow to get all content ids of given service id
// Reload when changes to service_content or service are done
var memServices = &Services{}

type Services struct {
	sync.RWMutex
	Map map[int64]Service
}
type Service struct {
	Id        int64
	Price     float64
	PaidHours int64
}

func (s Services) Reload() error {
	query := fmt.Sprintf("select id, price, paid_hours from %sservices where status = $1",
		svc.sConfig.DbConf.TablePrefix)
	rows, err := svc.db.Query(query, ACTIVE_STATUS)
	if err != nil {
		return fmt.Errorf("services QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var services []Service
	for rows.Next() {
		var srv Service
		if err := rows.Scan(
			&srv.Id,
			&srv.Price,
			&srv.PaidHours,
		); err != nil {
			return err
		}
		services = append(services, srv)
	}
	if rows.Err() != nil {
		return fmt.Errorf("RowsError: %s", err.Error())
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[int64]Service)
	for _, service := range services {
		s.Map[service.Id] = service
	}
	return nil
}
