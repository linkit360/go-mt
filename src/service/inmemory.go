package service

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/utils/cqr"
	"github.com/vostrok/utils/db"
)

var dbConn *sql.DB
var inMemConf = []cqr.CQRConfig{
	{
		Table:      "blacklist",
		ReloadFunc: memBlackListed.Reload,
	},
	{
		Table:      "postpaid",
		ReloadFunc: memPostPaid.Reload,
	},
	{
		Table:      "services",
		ReloadFunc: memServices.Reload,
	},
	{
		Table:      "operators",
		ReloadFunc: memOperators.Reload,
	},
}

func initInMem(dbConfig db.DataBaseConfig) error {
	dbConn = db.Init(dbConfig)
	return cqr.InitCQR(inMemConf)
}

func AddCQRHandlers(r *gin.Engine) {
	cqr.AddCQRHandler(reloadCQRFunc, r)
}

func reloadCQRFunc(c *gin.Context) {
	cqr.CQRReloadFunc(inMemConf, c)(c)
}

const ACTIVE_STATUS = 1

func init() {
	log.SetLevel(log.DebugLevel)
}

// Tasks:
// Keep in memory all active service to content mapping
// Allow to get all service of given service id
// Reload when changes to service are done
var memServices = &Services{}

type Services struct {
	sync.RWMutex
	Map map[int64]Service
}
type Service struct {
	Id             int64
	Price          float64
	PaidHours      int
	KeepDays       int
	DelayHours     int
	SMSSend        int
	SMSNotPaidText string
}

func (s *Services) Reload() error {
	var err error
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("services reload")
	}()
	query := fmt.Sprintf("select "+
		"id, "+
		"price, "+
		"paid_hours, "+
		"delay_hours, "+
		"keep_days, "+
		"sms_send, "+
		"wording "+
		"FROM %sservices "+
		"WHERE status = $1",
		svc.dbConf.TablePrefix)
	rows, err := dbConn.Query(query, ACTIVE_STATUS)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var services []Service
	for rows.Next() {
		var srv Service
		if err = rows.Scan(
			&srv.Id,
			&srv.Price,
			&srv.PaidHours,
			&srv.DelayHours,
			&srv.KeepDays,
			&srv.SMSSend,
			&srv.SMSNotPaidText,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		services = append(services, srv)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	s.Lock()
	defer s.Unlock()

	s.Map = make(map[int64]Service)
	for _, service := range services {
		s.Map[service.Id] = service
	}
	return nil
}

// Tasks:
// Keep in memory all active blacklisted msisdn-s
// Reload when changes to service are done
var memBlackListed = &BlackList{}

type BlackList struct {
	sync.RWMutex
	Map map[string]struct{}
}

func (bl *BlackList) Reload() error {
	var err error
	bl.Lock()
	defer bl.Unlock()
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("blacklist reload")
	}()

	query := fmt.Sprintf("SELECT "+
		"msisdn "+
		"FROM %smsisdn_blacklist",
		svc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = dbConn.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var blackList []string
	for rows.Next() {
		var msisdn string
		if err = rows.Scan(&msisdn); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		blackList = append(blackList, msisdn)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	bl.Map = make(map[string]struct{}, len(blackList))
	for _, msisdn := range blackList {
		bl.Map[msisdn] = struct{}{}
	}
	return nil
}

// Tasks:
// Keep in memory all active postpaid msisdn-s
// Reload when changes to service are done
var memPostPaid = &PostPaidList{}

type PostPaidList struct {
	sync.RWMutex
	Map map[string]struct{}
}

func (pp *PostPaidList) Reload() error {
	var err error
	pp.Lock()
	defer pp.Unlock()
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("post paid reload")
	}()

	query := fmt.Sprintf("select msisdn from %smsisdn_postpaid", svc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = dbConn.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var postPaidList []string
	for rows.Next() {
		var msisdn string
		if err = rows.Scan(&msisdn); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		postPaidList = append(postPaidList, msisdn)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	pp.Map = make(map[string]struct{}, len(postPaidList))
	for _, msisdn := range postPaidList {
		pp.Map[msisdn] = struct{}{}
	}
	return nil
}

// Tasks:
// Keep in memory all operators names and configuration
// Reload when changes to operators table are done
var memOperators = &Operators{}

type Operators struct {
	sync.RWMutex
	ByName map[string]Operator
	ByCode map[int64]Operator
}

type Operator struct {
	Name     string
	Rps      int
	Settings string
	Code     int64
}

func (ops *Operators) Reload() error {
	ops.Lock()
	defer ops.Unlock()
	var err error
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("operators reload")
	}()

	query := fmt.Sprintf("SELECT "+
		"name, "+
		"code, "+
		"rps, "+
		"settings "+
		"FROM %soperators",
		svc.dbConf.TablePrefix)
	var rows *sql.Rows
	rows, err = dbConn.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return err
	}
	defer rows.Close()

	var operators []Operator
	for rows.Next() {
		var op Operator
		if err = rows.Scan(
			&op.Name,
			&op.Code,
			&op.Rps,
			&op.Settings,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return err
		}
		operators = append(operators, op)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return err
	}

	ops.ByName = make(map[string]Operator, len(operators))
	for _, op := range operators {
		ops.ByName[op.Name] = op
	}

	ops.ByCode = make(map[int64]Operator, len(operators))
	for _, op := range operators {
		ops.ByCode[op.Code] = op
	}
	return nil
}
