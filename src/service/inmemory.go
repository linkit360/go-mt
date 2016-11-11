package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/db"
)

const ACTIVE_STATUS = 1

var dbConn *sql.DB

func init() {
	log.SetLevel(log.DebugLevel)
}
func initInMem(dbConfig db.DataBaseConfig) error {
	dbConn = db.Init(dbConfig)

	if err := memServices.Reload(); err != nil {
		return fmt.Errorf("memServices.Reload: %s", err.Error())
	}
	if err := memBlackListed.Reload(); err != nil {
		return fmt.Errorf("memBlackListed.Reload: %s", err.Error())
	}
	if err := memPostPaid.Reload(); err != nil {
		return fmt.Errorf("memPostPaid.Reload: %s", err.Error())
	}
	if err := memOperators.Reload(); err != nil {
		return fmt.Errorf("memOperators.Reload: %s", err.Error())
	}
	return nil
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
	log.WithFields(log.Fields{}).Debug("services reload...")
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
		"retry_days, "+
		"sms_send, "+
		"wording "+
		"from %sservices where status = $1",
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
	log.WithFields(log.Fields{}).Debug("blacklist reload...")
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

	query := fmt.Sprintf("select msisdn from %smsisdn_blacklist", svc.dbConf.TablePrefix)
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
	log.WithFields(log.Fields{}).Debug("post paid reload...")
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
// Keep in memory all active blacklisted msisdn-s
// Reload when changes to service are done
var memOperators = &Operators{}

type Operators struct {
	sync.RWMutex
	Map map[string]Operator
}

type Operator struct {
	Name     string
	Rps      int
	Settings string
}

func (ops *Operators) Reload() error {
	var err error
	log.WithFields(log.Fields{}).Debug("operators reload...")
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

	ops.Lock()
	defer ops.Unlock()

	ops.Map = make(map[string]Operator, len(operators))
	for _, op := range operators {
		ops.Map[op.Name] = op
	}
	return nil
}

type response struct {
	Success bool        `json:"success,omitempty"`
	Err     error       `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Status  int         `json:"-"`
}

func AddCQRHandlers(r *gin.Engine) {
	rg := r.Group("/cqr")
	rg.GET("", Reload)
}

func Reload(c *gin.Context) {
	var err error
	r := response{Err: err, Status: http.StatusOK}

	table, exists := c.GetQuery("table")
	if !exists || table == "" {
		table, exists = c.GetQuery("t")
		if !exists || table == "" {
			err := errors.New("Table name required")
			r.Status = http.StatusBadRequest
			r.Err = err
			render(r, c)
			return
		}
	}

	switch {
	case strings.Contains(table, "blacklist"):
		err = memBlackListed.Reload()
		if err != nil {
			r.Status = http.StatusInternalServerError
		} else {
			r.Success = true
		}
	case strings.Contains(table, "postpaid"):
		err = memPostPaid.Reload()
		if err != nil {
			r.Status = http.StatusInternalServerError
		} else {
			r.Success = true
		}
	case strings.Contains(table, "services"):
		err = memServices.Reload()
		if err != nil {
			r.Status = http.StatusInternalServerError
		} else {
			r.Success = true
		}
	case strings.Contains(table, "operators"):
		err = memOperators.Reload()
		if err != nil {
			r.Status = http.StatusInternalServerError
		} else {
			r.Success = true
		}
	default:
		err = fmt.Errorf("Table name %s not recognized", table)
		r.Status = http.StatusBadRequest
	}
	render(r, c)
	return
}

func render(msg response, c *gin.Context) {
	if msg.Err != nil {
		c.Header("Error", msg.Err.Error())
		c.Error(msg.Err)
	}
	c.JSON(msg.Status, msg)
}
