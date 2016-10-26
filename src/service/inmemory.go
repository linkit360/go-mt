package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/vostrok/db"
)

const ACTIVE_STATUS = 1

var dbConn *sql.DB
var conf db.DataBaseConfig

func initInMem(dbConf db.DataBaseConfig) error {
	dbConn = db.Init(dbConf)
	conf = dbConf

	if err := memServices.Reload(); err != nil {
		return fmt.Errorf("memServices.Reload: %s", err.Error())
	}
	if err := memBlackListed.Reload(); err != nil {
		return fmt.Errorf("memBlackListed.Reload: %s", err.Error())
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
	query := fmt.Sprintf("select "+
		"id, "+
		"price, "+
		"paid_hours, "+
		"pull_retry_delay, "+
		"retry_days, "+
		"sms_send, "+
		"wording "+
		"from %sservices where status = $1",
		svc.sConfig.DbConf.TablePrefix)
	rows, err := dbConn.Query(query, ACTIVE_STATUS)
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
			&srv.DelayHours,
			&srv.KeepDays,
			&srv.SMSSend,
			&srv.SMSNotPaidText,
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

// Tasks:
// Keep in memory all active blacklisted msisdn-s
// Reload when changes to service are done
var memBlackListed = &BlackList{}

type BlackList struct {
	sync.RWMutex
	Map map[string]struct{}
}

func (bl *BlackList) Reload() error {
	query := fmt.Sprintf("select msisdn from %smsisdn_blacklist", svc.sConfig.DbConf.TablePrefix)
	rows, err := dbConn.Query(query)
	if err != nil {
		return fmt.Errorf("BlackList QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var blackList []string
	for rows.Next() {
		var msisdn string
		if err := rows.Scan(&msisdn); err != nil {
			return err
		}
		blackList = append(blackList, msisdn)
	}
	if rows.Err() != nil {
		return fmt.Errorf("RowsError: %s", err.Error())
	}

	bl.Lock()
	defer bl.Unlock()

	bl.Map = make(map[string]struct{}, len(blackList))
	for _, msisdn := range blackList {
		bl.Map[msisdn] = struct{}{}
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
	query := fmt.Sprintf("select name, rps, settings from %soperators", svc.sConfig.DbConf.TablePrefix)
	rows, err := dbConn.Query(query)
	if err != nil {
		return fmt.Errorf("Operators QueryServices: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	var operators []Operator
	for rows.Next() {
		var op Operator
		if err := rows.Scan(
			&op.Name,
			&op.Rps,
			&op.Settings,
		); err != nil {
			return err
		}
		operators = append(operators, op)
	}
	if rows.Err() != nil {
		return fmt.Errorf("RowsError: %s", err.Error())
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
	rg.GET("/", Reload)
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
