// can make MT requests (request to tarifficate a msisdn via Mobilink API
// it is assumed we could edit settings from admin interface in the future,
// so there is a setting field in operators table (it is not used now)
//
// Mobilink provides also SMS send interface which could be used outside the module
// unique token used to check statistics with Mobilink using transactions table
package mobilink

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	smpp_client "github.com/fiorix/go-smpp/smpp"
	"github.com/fiorix/go-smpp/smpp/pdu/pdutext"
	"github.com/gin-gonic/gin"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"

	rec "github.com/vostrok/mt/src/service/instance"
	"os"
)

type Mobilink struct {
	conf        Config
	rps         int
	mtChannel   chan rec.Record
	Response    chan rec.Record
	M           Metrics
	location    *time.Location
	client      *http.Client
	smpp        *smpp_client.Transmitter
	responseLog *log.Logger
	requestLog  *log.Logger
}
type Config struct {
	Enabled        bool                 `default:"true" yaml:"enabled"`
	MTChanCapacity int                  `default:"1000" yaml:"channel_camacity"`
	Connection     ConnnectionConfig    `yaml:"connection"`
	Location       string               `default:"Asia/Karachi" yaml:"location"`
	TransactionLog TransactionLogConfig `yaml:"log_transaction"`
}
type TransactionLogConfig struct {
	ResponseLogPath string `default:"/var/log/response_mobilink.log" yaml:"response"`
	RequestLogPath  string `default:"/var/log/request_mobilink.log" yaml:"request"`
}

type ConnnectionConfig struct {
	MT   MTConfig   `yaml:"mt" json:"mt"`
	Smpp SmppConfig `yaml:"smpp" json:"smpp"`
}
type SmppConfig struct {
	ShortNumber string `default:"4162" yaml:"short_number" json:"short_number"`
	Addr        string `default:"182.16.255.46:15019" yaml:"endpoint"`
	User        string `default:"SLYEPPLA" yaml:"user"`
	Password    string `default:"SLYPEE_1" yaml:"pass"`
	Timeout     int    `default:"20" yaml:"timeout"`
}
type MTConfig struct {
	Url                  string            `default:"http://182.16.255.46:10020/Air" yaml:"url" json:"url"`
	Headers              map[string]string `yaml:"headers" json:"headers"`
	TimeoutSec           int               `default:"20" yaml:"timeout" json:"timeout"`
	TarifficateBody      string            `yaml:"mt_body"`
	PaidBodyContains     []string          `yaml:"paid_body_contains" json:"paid_body_contains"`
	CheckBalanceBody     string            `yaml:"check_balance_body"`
	PostPaidBodyContains []string          `yaml:"postpaid_body_contains" json:"postpaid_body_contains"`
}

type Metrics struct {
	SMPPConnected   metrics.Gauge
	ResponseLen     metrics.Gauge
	PendingRequests metrics.Gauge
	//ErrorCount           metrics.Gauge
	//ErrorRate            metrics.Gauge
	BalanceCheckDuration metrics.TimeHistogram
	TarifficateDuration  metrics.TimeHistogram
}

func initMetrics() Metrics {
	var quantiles = []int{50, 90, 95, 99}
	return Metrics{
		SMPPConnected:   expvar.NewGauge("mobilink_smpp_connected"),
		ResponseLen:     expvar.NewGauge("mobilink_responses_queue"),
		PendingRequests: expvar.NewGauge("mobilink_request_queue"),
		//ErrorRate:       expvar.NewGauge("mobilink_error_rate"),
		//ErrorCount:      expvar.NewGauge("mobilink_error_count"),
		BalanceCheckDuration: metrics.NewTimeHistogram(time.Millisecond,
			expvar.NewHistogram("duration_ms_charge", 0, 10000, 3, quantiles...)),
		TarifficateDuration: metrics.NewTimeHistogram(time.Millisecond,
			expvar.NewHistogram("duration_ms_tarifficate", 0, 10000, 3, quantiles...)),
	}
}

// todo: chan gap cannot be too big bzs of the size

func Init(mobilinkRps int, mobilinkConf Config) *Mobilink {
	mb := &Mobilink{
		rps:  mobilinkRps,
		conf: mobilinkConf,
		M:    initMetrics(),
	}
	log.Info("mb metrics init done")

	mb.requestLog = getLogger(mobilinkConf.TransactionLog.RequestLogPath)
	log.Info("request logger init done")

	mb.responseLog = getLogger(mobilinkConf.TransactionLog.ResponseLogPath)
	log.Info("response logger init done")

	mb.mtChannel = make(chan rec.Record, mobilinkConf.MTChanCapacity)
	mb.Response = make(chan rec.Record, mobilinkConf.MTChanCapacity)
	log.WithField("capacity", mobilinkConf.MTChanCapacity).Info("channels ini done")

	var err error
	mb.location, err = time.LoadLocation(mobilinkConf.Location)
	if err != nil {
		log.WithFields(log.Fields{
			"location": mobilinkConf.Location,
			"error":    err,
		}).Fatal("init location")
	}
	log.Info("location init done")

	mb.client = &http.Client{
		Timeout: time.Duration(mobilinkConf.Connection.MT.TimeoutSec) * time.Second,
	}
	log.Info("http client init done")

	mb.smpp = &smpp_client.Transmitter{
		Addr:        mobilinkConf.Connection.Smpp.Addr,
		User:        mobilinkConf.Connection.Smpp.User,
		Passwd:      mobilinkConf.Connection.Smpp.Password,
		RespTimeout: time.Duration(mobilinkConf.Connection.Smpp.Timeout) * time.Second,
		SystemType:  "SMPP",
	}
	log.Info("smpp client init done")

	connStatus := mb.smpp.Bind()
	go func() {
		for c := range connStatus {
			if c.Status().String() != "Connected" {
				mb.M.SMPPConnected.Set(0)
				log.WithFields(log.Fields{
					"operator": "mobilink",
					"status":   c.Status().String(),
					"error":    "disconnected:" + c.Status().String(),
				}).Error("smpp moblink connect status")
			} else {
				log.WithFields(log.Fields{
					"operator": "mobilink",
					"status":   c.Status().String(),
				}).Info("smpp moblink connect status")
				mb.M.SMPPConnected.Set(1)
			}
		}
	}()

	go func() {
		mb.mtReader()
	}()

	return mb
}
func getLogger(path string) *log.Logger {
	handler, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		log.WithFields(log.Fields{
			"path":  path,
			"error": err.Error(),
		}).Fatal("cannot open file")
	}
	return &log.Logger{
		Out:       handler,
		Formatter: new(log.TextFormatter),
		Hooks:     make(log.LevelHooks),
		Level:     log.DebugLevel,
	}
}

// prefix from table
func Belongs(msisdn string) bool {
	return msisdn[:2] == "92"
}

func (mb *Mobilink) Publish(r rec.Record) {
	log.WithField("rec", r).Debug("publish")
	mb.mtChannel <- r
}

func (mb *Mobilink) GetMTChanGap() int {
	return len(mb.mtChannel)
}

// rate limit our Service.Method RPCs
func (mb *Mobilink) mtReader() {
	log.WithFields(log.Fields{}).Info("runninng read from channel")
	for {
		throttle := time.Tick(time.Millisecond * 200)
		for record := range mb.mtChannel {
			log.WithFields(log.Fields{
				"rec":      record,
				"throttle": throttle,
			}).Info("mobilink accept from channel")
			<-throttle
			var err error

			record.OperatorToken, err = mb.mt(record.Tid, record.Msisdn, record.Price)
			if err != nil {
				record.OperatorErr = err.Error()
			}
			mb.Response <- record
		}
	}
}

func (mb *Mobilink) mt(tid, msisdn string, price int) (string, error) {
	if !Belongs(msisdn) {
		log.WithFields(log.Fields{
			"msisdn": msisdn,
			"tid":    tid,
		}).Debug("is not mobilink")
		return "", nil
	}

	token := msisdn + time.Now().Format("20060102150405")[6:]
	now := time.Now().In(mb.location).Format("20060102T15:04:05-0700")

	log.WithFields(log.Fields{
		"token":  token,
		"tid":    tid,
		"msisdn": msisdn,
		"time":   now,
	}).Debug("prepare to send to mobilink")

	requestBody := mb.conf.Connection.MT.TarifficateBody
	requestBody = strings.Replace(requestBody, "%price%", "-"+strconv.Itoa(price), 1)
	requestBody = strings.Replace(requestBody, "%msisdn%", msisdn[2:], 1)
	requestBody = strings.Replace(requestBody, "%token%", token, 1)
	requestBody = strings.Replace(requestBody, "%time%", now, 1)

	req, err := http.NewRequest("POST", mb.conf.Connection.MT.Url, strings.NewReader(requestBody))
	if err != nil {
		log.WithFields(log.Fields{
			"token":  token,
			"tid":    tid,
			"msisdn": msisdn,
			"time":   now,
			"error":  err.Error(),
		}).Error("create POST req to mobilink")
		err = fmt.Errorf("http.NewRequest: %s", err.Error())
		return "", err
	}
	for k, v := range mb.conf.Connection.MT.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(requestBody)))
	req.Close = false

	// transaction log for internal logging
	var mobilinkResponse []byte
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"token":           token,
			"tid":             tid,
			"msisdn":          msisdn,
			"endpoint":        mb.conf.Connection.MT.Url,
			"headers":         fmt.Sprintf("%#v", req.Header),
			"reqeustBody":     requestBody,
			"requestResponse": string(mobilinkResponse),
			"took":            time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
			//mb.M.ErrorCount.Add(1)
		}
		log.WithFields(fields).Info("mobilink")
		mb.M.TarifficateDuration.Observe(time.Since(begin))
	}()

	// separate transaction for mobilink
	// 1 - request body
	mb.requestLog.WithFields(log.Fields{
		"token":       token,
		"tid":         tid,
		"msisdn":      msisdn,
		"endpoint":    mb.conf.Connection.MT.Url,
		"headers":     fmt.Sprintf("%#v", req.Header),
		"reqeustBody": strings.TrimSpace(requestBody),
	}).Info("mobilink request")
	defer func() {
		// separate transaction for mobilink
		// 2 - response body
		fields := log.Fields{
			"token":           token,
			"tid":             tid,
			"msisdn":          msisdn,
			"requestResponse": strings.TrimSpace(string(mobilinkResponse)),
			"took":            time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		mb.responseLog.WithFields(fields).Println("mobilink response")
	}()

	resp, err := mb.client.Do(req)
	if err != nil {
		log.WithFields(log.Fields{
			"error":  err,
			"token":  token,
			"tid":    tid,
			"msisdn": msisdn,
			"time":   now,
		}).Error("do request to mobilink")
		err = fmt.Errorf("client.Do: %s", err.Error())
		return "", err
	}

	mobilinkResponse, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithFields(log.Fields{
			"token":  token,
			"tid":    tid,
			"msisdn": msisdn,
			"time":   now,
			"error":  err,
		}).Error("get raw body of mobilink response")
		err = fmt.Errorf("ioutil.ReadAll: %s", err.Error())
		return "", err
	}
	defer resp.Body.Close()

	for _, v := range mb.conf.Connection.MT.PaidBodyContains {
		if strings.Contains(string(mobilinkResponse), v) {
			log.WithFields(log.Fields{
				"msisdn": msisdn,
				"token":  token,
				"tid":    tid,
				"price":  price,
			}).Info("charged")
			return token, nil
		}
	}
	log.WithFields(log.Fields{
		"msisdn": msisdn,
		"tid":    tid,
		"price":  price,
	}).Info("charge has failed")
	return token, errors.New("Charge has failed")
}

func (mb *Mobilink) SMS(tid, msisdn, msg string) error {
	shortMsg, err := mb.smpp.Submit(&smpp_client.ShortMessage{
		Src:      mb.conf.Connection.Smpp.ShortNumber,
		Dst:      "00" + msisdn[2:],
		Text:     pdutext.Raw(msg),
		Register: smpp_client.NoDeliveryReceipt,
	})

	if err == smpp_client.ErrNotConnected {
		log.WithFields(log.Fields{
			"msisdn": msisdn,
			"msg":    msg,
			"tid":    tid,
			"error":  err.Error(),
		}).Error("counldn't sed sms: service unavialable")
		return fmt.Errorf("smpp.Submit: %s", err.Error())
	}
	if err != nil {
		log.WithFields(log.Fields{
			"msisdn": msisdn,
			"msg":    msg,
			"tid":    tid,
			"error":  err.Error(),
		}).Error("counldn't sed sms: bad request")
		return fmt.Errorf("smpp.Submit: %s", err.Error())
	}

	log.WithFields(log.Fields{
		"msisdn": msisdn,
		"msg":    msg,
		"tid":    tid,
		"respid": shortMsg.RespID(),
	}).Error("sms sent")
	return nil
}
func getToken(msisdn string) string {
	return msisdn + time.Now().Format("20060102150405")[6:]
}

func (mb *Mobilink) BalanceCheck(tid, msisdn string) (bool, error) {
	if !Belongs(msisdn) {
		return false, nil
	}

	token := getToken(msisdn)
	now := time.Now().In(mb.location).Format("20060102T15:04:05-0700")
	requestBody := mb.conf.Connection.MT.CheckBalanceBody
	requestBody = strings.Replace(requestBody, "%msisdn%", msisdn[2:], 1)
	requestBody = strings.Replace(requestBody, "%token%", token, 1)
	requestBody = strings.Replace(requestBody, "%time%", now, 1)

	req, err := http.NewRequest("POST", mb.conf.Connection.MT.Url, strings.NewReader(requestBody))
	if err != nil {
		err = fmt.Errorf("http.NewRequest: %s", err.Error())
		return false, err
	}
	for k, v := range mb.conf.Connection.MT.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(requestBody)))
	req.Close = false

	var mobilinkResponse []byte
	postPaid := false
	begin := time.Now()
	defer func() {
		fields := log.Fields{
			"tid":             tid,
			"postPaid":        postPaid,
			"msisdn":          msisdn,
			"token":           token,
			"endpoint":        mb.conf.Connection.MT.Url,
			"headers":         fmt.Sprintf("%#v", req.Header),
			"reqeustBody":     requestBody,
			"requestResponse": string(mobilinkResponse),
			"took":            time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Info("mobilink check balance")
		mb.M.BalanceCheckDuration.Observe(time.Since(begin))
	}()

	resp, err := mb.client.Do(req)
	if err != nil {
		err = fmt.Errorf("client.Do: %s", err.Error())
		return false, err
	}

	mobilinkResponse, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("ioutil.ReadAll: %s", err.Error())
		return false, err
	}
	defer resp.Body.Close()

	for _, v := range mb.conf.Connection.MT.PostPaidBodyContains {
		if strings.Contains(string(mobilinkResponse), v) {
			postPaid = true
			return true, nil
		}
	}
	return false, nil
}

func MobilinkHandler(c *gin.Context) {
	c.Writer.WriteHeader(200)
	c.Writer.Write([]byte(`<value><i4>11</i4></value>`))
}
