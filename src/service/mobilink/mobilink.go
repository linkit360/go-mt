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
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
	"github.com/nu7hatch/gouuid"

	rec "github.com/vostrok/mt/src/service/instance"
)

type Mobilink struct {
	conf      Config
	rps       int
	mtChannel chan rec.Record
	Response  chan rec.Record
	metrics   Metrics
	location  time.Location
	client    *http.Client
	smpp      *smpp_client.Transmitter
}
type Config struct {
	Enabled     bool              `default:"true"`
	Connection  ConnnectionConfig `yaml:"connection"`
	Location    string            `default:"Asia/Karachi" yaml:"location"`
	PostXMLBody []byte            `yaml:"mt_body"`
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
	Url            string            `default:"http://182.16.255.46:10020/Air" yaml:"url" json:"url"`
	Headers        map[string]string `yaml:"headers" json:"headers"`
	TimeoutSec     int               `default:"20" yaml:"timeout" json:"timeout"`
	OkBodyContains []string          `default:"<value><i4>0</i4></value>" yaml:"ok_body_contains" json:"ok_body_contains"`
}

type Metrics struct {
	SMPPConnected metrics.Gauge
}

func initMetrics() Metrics {
	return Metrics{
		SMPPConnected: expvar.NewGauge("smpp_connected"),
	}
}

func Init(rps int, conf Config) Mobilink {
	var mobilink Mobilink
	mobilink.rps = rps
	mobilink.mtChannel = make(chan rec.Record)
	mobilink.Response = make(chan rec.Record)

	mobilink.conf = conf
	mobilink.metrics = initMetrics()

	var err error
	mobilink.location, err = time.LoadLocation(conf.Location)

	mobilink.client = &http.Client{
		Timeout: time.Duration(conf.Connection.MT.TimeoutSec * time.Second),
	}

	if err != nil {
		log.WithFields(log.Fields{
			"location": conf.Location,
			"error":    err,
		}).Fatal("init location")
	}
	mobilink.smpp = &smpp_client.Transmitter{
		Addr:        conf.Connection.Smpp.Addr,
		User:        conf.Connection.Smpp.User,
		Passwd:      conf.Connection.Smpp.Password,
		RespTimeout: time.Duration(conf.Connection.Smpp.Timeout) * time.Second,
		SystemType:  "SMPP",
	}

	connStatus := mobilink.smpp.Bind()
	go func() {

		for c := range connStatus {
			if c.Status().String() != "Connected" {
				mobilink.metrics.SMPPConnected.Set(0)
				log.WithFields(log.Fields{
					"operator": "mobilink",
					"status":   c.Status().String(),
					"error":    "disconnected:" + c.Status().String(),
				}).Error("smpp moblink connect status")
			} else {
				mobilink.metrics.SMPPConnected.Set(1)
			}
		}
	}()

	go func() {
		mobilink.readChan()
	}()

	return mobilink
}

func Belongs(msisdn string) bool {
	return msisdn[:2] == "92"
}

func getToken(msisdn string) string {
	u4, err := uuid.NewV4()
	if err != nil {
		log.WithField("error", err.Error()).Error("get uuid")
		return fmt.Sprintf("%d-%s", time.Now().Unix(), msisdn)
	}
	return fmt.Sprintf("%d-%s-%s", time.Now().Unix(), msisdn, u4)
}

func (mb Mobilink) Publish(r rec.Record) {
	mb.mtChannel <- r
}

// rate limit our Service.Method RPCs
func (mb Mobilink) readChan() {
	rate := time.Second / mb.rps
	throttle := time.Tick(rate)
	for record := range mb.mtChannel {
		<-throttle
		record.OperatorToken, record.OperatorErr = mb.mt(record.Msisdn, record.Price)
		mb.Response <- record
	}
}

func (mb Mobilink) mt(msisdn string, price int) (string, error) {

	if !Belongs(msisdn) {
		log.WithFields(log.Fields{
			"msisdn": msisdn,
		}).Debug("is not mobilink")
		return "", nil
	}
	token := getToken(msisdn)
	now := time.Now().In(mb.location).Format("20060102T15:04:05-0700")

	s := string(mb.conf.PostXMLBody)
	s = strings.Replace(s, "%price%", strconv.Itoa(price), 1)
	s = strings.Replace(s, "%msisdn%", msisdn, 1)
	s = strings.Replace(s, "%token%", token, 1)
	s = strings.Replace(s, "%time%", now, 1)

	req, err := http.NewRequest("POST", mb.conf.Connection.MT.Url, strings.NewReader(s))
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("create POST req to mobilink")
		err = fmt.Errorf("http.NewRequest: %s", err.Error())
		return
	}
	for k, v := range mb.conf.Connection.MT.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Length", strconv.Itoa(len(s)))
	req.Close = false

	resp, err := mb.client.Do(req)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("do request to mobilink")
		err = fmt.Errorf("client.Do: %s", err.Error())
		return
	}
	var html_data []byte
	html_data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("get raw body of mobilink response")
		err = fmt.Errorf("ioutil.ReadAll: %s", err.Error())
		return
	}
	resp.Body.Close()

	for _, v := range mb.conf.Connection.MT.OkBodyContains {
		if strings.Contains(string(html_data), v) {
			log.WithFields(log.Fields{
				"msisdn": msisdn,
				"token":  token,
				"price":  price,
			}).Info("charged")
			return token, nil
		}
	}
	log.WithField("body", html_data).Info("charge has failed")
	return "", errors.New("Charge has failed")
}

func (mb Mobilink) SMS(msisdn, msg string) error {
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
			"error":  err.Error(),
		}).Error("counldn't sed sms: service unavialable")
		return err
	}
	if err != nil {
		log.WithFields(log.Fields{
			"msisdn": msisdn,
			"msg":    msg,
			"error":  err.Error(),
		}).Error("counldn't sed sms: bad request")
		return
	}

	log.WithFields(log.Fields{
		"msisdn": msisdn,
		"msg":    msg,
		"respid": shortMsg.RespID(),
	}).Error("sms sent")
	return nil
}
