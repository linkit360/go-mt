package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/utils/amqp"
	dbconn "github.com/vostrok/utils/db"
	m "github.com/vostrok/utils/metrics"
	"github.com/vostrok/utils/rec"
)

// get retries from json file and put them into database
type conf struct {
	Db             dbconn.DataBaseConfig `yaml:"db"`
	PuublisherConf amqp.NotifierConfig   `yaml:"publisher"`
}

var publisher *amqp.Notifier

var responseLog *string

func main() {
	process()
}

var paidCount = 0
