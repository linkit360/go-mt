package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	dbconn "github.com/vostrok/utils/db"
	"github.com/vostrok/utils/rec"
)

// get retries from json file and put them into database
type conf struct {
	Db dbconn.DataBaseConfig
}

func main() {
	cfg := flag.String("config", "mt_manager.yml", "configuration yml file")
	flag.Parse()

	var appConfig conf

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}

	rec.Init(appConfig.Db)

	file, err := os.Open("retries")
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		i++
		text := scanner.Text()

		var record rec.Record
		if err := json.Unmarshal([]byte(text), &record); err != nil {
			fmt.Println(text)
			fmt.Println(err.Error())
			continue
		}
		if err := record.StartRetry(); err != nil {
			fmt.Println(text)
			fmt.Println(err.Error())
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	log.Info("done")
	log.Info(i)

}
