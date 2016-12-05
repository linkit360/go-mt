package service

import (
	"fmt"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	rec "github.com/vostrok/utils/rec"
)

// does simple thing:
// selects all retries form database with result = 'pending' and before hours last_attempt_at
// and checks in transaction log
// deafult behaviour - dry run
func AddRetriesHandler(r *gin.Engine) {
	rg := r.Group("/retries")
	rg.GET("", retries)
	log.WithFields(log.Fields{"handler": "retries"}).Debug("added")
}

type Params struct {
	Limit        int
	Hours        int
	OperatorCode int64
}

func retries(c *gin.Context) {
	limitStr, _ := c.GetQuery("limit")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limitStr == "" {
		log.WithFields(log.Fields{}).Debug("no required param limit, so use 1000")
		limit = 1000
	}

	hoursStr, _ := c.GetQuery("hours")
	hours, err := strconv.Atoi(hoursStr)
	if err != nil || hoursStr == "" {
		log.WithFields(log.Fields{}).Debug("no required param, so use 1 hour")
		hours = 1
	}

	operatorCode := int64(41001)
	code, _ := c.GetQuery("code")
	if code == "" {
		log.WithFields(log.Fields{"code": operatorCode}).Debug("no code param, use default")
	} else {
		operatorCode, err = strconv.ParseInt(code, 10, 64)
		if err != nil {
			log.WithFields(log.Fields{"error": err.Error()}).Error("cannot parse code")
			c.JSON(500, err.Error())
			return
		}
	}

	params := Params{
		Limit:        limit,
		Hours:        hours,
		OperatorCode: operatorCode,
	}

	count, err := processPendingRetries(params)
	if err != nil {
		c.JSON(500, err.Error())
		return
	}
	c.JSON(200, count)
}

func processPendingRetries(p Params) (count int, err error) {
	records, err := rec.LoadPendingRetries(p.Hours, p.OperatorCode, p.Limit)
	if err != nil {
		err = fmt.Errorf("rec.LoadPendingRetries: %s", err.Error())
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("cannot get pending retries")
		return
	}
	count = 0
	for _, v := range records {
		count++
		result, err := v.GetSubscriptionResult()
		if err != nil {
			err = fmt.Errorf("rec.GetPaidTransactionId: %s", err.Error())
			log.WithFields(log.Fields{
				"error": err.Error(),
				"tid":   v.Tid,
			}).Error("cannot get subscription result")
			return 0, err
		}
		if result == "unknown" {
			continue
		}
		if result == "failed" {
			continue
		}
		if result == "paid" ||
			result == "postpaid" ||
			result == "blacklisted" {

			log.WithFields(log.Fields{
				"result": result,
				"tid":    v.Tid,
			}).Debug("remove retry: " + result)
			v.RemoveRetry()
		}
	}
	return count, nil
}
