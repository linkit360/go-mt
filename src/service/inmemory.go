package service

import (
	"fmt"
	"sync"
)

const ACTIVE_STATUS = 1

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
	Id         int64
	Price      float64
	PaidHours  int
	KeepDays   int
	DelayHours int
}

func (s Services) Reload() error {
	query := fmt.Sprintf("select id, price, paid_hours, pull_retry_delay, retry_days from %sservices where status = $1",
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
			&srv.DelayHours,
			&srv.KeepDays,
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
