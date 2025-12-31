package node

import "time"

type Status string

const (
	Alive Status = "ALIVE"
	Dead  Status = "DEAD"
)

type Worker struct {
	ID            string
	Addr          string
	Status        Status
	LastHeartbeat time.Time
}
