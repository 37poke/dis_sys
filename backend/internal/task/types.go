package task

import "time"

type State string

const (
	Pending  State = "PENDING"
	Assigned State = "ASSIGNED"
	Running  State = "RUNNING"
	Finished State = "FINISHED"
	Failed   State = "FAILED"
)

type Task struct {
	ID        string
	Payload   string
	State     State
	WorkerID  string
	CreatedAt time.Time
	UpdatedAt time.Time
}
