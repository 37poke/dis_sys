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

	Attempts int // 已尝试次数（累计）；每次被分配时 +1
	MaxRetry  int           // 最大重试次数（默认 3）
	LeaseUntil time.Time    // 租约到期时间（超时回收）
	LastError string
	AssignedAt time.Time
	LeaseToken string  // 本次分配的令牌，用于防止旧worker乱报
	CreatedAt time.Time
	UpdatedAt time.Time
}