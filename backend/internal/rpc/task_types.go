package rpc

// Worker -> Coordinator: 拉取任务
type RequestTaskArgs struct {
	WorkerID string
}

type RequestTaskReply struct {
	HasTask bool

	TaskID   string
	Payload  string
	State    string
	WorkerID string
}
type ReportTaskDoneArgs struct {
    WorkerID string
    TaskID   string
}
type ReportTaskDoneReply struct {
    OK bool
}