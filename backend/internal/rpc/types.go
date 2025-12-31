package rpc

type RegisterWorkerArgs struct {
	Addr string // worker address (for display / future use)
}

type RegisterWorkerReply struct {
	WorkerID string
}

type HeartbeatArgs struct {
	WorkerID string
}

type HeartbeatReply struct {
	OK bool
}
