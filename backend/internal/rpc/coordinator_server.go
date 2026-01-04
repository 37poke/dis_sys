package rpc

import (
	"fmt"
	"dis_sys/backend/internal/task"
	"dis_sys/backend/internal/node"
)

type CoordinatorRPC struct {
	Nodes *node.Manager
	Tasks *task.Manager
}

// RegisterWorker RPC
func (c *CoordinatorRPC) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	if args == nil || args.Addr == "" {
		return fmt.Errorf("missing worker addr")
	}
	id := c.Nodes.Register(args.Addr)
	reply.WorkerID = id
	return nil
}

// Heartbeat RPC
func (c *CoordinatorRPC) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	if args == nil || args.WorkerID == "" {
		return fmt.Errorf("missing worker id")
	}
	if err := c.Nodes.Heartbeat(args.WorkerID); err != nil {
		return err
	}
	reply.OK = true
	return nil
}

// RequestTask: Worker 拉任务
func (c *CoordinatorRPC) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if args == nil || args.WorkerID == "" {
		return fmt.Errorf("missing worker id")
	}
	if c.Tasks == nil {
		return fmt.Errorf("task manager not initialized")
	}

	t, ok := c.Tasks.AssignNext(args.WorkerID)
	if !ok {
		reply.HasTask = false
		return nil
	}

	reply.HasTask = true
	reply.TaskID = t.ID
	reply.Payload = t.Payload
	reply.State = string(t.State)
	reply.WorkerID = t.WorkerID
	return nil
}

func (c *CoordinatorRPC) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
    if args == nil || args.WorkerID == "" || args.TaskID == "" {
        return fmt.Errorf("missing args")
    }
    if c.Tasks == nil {
        return fmt.Errorf("task manager not initialized")
    }
    if err := c.Tasks.MarkFinished(args.TaskID, args.WorkerID); err != nil {
        return err
    }
    reply.OK = true
    return nil
}


