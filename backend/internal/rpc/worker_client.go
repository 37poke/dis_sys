package rpc

import (
	"net/rpc"
	"time"
	"fmt"
)

type Client struct {
	addr string
	c    *rpc.Client
}

func NewClient(coordAddr string) (*Client, error) {
	c, err := rpc.Dial("tcp", coordAddr)
	if err != nil {
		return nil, err
	}
	return &Client{addr: coordAddr, c: c}, nil
}

func (cl *Client) Close() error {
	if cl.c != nil {
		return cl.c.Close()
	}
	return nil
}

func (cl *Client) RegisterWorker(workerAddr string) (string, error) {
	args := &RegisterWorkerArgs{Addr: workerAddr}
	reply := &RegisterWorkerReply{}
	if err := cl.c.Call("CoordinatorRPC.RegisterWorker", args, reply); err != nil {
		return "", err
	}
	return reply.WorkerID, nil
}

func (cl *Client) Heartbeat(workerID string) error {
	args := &HeartbeatArgs{WorkerID: workerID}
	reply := &HeartbeatReply{}
	call := cl.c.Go("CoordinatorRPC.Heartbeat", args, reply, nil)

	select {
	case <-call.Done:
		return call.Error
	case <-time.After(2 * time.Second):
		return rpc.ErrShutdown // 简单超时处理；后续可自定义错误
	}
}

func (cl *Client) RequestTask(workerID string) (*RequestTaskReply, error) {
	args := &RequestTaskArgs{WorkerID: workerID}
	reply := &RequestTaskReply{}
	if err := cl.c.Call("CoordinatorRPC.RequestTask", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (cl *Client) ReportTaskDone(workerID, taskID string) error {
    args := &ReportTaskDoneArgs{WorkerID: workerID, TaskID: taskID}
    reply := &ReportTaskDoneReply{}
    if err := cl.c.Call("CoordinatorRPC.ReportTaskDone", args, reply); err != nil {
        return err
    }
    if !reply.OK {
        return fmt.Errorf("report not ok")
    }
    return nil
}



