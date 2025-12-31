package rpc

import (
	"fmt"

	"dis_sys/backend/internal/node"
)

type CoordinatorRPC struct {
	Nodes *node.Manager
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
