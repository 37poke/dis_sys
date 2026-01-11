package rpc

import (
	"fmt"
	"time"

	"dis_sys/backend/internal/task" // 任务管理包：提供任务分配、状态更新等核心功能
	"dis_sys/backend/internal/node" // 节点管理包：提供Worker注册、心跳更新等功能
)

// CoordinatorRPC 协调器RPC服务核心结构体
// 作为RPC服务的实现载体，关联节点管理器和任务管理器，
// 对外暴露Worker节点所需的注册、心跳、任务拉取、结果上报等RPC接口
type CoordinatorRPC struct {
	Nodes *node.Manager // 节点管理器实例：管理所有Worker节点的注册、心跳、状态
	Tasks *task.Manager // 任务管理器实例：管理所有任务的分配、状态、重试、过期等逻辑
}

// RegisterWorker RPC方法：Worker节点注册接口
// 入参：args - 包含Worker节点地址的参数结构体
// 出参：reply - 包含生成的Worker唯一ID的响应结构体
// 返回值：错误信息（参数非法时返回，否则返回nil）
func (c *CoordinatorRPC) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	// 参数校验：确保入参非空且Worker地址有效
	if args == nil || args.Addr == "" {
		return fmt.Errorf("missing worker addr")
	}
	// 调用节点管理器完成Worker注册，生成唯一WorkerID
	id := c.Nodes.Register(args.Addr)
	// 将生成的WorkerID写入响应，返回给Worker节点
	reply.WorkerID = id
	return nil // 注册成功，无错误返回
}

// Heartbeat RPC方法：Worker节点心跳保活接口
// 入参：args - 包含WorkerID的参数结构体
// 出参：reply - 包含心跳是否成功的响应结构体
// 返回值：错误信息（参数非法/Worker不存在时返回，否则返回nil）
func (c *CoordinatorRPC) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	// 参数校验：确保入参非空且WorkerID有效
	if args == nil || args.WorkerID == "" {
		return fmt.Errorf("missing worker id")
	}
	// 调用节点管理器更新该Worker的心跳时间和状态（标记为存活）
	if err := c.Nodes.Heartbeat(args.WorkerID); err != nil {
		return err // Worker不存在时返回node.ErrWorkerNotFound
	}
	// 心跳更新成功，设置响应状态为OK
	reply.OK = true
	return nil
}

// RequestTask RPC方法：Worker节点拉取任务接口
// 入参：args - 包含WorkerID的参数结构体
// 出参：reply - 包含任务信息（如有）的响应结构体
// 返回值：错误信息（参数非法/任务管理器未初始化时返回，否则返回nil）
func (c *CoordinatorRPC) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// 参数校验：确保入参非空且WorkerID有效
	if args == nil || args.WorkerID == "" {
		return fmt.Errorf("missing worker id")
	}
	// 校验任务管理器是否初始化（防止空指针）
	if c.Tasks == nil {
		return fmt.Errorf("task manager not initialized")
	}

	// 调用任务管理器分配下一个待执行任务，设置任务租约时间为5秒
	// 租约时间：Worker需在该时间内完成任务/上报状态，否则任务视为过期重入队列
	t, ok := c.Tasks.AssignNext(args.WorkerID, 5*time.Second)
	if !ok {
		// 无可用任务，标记HasTask为false，直接返回
		reply.HasTask = false
		return nil
	}

	// 有可用任务，填充任务详情到响应结构体
	reply.HasTask = true            // 标记存在可执行任务
	reply.TaskID = t.ID             // 任务唯一标识
	reply.Payload = t.Payload       // 任务内容（如"demo-0"）
	reply.State = string(t.State)   // 任务当前状态（如Assigned）
	reply.WorkerID = t.WorkerID     // 分配的Worker节点ID
	reply.AssignedAtMs = t.AssignedAt.UnixMilli() // 任务分配时间（毫秒级时间戳）
	reply.Attempts = t.Attempts     // 任务已重试次数
	reply.LeaseUntilMs = t.LeaseUntil.UnixMilli() // 新增：任务租约过期时间（毫秒级时间戳）
	reply.LeaseToken = t.LeaseToken// 分配token
	return nil
}

// ReportTaskDone RPC方法：Worker节点上报任务执行成功接口
// 入参：args - 包含WorkerID和TaskID的参数结构体
// 出参：reply - 包含上报是否成功的响应结构体
// 返回值：错误信息（参数非法/任务管理器未初始化/任务状态更新失败时返回）
func (c *CoordinatorRPC) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	// 参数校验：确保WorkerID和TaskID均不为空
	if args == nil || args.WorkerID == "" || args.TaskID == "" {
		return fmt.Errorf("missing args")
	}
	// 校验任务管理器是否初始化
	if c.Tasks == nil {
		return fmt.Errorf("task manager not initialized")
	}
	// 调用任务管理器标记任务为“已完成”状态
	if err := c.Tasks.MarkFinished(args.TaskID, args.WorkerID, args.LeaseToken); err != nil {
		return err // 任务不存在/已被其他节点处理/状态异常时返回错误
	}
	// 任务状态更新成功，设置响应为OK
	reply.OK = true
	return nil
}

// ReportTaskFailed RPC方法：Worker节点上报任务执行失败接口
// 入参：args - 包含WorkerID、TaskID、错误信息的参数结构体
// 出参：reply - 包含上报是否成功、任务是否重入队列的响应结构体
// 返回值：错误信息（参数非法/任务管理器未初始化/任务状态更新失败时返回）
func (c *CoordinatorRPC) ReportTaskFailed(args *ReportTaskFailedArgs, reply *ReportTaskFailedReply) error {
	// 参数校验：确保WorkerID和TaskID均不为空
	if args == nil || args.WorkerID == "" || args.TaskID == "" {
		return fmt.Errorf("missing worker id or task id")
	}
	// 校验任务管理器是否初始化
	if c.Tasks == nil {
		return fmt.Errorf("task manager not initialized")
	}

	// 调用任务管理器标记任务为“失败”，并判断是否重新入队（根据最大重试次数）
	// requeued：true=任务重入队列（可重试），false=达到最大重试次数（不再重试）
	requeued, err := c.Tasks.MarkFailed(args.TaskID, args.WorkerID,args.LeaseToken, args.ErrorMsg)
	if err != nil {
		return err // 任务不存在/已完成/状态异常时返回错误
	}

	// 上报失败状态成功，填充响应参数
	reply.OK = true           // 上报操作本身成功
	reply.Requeued = requeued // 任务是否重新入队（供Worker感知重试状态）
	return nil
}

