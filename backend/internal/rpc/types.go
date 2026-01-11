package rpc

// 该文件定义了 Worker 与 Coordinator 之间 RPC 通信的所有请求/响应参数结构体
// 遵循“参数结构体Args + 响应结构体Reply”的命名规范，字段与业务逻辑严格对齐

// ---- Worker <-> Coordinator: 注册与心跳相关结构体 ----

// RegisterWorkerArgs Worker向Coordinator注册的请求参数结构体
type RegisterWorkerArgs struct {
	Addr string // Worker节点的地址（如127.0.0.1:42997），用于展示/后续健康检查等扩展场景
}

// RegisterWorkerReply Coordinator响应Worker注册的结果结构体
type RegisterWorkerReply struct {
	WorkerID string // Coordinator为Worker生成的唯一标识，后续心跳/拉取任务需携带该ID
}

// HeartbeatArgs Worker向Coordinator发送心跳的请求参数结构体
type HeartbeatArgs struct {
	WorkerID string // Worker唯一标识，用于Coordinator识别并更新对应节点的心跳时间
}

// HeartbeatReply Coordinator响应Worker心跳的结果结构体
type HeartbeatReply struct {
	OK bool // 心跳更新是否成功（true=成功，false=失败，如WorkerID不存在）
}

// ---- Worker -> Coordinator: 拉取任务相关结构体 ----

// RequestTaskArgs Worker向Coordinator拉取任务的请求参数结构体
type RequestTaskArgs struct {
	WorkerID string // Worker唯一标识，用于Coordinator分配任务时绑定节点
}

// RequestTaskReply Coordinator响应Worker拉取任务的结果结构体
type RequestTaskReply struct {
	HasTask bool // 是否有可分配的任务（true=有任务，false=无任务）

	// 以下字段仅在HasTask=true时有效
	TaskID   string // 任务唯一标识
	Payload  string // 任务内容（如"demo-0"，实际场景可存放宠物健康分析的计算指令）
	State    string // 任务当前状态（如Assigned/Processing/Failed等）
	WorkerID string // 分配给该任务的WorkerID（与请求的WorkerID一致）

	// M3: 调试/可靠性字段（与task.Task结构体的Attempts/AssignedAt字段对齐）
	Attempts     int   // 任务已尝试执行的次数（累计重试次数，用于控制最大重试阈值）
	AssignedAtMs int64 // 任务分配时间（毫秒级Unix时间戳），用于观测/排错（如任务分配耗时）
	LeaseUntilMs int64 // 任务租约过期时间（毫秒级Unix时间戳），Worker需在该时间前完成/上报任务，否则任务过期重入队列
	LeaseToken string
}

// ---- Worker -> Coordinator: 上报任务完成相关结构体 ----

// ReportTaskDoneArgs Worker向Coordinator上报任务执行成功的请求参数结构体
type ReportTaskDoneArgs struct {
	WorkerID string // Worker唯一标识，用于Coordinator验证任务是否分配给该节点
	TaskID   string // 执行成功的任务唯一标识
	LeaseToken  string   // ✅新增

}

// ReportTaskDoneReply Coordinator响应Worker任务完成上报的结果结构体
type ReportTaskDoneReply struct {
	OK bool // 任务状态更新是否成功（true=成功，false=失败，如任务不存在/已被其他节点处理）
}

// ---- Worker -> Coordinator: 上报任务失败（可重试）相关结构体 ----

// ReportTaskFailedArgs Worker向Coordinator上报任务执行失败的请求参数结构体
type ReportTaskFailedArgs struct {
	WorkerID string // Worker唯一标识，用于Coordinator验证任务归属
	TaskID   string // 执行失败的任务唯一标识
	ErrorMsg string // 任务失败的原因描述（如"simulated error"），用于调试/日志分析
	LeaseToken  string   // ✅新增
}

// ReportTaskFailedReply Coordinator响应Worker任务失败上报的结果结构体
type ReportTaskFailedReply struct {
	OK       bool // 失败状态上报是否成功（true=成功，false=失败，如任务不存在）
	Requeued bool // Coordinator是否将该任务重新入队（true=可重试，false=达到最大重试次数，不再重试）
}