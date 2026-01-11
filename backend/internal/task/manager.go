package task

import (
	"fmt"
	"sync"   // 用于互斥锁，保证并发安全
	"time"   // 用于处理任务的时间相关字段（创建、分配、租约等）
	"dis_sys/backend/internal/utils"
)


// Manager 任务管理器：负责任务的提交、分配、状态更新、超时回收、失败重试等核心逻辑
// 核心特性：
// 1. 并发安全：所有方法通过sync.Mutex保证多协程操作安全
// 2. FIFO队列：待执行任务按提交顺序分配
// 3. 容错机制：超时任务回收、失败任务重试（达到阈值则标记为Failed）
type Manager struct {
	mu    sync.Mutex // 互斥锁：保护tasks和queue的并发读写
	seq   int        // 任务序列号：用于生成唯一任务ID
	tasks map[string]*Task // 存储所有任务的映射表：key=任务ID，value=任务实例
	queue []string   // 待执行任务队列（FIFO）：存储Pending状态的任务ID
}

// NewManager 创建并初始化任务管理器实例
func NewManager() *Manager {
	return &Manager{
		tasks: make(map[string]*Task),      // 初始化任务映射表
		queue: make([]string, 0, 16),       // 初始化待执行队列，初始容量16
	}
}

// Submit 提交新任务，返回任务唯一ID
// 参数：payload - 任务内容
// 返回值：任务ID
func (m *Manager) Submit(payload string) string {
	m.mu.Lock()         // 加锁：保证任务ID生成和队列操作的原子性
	defer m.mu.Unlock() // 函数退出时解锁

	m.seq++                          // 序列号自增（保证任务ID唯一）
	id := fmt.Sprintf("t-%06d", m.seq) // 生成任务ID（格式：t-000001）
	now := time.Now()                // 当前时间作为任务创建时间

	// 创建新任务实例并存入映射表
	m.tasks[id] = &Task{
		ID:        id,
		Payload:   payload,
		State:     Pending,    // 初始状态为待执行
		CreatedAt: now,
		UpdatedAt: now,
	}
	// 将新任务ID加入待执行队列
	m.queue = append(m.queue, id)
	return id // 返回任务唯一ID
}

// Get 根据任务ID查询任务实例
// 参数：id - 任务ID
// 返回值：任务实例 / 是否存在（bool）
func (m *Manager) Get(id string) (*Task, bool) {
	m.mu.Lock()         // 加锁：保证读取时map不被修改
	defer m.mu.Unlock() // 函数退出时解锁
	t, ok := m.tasks[id]
	return t, ok
}

// List 获取所有任务的列表（只读）
// 返回值：所有任务实例的切片
func (m *Manager) List() []*Task {
	m.mu.Lock()         // 加锁：保证读取时map不被修改
	defer m.mu.Unlock() // 函数退出时解锁

	out := make([]*Task, 0, len(m.tasks)) // 初始化切片，容量为任务总数
	for _, t := range m.tasks {
		out = append(out, t) // 遍历所有任务，加入切片
	}
	return out
}

// AssignNext 分配下一个待执行任务给Worker（FIFO顺序）
// 参数：
//   workerID - 接收任务的WorkerID
//   lease - 任务租约时间（Worker需在此时间内完成/上报任务）
// 返回值：
//   任务实例 / 是否分配成功（bool，无待执行任务则返回false）
func (m *Manager) AssignNext(workerID string, lease time.Duration) (*Task, bool) {
	m.mu.Lock()         // 加锁：保证队列和任务状态操作的原子性
	defer m.mu.Unlock() // 函数退出时解锁

	// 遍历待执行队列，寻找第一个Pending状态的任务
	for len(m.queue) > 0 {
		id := m.queue[0]          // 取出队列头部的任务ID（FIFO）
		m.queue = m.queue[1:]     // 从队列中移除该任务ID

		t := m.tasks[id]
		if t == nil {             // 任务不存在（异常场景），跳过
			continue
		}
		if t.State != Pending {  // 任务非待执行状态（如已被分配/完成），跳过
			continue
		}

		// 设置默认最大重试次数（未显式设置时为3次）
		if t.MaxRetry == 0 {
			t.MaxRetry = 3
		}

		now := time.Now()
		// 更新任务状态为已分配
		t.State = Assigned
		t.WorkerID = workerID    // 绑定分配的WorkerID
		t.Attempts++             // ✅ 每次分配算一次尝试（核心逻辑：重试次数计数）
		t.AssignedAt = now       // 记录分配时间
		t.LeaseUntil = now.Add(lease) // 设置租约过期时间
		t.LeaseToken = utils.NewToken()     // ✅新增：每次分配一个新token
		t.UpdatedAt = now        // 更新最后修改时间

		return t, true // 任务分配成功
	}

	return nil, false // 无待执行任务，分配失败
}

// MarkFinished 标记任务为已完成状态
// 参数：
//   taskID - 任务ID
//   workerID - 执行该任务的WorkerID
// 返回值：错误（任务不存在/非当前Worker分配时返回）
func (m *Manager) MarkFinished(taskID, workerID string, leaseToken string) error {
	m.mu.Lock()         // 加锁：保证任务状态更新的原子性
	defer m.mu.Unlock() // 函数退出时解锁

	t, ok := m.tasks[taskID]
	if !ok { // 任务不存在
		return fmt.Errorf("task not found: %s", taskID)
	}
	// 校验任务归属：只有分配给该Worker的任务才能标记为完成（防止误操作）
	if t.WorkerID != "" && t.WorkerID != workerID {
		return fmt.Errorf("task %s owned by %s, not %s", taskID, t.WorkerID, workerID)
	}
	// ✅ token 校验：防止旧 lease 上报
	if t.LeaseToken != "" && leaseToken != t.LeaseToken {
		return fmt.Errorf("stale lease token for task %s: got=%s want=%s",
			taskID, leaseToken, t.LeaseToken)
	}

	// 更新任务状态为已完成
	// ====== 更新状态 ======
	t.State = Finished
	t.UpdatedAt = time.Now()

	// ====== 清理租约与归属信息（非常重要） ======
	t.WorkerID = ""
	t.AssignedAt = time.Time{}
	t.LeaseUntil = time.Time{}
	t.LeaseToken = ""

	return nil
}

// ===== M3 新增：超时回收（核心容错） =====
// RequeueExpired 回收过期任务：将超过租约时间未完成的任务重新入队（未达重试阈值）或标记为失败（达阈值）
// 参数：
//   now - 当前时间（外部传入保证时间一致性）
//   lease - 任务租约时间（兜底：实际优先用Task.LeaseUntil判断）
//   maxAttempts - 全局最大重试次数（兜底：优先用Task.MaxRetry）
// 返回值：被重新入队的任务ID列表
func (m *Manager) RequeueExpired(now time.Time, lease time.Duration, maxAttempts int) []string {
	m.mu.Lock()         // 加锁：保证任务状态更新和队列操作的原子性
	defer m.mu.Unlock() // 函数退出时解锁

	var requeued []string // 存储被重新入队的任务ID

	// 遍历所有任务，筛选出已分配且过期的任务
	for _, t := range m.tasks {
		if t == nil {
			continue
		}
		if t.State != Assigned { // 仅处理已分配状态的任务
			continue
		}

		// ✅ 核心判断：只用LeaseUntil判断是否超时（优先使用任务自身的租约时间）
		if t.LeaseUntil.IsZero() || now.Before(t.LeaseUntil) {
			continue // 未超时，跳过
		}
	
		// 确定最大重试次数：优先用任务自身的MaxRetry，否则用全局maxAttempts（默认3次）
		limit := maxAttempts
		if limit <= 0 {
			limit = 3
		}
		if t.MaxRetry > 0 {
			limit = t.MaxRetry
		}

		// 未达最大重试次数：重新入队
		if t.Attempts < limit {
			t.State = Pending          // 重置为待执行状态
			t.WorkerID = ""            // 清空WorkerID
			t.AssignedAt = time.Time{} // 清空分配时间
			t.LeaseUntil = time.Time{} // 清空租约时间
			t.UpdatedAt = now          // 更新最后修改时间
			m.queue = append(m.queue, t.ID) // 重新加入待执行队列
			requeued = append(requeued, t.ID) // 记录重新入队的任务ID
		} else {
			// 达到最大重试次数：标记为失败
			t.State = Failed
			t.UpdatedAt = now
		}
	}

	return requeued // 返回被重新入队的任务ID列表
}

// MarkFailed 标记任务执行失败，根据重试阈值判断是否重新入队
// 参数：
//   taskID - 任务ID
//   workerID - 执行该任务的WorkerID
//   errMsg - 失败原因描述
// 返回值：
//   requeued - 是否重新入队（true=可重试，false=达阈值/任务已完成）
//   error - 任务不存在/非当前Worker分配时返回
func (m *Manager) MarkFailed(taskID, workerID, leaseToken,  errMsg string) (bool, error) {
	m.mu.Lock()         // 加锁：保证任务状态更新和队列操作的原子性
	defer m.mu.Unlock() // 函数退出时解锁

	t, ok := m.tasks[taskID]
	if !ok { // 任务不存在
		return false, fmt.Errorf("task not found: %s", taskID)
	}

	// 幂等处理：任务已完成则忽略（防止Worker重复上报）
	if t.State == Finished {
		return false, nil
	}

	// 可选校验：只有分配给该Worker的任务才能上报失败（建议保留，防止误操作）
	if t.WorkerID != "" && t.WorkerID != workerID {
		return false, fmt.Errorf("task %s owned by %s, not %s", taskID, t.WorkerID, workerID)
	}

	// ✅ token 校验：防止旧 lease 上报
	if t.LeaseToken != "" && leaseToken != t.LeaseToken {
		return false, fmt.Errorf("stale lease token for task %s: got=%s want=%s",
			taskID, leaseToken, t.LeaseToken)
	}
	
	// 设置默认最大重试次数（未显式设置时为3次）
	if t.MaxRetry == 0 {
		t.MaxRetry = 3
	}

	// 记录失败信息和更新时间
	t.LastError = errMsg
	t.UpdatedAt = time.Now()



	// ✅ 核心判断：Attempts已在AssignNext中增加，此处仅判断是否达阈值
	if t.Attempts >= t.MaxRetry {
		t.State = Failed // 达阈值，标记为失败
		t.UpdatedAt = time.Now()

		// ====== 清理租约 ======
		t.WorkerID = ""
		t.AssignedAt = time.Time{}
		t.LeaseUntil = time.Time{}
		t.LeaseToken = ""
		return false, nil
	}

	// ✅（可选但推荐）保留 workerID，便于日志展示/追踪
	t.WorkerID = workerID
	// 未达阈值：重置状态并重新入队
	t.State = Pending          // 重置为待执行状态
	t.WorkerID = ""            // 清空WorkerID
	t.AssignedAt = time.Time{} // 清空分配时间
	t.LeaseUntil = time.Time{} // 清空租约时间
	t.LeaseToken = "" //清空token
	m.queue = append(m.queue, t.ID) // 重新加入待执行队列

	return true, nil // 返回重新入队标识
}