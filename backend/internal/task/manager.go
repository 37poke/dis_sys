package task

import (
	"fmt"
	"sync"
	"time"
)

type Manager struct {
	mu    sync.Mutex
	seq   int
	tasks map[string]*Task
	queue []string // pending task ids FIFO
}

func NewManager() *Manager {
	return &Manager{
		tasks: make(map[string]*Task),
		queue: make([]string, 0, 16),
	}
}

func (m *Manager) Submit(payload string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.seq++
	id := fmt.Sprintf("t-%06d", m.seq)
	now := time.Now()

	m.tasks[id] = &Task{
		ID:        id,
		Payload:   payload,
		State:     Pending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	m.queue = append(m.queue, id)
	return id
}

func (m *Manager) Get(id string) (*Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	return t, ok
}

func (m *Manager) List() []*Task {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]*Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t)
	}
	return out
}

// AssignNext: M2 核心 —— worker 来拉任务，coordinator 分配一个 pending 任务
func (m *Manager) AssignNext(workerID string) (*Task, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 找到队列里第一个仍然 pending 的任务
	for len(m.queue) > 0 {
		id := m.queue[0]
		m.queue = m.queue[1:]

		t := m.tasks[id]
		if t == nil {
			continue
		}
		if t.State != Pending {
			continue
		}

		t.State = Assigned
		t.WorkerID = workerID
		t.UpdatedAt = time.Now()
		return t, true
	}
	return nil, false
}


func (m *Manager) MarkFinished(taskID, workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.tasks[taskID]
	if !ok {
		return fmt.Errorf("task not found: %s", taskID)
	}
	if t.WorkerID != "" && t.WorkerID != workerID {
		return fmt.Errorf("task %s owned by %s, not %s", taskID, t.WorkerID, workerID)
	}

	t.State = Finished
	t.UpdatedAt = time.Now()
	return nil
}


