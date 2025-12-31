package node

import (
	"errors"
	"sync"
	"time"

	"dis_sys/backend/internal/utils"
)

var ErrWorkerNotFound = errors.New("worker not found")

type Manager struct {
	mu      sync.Mutex
	workers map[string]*Worker
}

func NewManager() *Manager {
	return &Manager{
		workers: make(map[string]*Worker),
	}
}

func (m *Manager) Register(addr string) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := utils.NewWorkerID()
	m.workers[id] = &Worker{
		ID:            id,
		Addr:          addr,
		Status:        Alive,
		LastHeartbeat: time.Now(),
	}
	return id
}

func (m *Manager) Heartbeat(workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	w, ok := m.workers[workerID]
	if !ok {
		return ErrWorkerNotFound
	}
	w.LastHeartbeat = time.Now()
	w.Status = Alive
	return nil
}

func (m *Manager) List() []*Worker {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]*Worker, 0, len(m.workers))
	for _, w := range m.workers {
		// copy pointer is OK for read-only display; if worried, deep copy
		out = append(out, w)
	}
	return out
}

// M1 可以先不做 dead detection；M4 再补。
// 这里只留签名，避免后面大改。
func (m *Manager) DetectDead(now time.Time, deadTimeout time.Duration) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var dead []string
	for id, w := range m.workers {
		if w.Status == Dead {
			continue
		}
		if now.Sub(w.LastHeartbeat) > deadTimeout {
			w.Status = Dead
			dead = append(dead, id)
		}
	}
	return dead
}
