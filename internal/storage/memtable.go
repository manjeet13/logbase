package storage

import "sync"

type MemTable struct {
	mu    sync.RWMutex
	data  map[string][]byte
	bytes int
}

func NewMemTable() *MemTable {
	return &MemTable{
		data: make(map[string][]byte),
	}
}

func (m *MemTable) Put(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := string(key)
	if old, ok := m.data[k]; ok {
		m.bytes -= len(old)
	}

	m.data[k] = value
	m.bytes += len(k) + len(value)
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[string(key)]
	return val, ok
}

func (m *MemTable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := string(key)
	if old, ok := m.data[k]; ok {
		m.bytes -= len(old)
		delete(m.data, k)
	}
}

func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bytes
}

func (m *MemTable) Snapshot() map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snap := make(map[string][]byte, len(m.data))
	for k, v := range m.data {
		snap[k] = v
	}
	return snap
}

func (m *MemTable) Range(start, end []byte) map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string][]byte)
	s := string(start)
	e := string(end)

	for k, v := range m.data {
		if k >= s && k <= e {
			result[k] = v
		}
	}
	return result
}
