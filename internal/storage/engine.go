package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/manjeet13/logbase/internal/config"
)

var MemTableFlushThreshold int // 1MB (small for testing)
var maxSSTables int

type Engine struct {
	wal       *WAL
	memtable  *MemTable
	sstables  []*SSTable
	dataDir   string
	nextTable int
}

func NewEngineWithConfig(cfg *config.Config) (*Engine, error) {
	// wire config values into package-level vars
	MemTableFlushThreshold = cfg.MemTableFlushSize
	maxSSTables = cfg.MaxSSTablesBeforeComp

	return NewEngine(cfg.DataDir)
}

func NewEngine(dataDir string) (*Engine, error) {
	os.MkdirAll(dataDir, 0755)

	wal, err := OpenWAL(filepath.Join(dataDir, "wal.log"))
	if err != nil {
		return nil, err
	}

	memtable := NewMemTable()

	engine := &Engine{
		wal:      wal,
		memtable: memtable,
		dataDir:  dataDir,
	}

	engine.loadSSTables()

	records, err := wal.Replay()
	if err != nil {
		return nil, err
	}

	for _, r := range records {
		if r.Type == PutRecord {
			memtable.Put(r.Key, r.Value)
		} else {
			memtable.Delete(r.Key)
		}
	}

	return engine, nil
}

func (e *Engine) Put(key, value []byte) error {
	if err := e.wal.AppendPut(key, value); err != nil {
		return err
	}

	e.memtable.Put(key, value)

	if e.memtable.Size() >= MemTableFlushThreshold {
		return e.flushMemTable()
	}

	return nil
}

func (e *Engine) Get(key []byte) ([]byte, bool) {
	if val, ok := e.memtable.Get(key); ok {
		return val, true
	}

	for i := len(e.sstables) - 1; i >= 0; i-- {
		table := e.sstables[i]

		if table.Bloom != nil && !table.Bloom.MightContain(key) {
			continue // definitely not here
		}

		if val, ok, _ := table.Get(key); ok {
			return val, true
		}
	}

	return nil, false
}

func (e *Engine) Delete(key []byte) error {
	// 1️⃣ Write delete to WAL
	if err := e.wal.AppendDelete(key); err != nil {
		return err
	}

	// 2️⃣ Insert tombstone into MemTable
	e.memtable.Delete(key)

	// 3️⃣ Flush if needed
	if e.memtable.Size() >= MemTableFlushThreshold {
		return e.flushMemTable()
	}

	return nil
}

func (e *Engine) BatchPut(entries map[string][]byte) error {
	// 1️⃣ Append all entries to WAL
	if err := e.wal.AppendBatch(entries); err != nil {
		return err
	}

	// 2️⃣ Apply to MemTable
	for k, v := range entries {
		e.memtable.Put([]byte(k), v)
	}

	// 3️⃣ Flush if needed
	if e.memtable.Size() >= MemTableFlushThreshold {
		return e.flushMemTable()
	}

	return nil
}

func (e *Engine) flushMemTable() error {
	snapshot := e.memtable.Snapshot()
	if len(snapshot) == 0 {
		return nil
	}

	path := fmt.Sprintf("%s/sst_%06d.dat", e.dataDir, e.nextTable)
	table, err := WriteSSTable(path, snapshot)
	if err != nil {
		return err
	}

	e.sstables = append(e.sstables, table)
	e.nextTable++
	e.memtable = NewMemTable()

	if err := e.wal.Rotate(); err != nil {
		return err
	}
	e.wal.Truncate(e.wal.segment - 1)
	if err := e.maybeCompact(); err != nil {
		return err
	}

	return nil
}

func (e *Engine) loadSSTables() {
	files, _ := filepath.Glob(filepath.Join(e.dataDir, "sst_*.dat"))
	sort.Strings(files)

	for _, f := range files {
		bf, _ := LoadBloomFilter(f + ".bloom")
		table := &SSTable{
			Path:  f,
			Bloom: bf,
		}
		table.LoadIndex()
		e.sstables = append(e.sstables, table)
		e.nextTable++
	}
}

func (e *Engine) ReadKeyRange(start, end []byte) (map[string][]byte, error) {
	result := make(map[string][]byte)

	// 1. MemTable
	for k, v := range e.memtable.Range(start, end) {
		result[k] = v
	}

	// 2. SSTables (newest → oldest)
	for i := len(e.sstables) - 1; i >= 0; i-- {
		data, err := e.sstables[i].Range(start, end)
		if err != nil {
			return nil, err
		}
		for k, v := range data {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
	}

	// 3. Remove tombstones
	for k, v := range result {
		if len(v) == 0 {
			delete(result, k)
		}
	}

	return result, nil
}

const MaxSSTables = 4

func (e *Engine) maybeCompact() error {
	if len(e.sstables) < MaxSSTables {
		return nil
	}
	return e.compactAll()
}

func (e *Engine) Close() error {
	//Flush remaining MemTable
	if e.memtable.Size() > 0 {
		if err := e.flushMemTable(); err != nil {
			return err
		}
	}

	//Close WAL
	if e.wal != nil {
		if err := e.wal.Close(); err != nil {
			return err
		}
	}

	return nil
}
