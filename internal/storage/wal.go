package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	walPut byte = iota + 1
	walDelete
)

type RecordType byte

const (
	PutRecord    RecordType = 1
	DeleteRecord RecordType = 2
)

type WALRecord struct {
	Type  RecordType
	Key   []byte
	Value []byte
}

type WAL struct {
	mu      sync.Mutex
	dir     string
	file    *os.File
	writer  *bufio.Writer
	segment int
}

func OpenWAL(dir string) (*WAL, error) {
	os.MkdirAll(dir, 0755)

	wal := &WAL{dir: dir}
	wal.segment = wal.nextSegmentID()
	err := wal.openSegment(wal.segment)
	return wal, err
}

func (w *WAL) openSegment(id int) error {
	path := filepath.Join(w.dir, fmt.Sprintf("wal_%06d.log", id))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.file = file
	w.writer = bufio.NewWriter(file)
	w.segment = id
	return nil
}

func (w *WAL) AppendPut(key, value []byte) error {
	if err := w.appendRecord(PutRecord, key, value); err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *WAL) AppendDelete(key []byte) error {
	if err := w.appendRecord(DeleteRecord, key, nil); err != nil {
		return err
	}
	return w.writer.Flush()
}

func (w *WAL) appendRecord(rt RecordType, key, value []byte) error {
	if err := binary.Write(w.writer, binary.BigEndian, rt); err != nil {
		return err
	}

	if err := binary.Write(w.writer, binary.BigEndian, uint32(len(key))); err != nil {
		return err
	}
	if _, err := w.writer.Write(key); err != nil {
		return err
	}

	if err := binary.Write(w.writer, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	if _, err := w.writer.Write(value); err != nil {
		return err
	}

	return nil
}

func (w *WAL) AppendBatch(entries map[string][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for k, v := range entries {
		w.writer.WriteByte(walPut)

		binary.Write(w.writer, binary.BigEndian, uint32(len(k)))
		w.writer.Write([]byte(k))

		binary.Write(w.writer, binary.BigEndian, uint32(len(v)))
		w.writer.Write(v)
	}

	// 🔑 Single flush for the whole batch
	return w.writer.Flush()
}

func (w *WAL) Replay() ([]WALRecord, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(w.file)
	records := []WALRecord{}

	for {
		var rt RecordType
		if err := binary.Read(reader, binary.BigEndian, &rt); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, err
		}

		var valLen uint32
		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return nil, err
		}

		value := make([]byte, valLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}

		records = append(records, WALRecord{
			Type:  rt,
			Key:   key,
			Value: value,
		})
	}

	return records, nil
}

func (w *WAL) Rotate() error {
	w.writer.Flush()
	w.file.Close()
	return w.openSegment(w.segment + 1)
}

func (w *WAL) Truncate(before int) error {
	files, _ := filepath.Glob(filepath.Join(w.dir, "wal_*.log"))
	for _, f := range files {
		id := extractID(f)
		if id < before {
			os.Remove(f)
		}
	}
	return nil
}

func (w *WAL) nextSegmentID() int {
	files, err := filepath.Glob(filepath.Join(w.dir, "wal_*.log"))
	if err != nil || len(files) == 0 {
		return 0
	}

	maxID := 0
	for _, f := range files {
		id := extractID(f)
		if id > maxID {
			maxID = id
		}
	}

	return maxID + 1
}

func extractID(path string) int {
	base := filepath.Base(path)
	// base = "wal_000012.log"

	start := strings.Index(base, "_")
	end := strings.Index(base, ".log")
	if start == -1 || end == -1 {
		return 0
	}

	idStr := base[start+1 : end]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return 0
	}

	return id
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}

	if w.file != nil {
		return w.file.Close()
	}

	return nil
}
