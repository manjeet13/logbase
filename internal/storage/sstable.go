package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

type SSTable struct {
	Path  string
	Index []IndexEntry
	Bloom *BloomFilter
}

type IndexEntry struct {
	Key    string
	Offset int64
}

const IndexInterval = 128

func WriteSSTable(path string, data map[string][]byte) (*SSTable, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	bf := NewBloomFilter(1024, 3) // 1KB bloom, 3 hashes

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := data[k]
		bf.Add([]byte(k))

		binary.Write(writer, binary.BigEndian, uint32(len(k)))
		writer.Write([]byte(k))
		binary.Write(writer, binary.BigEndian, uint32(len(v)))
		writer.Write(v)
	}

	writer.Flush()

	bfPath := path + ".bloom"
	if err := bf.Save(bfPath); err != nil {
		return nil, err
	}

	return &SSTable{
		Path:  path,
		Bloom: bf,
	}, nil
}

// Get performs a point lookup in the SSTable.
// This implementation performs a linear scan (v1).
func (s *SSTable) Get(key []byte) ([]byte, bool, error) {
	file, err := os.Open(s.Path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	target := string(key)

	for {
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, false, err
		}

		k := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, k); err != nil {
			return nil, false, err
		}

		var valLen uint32
		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return nil, false, err
		}

		v := make([]byte, valLen)
		if _, err := io.ReadFull(reader, v); err != nil {
			return nil, false, err
		}

		if string(k) == target {
			return v, true, nil
		}
	}

	return nil, false, nil
}

func (s *SSTable) Range(start, end []byte) (map[string][]byte, error) {
	file, err := os.Open(s.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	result := make(map[string][]byte)

	sKey := string(start)
	eKey := string(end)

	for {
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		k := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, k); err != nil {
			return nil, err
		}

		var valLen uint32
		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return nil, err
		}

		v := make([]byte, valLen)
		if _, err := io.ReadFull(reader, v); err != nil {
			return nil, err
		}

		keyStr := string(k)
		if keyStr < sKey {
			continue
		}
		if keyStr > eKey {
			break // sorted order lets us stop early
		}

		result[keyStr] = v
	}

	return result, nil
}

func (s *SSTable) LoadIndex() error {
	file, err := os.Open(s.Path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var offset int64
	count := 0

	for {
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			break
		}

		k := make([]byte, keyLen)
		io.ReadFull(reader, k)

		var valLen uint32
		binary.Read(reader, binary.BigEndian, &valLen)
		reader.Discard(int(valLen))

		if count%IndexInterval == 0 {
			s.Index = append(s.Index, IndexEntry{
				Key:    string(k),
				Offset: offset,
			})
		}

		offset += 4 + int64(keyLen) + 4 + int64(valLen)
		count++
	}
	return nil
}

func (e *Engine) compactAll() error {
	merged := make(map[string][]byte)

	// Newest → oldest
	for i := len(e.sstables) - 1; i >= 0; i-- {
		data, err := e.sstables[i].Range([]byte(""), []byte("\xff"))
		if err != nil {
			return err
		}

		for k, v := range data {
			if _, exists := merged[k]; !exists {
				merged[k] = v
			}
		}
	}

	// Remove tombstones
	for k, v := range merged {
		if len(v) == 0 {
			delete(merged, k)
		}
	}

	// Write new SSTable
	path := fmt.Sprintf("%s/sst_compacted_%06d.dat", e.dataDir, e.nextTable)
	table, err := WriteSSTable(path, merged)
	if err != nil {
		return err
	}

	// Remove old SSTables
	for _, t := range e.sstables {
		os.Remove(t.Path)
	}

	e.sstables = []*SSTable{table}
	e.nextTable++

	return nil
}
