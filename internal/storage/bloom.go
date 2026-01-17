package storage

import (
	"encoding/gob"
	"hash/fnv"
	"os"
)

type BloomFilter struct {
	bits []byte
	k    int // number of hash functions
}

func NewBloomFilter(size int, k int) *BloomFilter {
	return &BloomFilter{
		bits: make([]byte, size),
		k:    k,
	}
}

func (b *BloomFilter) Add(key []byte) {
	for i := 0; i < b.k; i++ {
		idx := b.hash(key, i) % (uint64(len(b.bits)) * 8)
		byteIdx := idx / 8
		bitIdx := idx % 8
		b.bits[byteIdx] |= (1 << bitIdx)
	}
}

func (b *BloomFilter) MightContain(key []byte) bool {
	for i := 0; i < b.k; i++ {
		idx := b.hash(key, i) % (uint64(len(b.bits)) * 8)
		byteIdx := idx / 8
		bitIdx := idx % 8
		if (b.bits[byteIdx] & (1 << bitIdx)) == 0 {
			return false
		}
	}
	return true
}

func (b *BloomFilter) hash(key []byte, seed int) uint64 {
	h := fnv.New64a()
	h.Write([]byte{byte(seed)})
	h.Write(key)
	return h.Sum64()
}

func (b *BloomFilter) Save(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(b)
}

func LoadBloomFilter(path string) (*BloomFilter, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var bf BloomFilter
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&bf)
	return &bf, err
}
