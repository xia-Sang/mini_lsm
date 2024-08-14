package lsm

// todo: 实现布隆过滤器
import (
	"hash"
	"math"

	"github.com/spaolacci/murmur3"
)

// BloomFilter represents a Bloom filter data structure
type BloomFilter struct {
	bitSet    []bool
	size      uint
	hashFuncs []hash.Hash64
}

// NewBloomFilter creates a new Bloom filter with the given size and number of hash functions
func NewBloomFilter(size uint, numHashFuncs uint) *BloomFilter {
	hashFuncs := make([]hash.Hash64, numHashFuncs)
	for i := uint(0); i < numHashFuncs; i++ {
		hashFuncs[i] = murmur3.New64WithSeed(uint32(i))
	}
	return &BloomFilter{
		bitSet:    make([]bool, size),
		size:      size,
		hashFuncs: hashFuncs,
	}
}

// Add adds an item to the Bloom filter
func (bf *BloomFilter) Add(item []byte) {
	for _, h := range bf.hashFuncs {
		h.Reset()
		h.Write(item)
		index := uint(h.Sum64() % uint64(bf.size))
		bf.bitSet[index] = true
	}
}

// Contains checks if an item might be in the Bloom filter
func (bf *BloomFilter) Contains(item []byte) bool {
	for _, h := range bf.hashFuncs {
		h.Reset()
		h.Write(item)
		index := uint(h.Sum64() % uint64(bf.size))
		if !bf.bitSet[index] {
			return false
		}
	}
	return true
}

// EstimateFalsePositiveRate calculates the estimated false positive rate
func (bf *BloomFilter) EstimateFalsePositiveRate(numItems int) float64 {
	k := float64(len(bf.hashFuncs))
	m := float64(bf.size)
	n := float64(numItems)
	return math.Pow(1-math.Exp(-k*n/m), k)
}
