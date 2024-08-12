package lsm

import (
	"errors"
	"fmt"
)

type Node struct {
	opts       *Options
	fileName   string
	startKey   string
	endKey     string
	sstReader  *SSTReader
	spareIndex []*SparseIndex
	_cache     map[int]*MemTable
}

func (n *Node) Close() {
	n.sstReader.Close()
	n.spareIndex = nil
	n._cache = nil
}
func (n *Node) Show() {
	for i := len(n.spareIndex) - 1; i >= 0; i-- {
		mem, err := n.sstReader.readSSTBlock(n.spareIndex[i].DataOffset)
		if err != nil {
			panic(err)
		}
		mem.Show()

	}
}
func NewNode(fileName string, sstReader *SSTReader, opts *Options, spareIndex []*SparseIndex) (*Node, error) {
	n := &Node{
		fileName:   fileName,
		sstReader:  sstReader,
		spareIndex: spareIndex,
		_cache:     make(map[int]*MemTable),
		opts:       opts,
	}
	var err error
	n.spareIndex, err = n.sstReader.ReadBlock()
	if len(n.spareIndex) == 0 {
		fmt.Println(n.spareIndex, n.fileName)
		return nil, ErrorNotExist
	}
	n.startKey = n.spareIndex[0].MinKey
	n.endKey = n.spareIndex[len(n.spareIndex)-1].MaxKey
	return n, err
}

func (n *Node) Query(key string) (string, bool, error) {
	if n.startKey > key || n.endKey < key {
		return "", false, nil
	}
	for i := len(n.spareIndex) - 1; i >= 0; i-- {
		if n.startKey > key || n.endKey < key {
			continue
		}
		val, ok, err := n.load(i, key)
		if ok {
			return val, true, err
		}
		if err != nil && !errors.Is(err, ErrorNotExist) {
			return "", false, err
		}
	}
	return "", false, nil
}
func query(v *MemTable, key string) (string, bool, error) {
	record := v.Query(key)
	if record != nil {
		if record.RType == RecordDelete {
			return "", false, ErrorNotExist
		}
		return record.Value, true, nil
	}
	return "", false, nil
}
func (n *Node) load(i int, key string) (string, bool, error) {
	if v, ok := n._cache[i]; ok {
		return query(v, key)
	}
	mem, err := n.sstReader.readSSTBlock(n.spareIndex[i].DataOffset)
	if err != nil {
		return "", false, err
	}
	n._cache[i] = mem
	return query(mem, key)
}
func (n *Node) Merge() (*MemTable, error) {
	m := NewMemTable()
	if len(n._cache) != 0 {
		for j := range n.opts.tableNum {
			m.Merge(n._cache[j])
		}
		return m, nil
	}

	var err error
	n.spareIndex, err = n.sstReader.ReadBlock()
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(n.spareIndex); i++ {
		mem, err := n.sstReader.readSSTBlock(n.spareIndex[i].DataOffset)
		if err != nil {
			return nil, err
		}
		m.Merge(mem)
	}
	return m, nil
}
