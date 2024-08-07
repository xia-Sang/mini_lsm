package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/google/btree"
)

func (r *Record) Less(than btree.Item) bool {
	return r.Key < than.(*Record).Key
}

// 结构体
type MemTable struct {
	data *btree.BTree //树
	mu   sync.RWMutex //锁
	size int          //容量
}

// 产生新的memtable
func NewMemTable() *MemTable {
	return &MemTable{
		data: btree.New(9),
		size: 0,
	}
}

// set
func (t *MemTable) Set(r *Record) {
	t.mu.Lock()
	defer t.mu.Unlock()

	old := t.data.ReplaceOrInsert(r)
	if old != nil {
		t.size -= len(old.(*Record).Value)
	}
	t.size += len(r.Value)
}

// 查询
func (t *MemTable) Query(key string) *Record {
	t.mu.RLock()
	defer t.mu.RUnlock()

	item := &Record{Key: key}
	found := t.data.Get(item)
	if found == nil {
		return nil
	}
	return found.(*Record)
}

// 获取容量
func (t *MemTable) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.size
}

// 将数据装维bytes
func (t *MemTable) Bytes() []byte {
	t.mu.RLock()
	defer t.mu.RUnlock()

	buf := bytes.NewBuffer(nil)
	t.data.Ascend(func(item btree.Item) bool {
		v := item.(*Record)
		n, body := v.Bytes()
		binary.Write(buf, binary.LittleEndian, uint32(n))
		buf.Write(body)
		return true
	})
	return buf.Bytes()
}

// 得到所有的records
func (t *MemTable) GetRecords() []*Record {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var records []*Record
	t.data.Ascend(func(item btree.Item) bool {
		v := item.(*Record)
		records = append(records, v)
		return true
	})
	return records
}

// 不需要添加锁的 set部分已经添加了
func (t *MemTable) Restore(data []byte) error {
	buf := bytes.NewBuffer(data)
	var n uint32
	for {
		if err := binary.Read(buf, binary.LittleEndian, &n); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		cmd := new(Record)
		cmd.Restore(buf.Next(int(n)))
		t.Set(cmd)
	}
	return nil
}

// 得到第一个key
func (t *MemTable) First() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var firstKey string
	t.data.Ascend(func(item btree.Item) bool {
		firstKey = item.(*Record).Key
		return false
	})
	return firstKey
}

// 测试使用
func (t *MemTable) Show() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	fmt.Println("memory table info!")
	t.data.Ascend(func(item btree.Item) bool {
		fmt.Printf("(%s:%v:%v)\n", item.(*Record).Key, item.(*Record).Value, item.(*Record).RType)
		return true
	})
}

// 数据合并
func (t *MemTable) Merge(other *MemTable) {
	if other == nil {
		return
	}
	other.mu.RLock()
	defer other.mu.RUnlock()

	other.data.Ascend(func(item btree.Item) bool {
		record := item.(*Record)
		t.Set(record)
		return true
	})
}
