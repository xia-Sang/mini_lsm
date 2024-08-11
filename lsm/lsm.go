package lsm

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	WalFileName = "wal_file"
	WalSuffix   = ".wal"
	SSTSuffix   = ".sst"
)

type ReadOnlyMemTable struct {
	walFile  string
	memTable *MemTable
}
type Lsm struct {
	opts           *Options
	lock           sync.RWMutex           //锁
	memTable       *MemTable              //memtable信息
	rOnlyMemTable  []*ReadOnlyMemTable    //只读的memtable
	walWriter      *WalWriter             //wal写入
	memTableIndex  int                    //index
	memCompactChan chan *ReadOnlyMemTable //管道传递 todo：并发使用
	nodes          [][]*Node              //节点配置
	sstSeq         []atomic.Int32         //sst seq序号
}

func NewLsm(options *Options) *Lsm {
	lsm, err := DefaultLsmTree(options)
	if err != nil {
		panic(err)
	}
	return lsm
}
func DefaultLsmTree(opts *Options) (*Lsm, error) {
	lsm := &Lsm{
		opts:           opts,
		rOnlyMemTable:  make([]*ReadOnlyMemTable, 0),
		memTableIndex:  0,
		memCompactChan: make(chan *ReadOnlyMemTable),
		nodes:          make([][]*Node, opts.maxLevel),
		sstSeq:         make([]atomic.Int32, opts.maxLevel),
	}
	go lsm.compact()

	if err := lsm.LoadWal(); err != nil {
		return nil, err
	}
	if err := lsm.LoadSST(); err != nil {
		return nil, err
	}
	return lsm, nil
}
func (t *Lsm) Put(key, value string) error {
	record := &Record{
		Key:   key,
		Value: value,
		RType: RecordUpdate,
	}
	return t.setRecord(record)
}
func (t *Lsm) Delete(key string) error {
	record := &Record{
		Key:   key,
		RType: RecordDelete,
	}
	return t.setRecord(record)
}
func (t *Lsm) setRecord(record *Record) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Write to WAL and ensure it's synced to disk
	if err := t.walWriter.WriteAndSync(record); err != nil {
		return err
	}

	// Add to memTable
	t.memTable.Set(record)

	if !t.checkOverflow() {
		return nil
	}
	t.refreshMemTableLocked()
	return nil
}
func (t *Lsm) checkOverflow() bool {
	return t.memTable.Len() >= t.opts.maxSSTSize
}
func (t *Lsm) Query(key string) (string, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	record := t.memTable.Query(key)
	if record != nil {
		if record.RType == RecordDelete {
			return "", ErrorNotExist
		}
		return record.Value, nil
	}

	for i := len(t.rOnlyMemTable) - 1; i >= 0; i-- {
		record := t.rOnlyMemTable[i].memTable.Query(key)
		if record != nil {
			if record.RType == RecordDelete {
				return "", ErrorNotExist
			}
			return record.Value, nil
		}
	}

	for _, nodes := range t.nodes {
		for j := len(nodes) - 1; j >= 0; j-- {
			node := nodes[j]
			value, ok, err := node.Query(key)
			if ok {
				fmt.Println("node", node.fileName, key)
				return value, err
			}
			if err != nil && !errors.Is(err, ErrorNotExist) {
				return "", err
			}
		}
	}

	return "", ErrorNotExist
}

// 不是并发来实现的来实现的 目前先使用这个
// 并发会存在资源竞争 后续来完善即可
// func (t *Lsm) refreshMemTableLocked() {
// 	t.syncMemTable(t.memTable)
// 	t.walWriter.Close()
// 	_ = os.Remove(t.walFile())
// 	t.memTableIndex++
// 	t.newMemTable()
// }

func (t *Lsm) refreshMemTableLocked() {
	oldItem := &ReadOnlyMemTable{
		walFile:  t.walFile(),
		memTable: t.memTable,
	}
	t.rOnlyMemTable = append(t.rOnlyMemTable, oldItem)

	go func() {
		t.memCompactChan <- oldItem
	}()
	t.memTableIndex++
	t.newMemTable()
}

func (t *Lsm) newMemTable() {
	t.walWriter, _ = NewWalWriter(t.walFile())
	t.memTable = NewMemTable()
}
func (t *Lsm) walFile() string {
	return path.Join(t.opts.dirPath, WalFileName, fmt.Sprintf("%09d%s", t.memTableIndex, WalSuffix))
}
func (t *Lsm) sstFile(level int, seq int32) string {
	return path.Join(t.opts.dirPath, fmt.Sprintf("%02d_%06d%s", level, seq, SSTSuffix))
}

// 后台开启 进行操作 todo
func (t *Lsm) compact() {
	for {
		select {
		case item := <-t.memCompactChan:
			fmt.Println("compact?")
			t.compactMemTable(item)
		}
	}
}
func (t *Lsm) compactMemTable(item *ReadOnlyMemTable) {
	if err := t.syncMemTable(item.memTable); err != nil {
		panic(err)
	}

	t.lock.Lock()
	for i := 0; i < len(t.rOnlyMemTable); i++ {
		if t.rOnlyMemTable[i].memTable != item.memTable {
			continue
		}
		t.rOnlyMemTable = t.rOnlyMemTable[i+1:]
	}
	t.lock.Unlock()

	_ = os.Remove(item.walFile)
}
func (t *Lsm) syncMemTable(mem *MemTable) error {
	if err := t.sync(mem, 0, t.sstSeq[0].Load()); err != nil {
		return err
	}
	t.sstSeq[0].Add(1)
	return nil
}

func (t *Lsm) LoadWal() error {
	dirPath := path.Join(t.opts.dirPath, WalFileName)
	fs, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}
	if len(fs) == 0 {
		t.newMemTable()
		return nil
	}
	var ls []string
	for _, f := range fs {
		if f.IsDir() {
			continue
		}
		if path.Ext(f.Name()) == WalSuffix {
			ls = append(ls, f.Name())
		}
	}

	sort.Slice(ls, func(i, j int) bool {
		return ls[i] < ls[j]
	})
	for i, f := range ls {
		walReader, err := NewWalReader(path.Join(dirPath, f))
		if err != nil {
			return err
		}
		defer walReader.Close()

		memtable := NewMemTable()
		if err := walReader.RestoreToMemTable(memtable); err != nil {
			return err
		}
		if i == len(ls)-1 {
			t.memTable = memtable
			t.memTableIndex = getWalFileIndex(f)
			t.walWriter, _ = NewWalWriter(path.Join(dirPath, f))
		} else {
			compactItem := &ReadOnlyMemTable{
				walFile:  f,
				memTable: memtable,
			}
			t.rOnlyMemTable = append(t.rOnlyMemTable, compactItem)
			t.memCompactChan <- compactItem
		}
	}
	return nil
}
func (t *Lsm) LoadSST() error {
	fs, err := os.ReadDir(t.opts.dirPath)
	if err != nil {
		return err
	}

	var ls []string
	for _, f := range fs {
		if f.IsDir() {
			continue
		}
		if path.Ext(f.Name()) == SSTSuffix {
			ls = append(ls, f.Name())
		}
	}
	sort.Slice(ls, func(i, j int) bool {
		return ls[i] < ls[j]
	})
	for _, f := range ls {
		level, seq, err := parseSstFile(f)
		if err != nil {
			return err
		}
		sstReader, err := NewSSTReader(path.Join(t.opts.dirPath, f))
		if err != nil {
			return err
		}
		node, err := NewNode(path.Join(t.opts.dirPath, f), sstReader, t.opts, nil)
		if err != nil {
			return err
		}
		t.sstSeq[level].Store(seq)
		t.nodes[level] = append(t.nodes[level], node)
	}
	if len(ls) == 0 {
		return nil
	}
	return nil
}
func getWalFileIndex(walFile string) int {
	rawIndex := strings.Replace(walFile, WalSuffix, "", -1)
	index, _ := strconv.Atoi(rawIndex)
	return index
}

func parseSstFile(fileName string) (int, int32, error) {
	baseName := strings.TrimSuffix(fileName, SSTSuffix)
	parts := strings.Split(baseName, "_")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid file name format")
	}

	level, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid level format: %v", err)
	}

	seq, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid seq format: %v", err)
	}

	return level, int32(seq), nil
}
