package lsm

import (
	"os"
)

// 获取需要合并的nodes
func (t *Lsm) getMergeBlock(level int) ([]*Node, []string) {
	var fileNames []string
	for _, node := range t.nodes[level] {
		fileNames = append(fileNames, node.fileName)
	}
	return t.nodes[level], fileNames
}

// 查看这层是否进行合并操作
func (t *Lsm) checkLevelOverflow(level int) bool {
	if level == t.opts.maxLevel {
		return false
	}
	return len(t.nodes[level]) >= t.opts.maxLevelNum
}

// 需要进行层次合并
func (t *Lsm) compactLevel(level int) error {
	if !t.checkLevelOverflow(level) {
		return nil
	}
	if err := t.getAllData(level); err != nil {
		return err
	}
	if err := t.compactLevel(level + 1); err != nil {
		return err
	}
	return nil
}

// 获取所有数据并合并到下一个层次
func (t *Lsm) getAllData(level int) error {
	mem := NewMemTable()
	mergeNode, fileNames := t.getMergeBlock(level)
	for _, node := range mergeNode {
		m, err := node.Merge()
		if err != nil {
			return err
		}
		mem.Merge(m)
		node.sstReader.Close()
	}

	if err := t.sync(mem, level+1, t.sstSeq[level+1].Load()); err != nil {
		return err
	}
	t.sstSeq[level+1].Add(1)
	// 清理旧的节点和文件
	for _, name := range fileNames {
		_ = os.Remove(name)
	}
	t.nodes[level] = []*Node{}
	return nil
}

// 将 MemTable 同步到磁盘
func (t *Lsm) sync(mem *MemTable, level int, seq int32) error {
	// 生成 SST 文件名
	sstFileName := t.sstFile(level, seq)
	sstWriter, err := NewSSTWriter(sstFileName, t.opts)
	if err != nil {
		return err
	}
	defer sstWriter.Close()

	// 将 MemTable 落盘
	sparseIndex, err := sstWriter.SyncMemTable(mem)
	if err != nil {
		return err
	}

	// 创建 SSTReader
	sstReader, err := NewSSTReader(sstFileName)
	if err != nil {
		return err
	}

	// 创建新节点并添加到节点列表
	node, err := NewNode(sstFileName, sstReader, t.opts, sparseIndex)
	if err != nil {
		return err
	}
	t.nodes[level] = append(t.nodes[level], node)

	// 检查并进行下一个层次的合并操作
	if err := t.compactLevel(level); err != nil {
		return err
	}

	return nil
}
