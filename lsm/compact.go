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

	// 实现随机选取连续节点
	// var fileNames []string
	// var mergeNodes []*Node
	// if len(t.nodes[level]) > 0 {
	// 	mid := len(t.nodes[level]) / 2
	// 	start := mid - rand.Intn(mid)
	// 	end := mid + rand.Intn(len(t.nodes[level])-mid-1)
	// 	mergeNodes = t.nodes[level][start:end]
	// 	for _, node := range mergeNodes {
	// 		fileNames = append(fileNames, node.fileName)
	// 	}
	// }
	// return mergeNodes, fileNames
}

// 查看这层是否进行合并操作
func (t *Lsm) checkLevelOverflow(level int) bool {
	if level == t.opts.maxLevel {
		return false
	}
	// fmt.Println("checkLevelOverflow", level, len(t.nodes[level]), t.opts.maxLevelNum)
	return len(t.nodes[level]) >= t.opts.maxLevelNum
}

// 尝试进行层次合并
func (t *Lsm) tryCompactLevel(level int) {
	if !t.checkLevelOverflow(level) {
		return
	}
	go func() {
		t.levelCompactChan <- level
	}()
}

func (t *Lsm) compactNodesLevel(level int) {
	mem := NewMemTable()
	mergeNode, fileNames := t.getMergeBlock(level)
	for _, node := range mergeNode {
		m, err := node.Merge()
		if err != nil {
			panic(err)
		}
		mem.Merge(m)
		if err := node.sstReader.Close(); err != nil {
			// return err
			panic(err)
		}
	}

	if err := t.sync(mem, level+1, t.sstSeq[level+1].Load()); err != nil {
		// return err
		panic(err)
	}
	t.sstSeq[level+1].Add(1)
	// 清理旧的节点和文件

	for _, filename := range fileNames {
		_ = os.Remove(filename)
	}
	t.nodes[level] = nil

	t.tryCompactLevel(level + 1)
	// return nil
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

	t.tryCompactLevel(level)
	return nil
}
