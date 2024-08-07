package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// SparseIndex 实现支持稀疏索引
type SparseIndex struct {
	MinKey     string //key的数值
	MaxKey     string //key的数值
	BlockIndex uint32 //block的索引信息
	DataOffset uint32 //数据的开始
	FileName   string //文件名称
	// 此处是需要支持过滤器的
}

// Show 测试函数
func (si *SparseIndex) Show() string {
	return fmt.Sprintf("%s->%s-%d-%d-%s", si.MinKey, si.MaxKey, si.BlockIndex, si.DataOffset, si.FileName)
}
func (si *SparseIndex) Bytes() (int, []byte) {
	buf := bytes.NewBuffer(nil)

	if err := binary.Write(buf, binary.LittleEndian, si.BlockIndex); err != nil {
		return 0, nil
	}
	if err := binary.Write(buf, binary.LittleEndian, si.DataOffset); err != nil {
		return 0, nil
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(si.MinKey))); err != nil {
		return 0, nil
	}
	buf.Write([]byte(si.MinKey))
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(si.MaxKey))); err != nil {
		return 0, nil
	}
	buf.Write([]byte(si.MaxKey))
	return buf.Len(), buf.Bytes()
}
func (si *SparseIndex) Restore(data []byte) {
	var n uint32
	buf := bytes.NewBuffer(data)
	binary.Read(buf, binary.LittleEndian, &si.BlockIndex)
	binary.Read(buf, binary.LittleEndian, &si.DataOffset)
	binary.Read(buf, binary.LittleEndian, &n)
	si.MinKey = string(buf.Next(int(n)))
	binary.Read(buf, binary.LittleEndian, &n)
	si.MaxKey = string(buf.Next(int(n)))
	buf = nil
}
