package lsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// 实现record记录信息
type Record struct {
	Key   string     // key
	Value string     // value
	RType RecordType // 类型信息
}

func (r *Record) Show() string {
	return fmt.Sprintf("%s:%v:%v", r.Key, r.Value, r.RType)
}

type RecordType uint8 //record类型信息

const (
	RecordUpdate RecordType = iota //数据更新
	RecordDelete                   //数据删除
)

// Bytes 将数据转为 bytes进行存储
func (r *Record) Bytes() (int, []byte) {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, r.RType)
	binary.Write(buf, binary.LittleEndian, uint32(len(r.Key)))
	buf.Write([]byte(r.Key))
	if r.RType == RecordUpdate {
		binary.Write(buf, binary.LittleEndian, uint32(len(r.Value)))
		buf.Write([]byte(r.Value))
	}
	return buf.Len(), buf.Bytes()
}

// Restore 将数据进行恢复处理
func (r *Record) Restore(data []byte) {
	var n uint32
	buf := bytes.NewBuffer(data)
	binary.Read(buf, binary.LittleEndian, &r.RType)
	binary.Read(buf, binary.LittleEndian, &n)
	r.Key = string(buf.Next(int(n)))
	if r.RType == RecordUpdate {
		binary.Read(buf, binary.LittleEndian, &n)
		r.Value = string(buf.Next(int(n)))
	}
	buf = nil
}
