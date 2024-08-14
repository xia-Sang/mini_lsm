package lsm

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

// SSTableMetaInfo 目前是没有写入过滤器信息
type SSTableMetaInfo struct {
	DataOffset    uint64 // data segment position
	DataLength    uint64 // data segment length
	IndexOffset   uint64 // sparse index position
	IndexLength   uint64 // sparse index length
	BlockKeyNum   uint16 // each block contains N keys
	TableBlockNum uint16 // each table contains N blocks
	Version       uint32 // data version
}

// SizeOfMetaInfo 8+8+8+8+2+2+4=40
var SizeOfMetaInfo = unsafe.Sizeof(SSTableMetaInfo{})

func (mi *SSTableMetaInfo) Bytes() []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.LittleEndian, mi.DataOffset)
	binary.Write(buf, binary.LittleEndian, mi.DataLength)
	binary.Write(buf, binary.LittleEndian, mi.IndexOffset)
	binary.Write(buf, binary.LittleEndian, mi.IndexLength)
	binary.Write(buf, binary.LittleEndian, mi.BlockKeyNum)
	binary.Write(buf, binary.LittleEndian, mi.TableBlockNum)
	binary.Write(buf, binary.LittleEndian, mi.Version)
	return buf.Bytes()
}
func (mi *SSTableMetaInfo) Restore(data []byte) {
	buf := bytes.NewBuffer(data)
	binary.Read(buf, binary.LittleEndian, &mi.DataOffset)
	binary.Read(buf, binary.LittleEndian, &mi.DataLength)
	binary.Read(buf, binary.LittleEndian, &mi.IndexOffset)
	binary.Read(buf, binary.LittleEndian, &mi.IndexLength)
	binary.Read(buf, binary.LittleEndian, &mi.BlockKeyNum)
	binary.Read(buf, binary.LittleEndian, &mi.TableBlockNum)
	binary.Read(buf, binary.LittleEndian, &mi.Version)
	buf = nil
}
