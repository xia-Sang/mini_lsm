package lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/pierrec/lz4"
)

type SSTWriter struct {
	opts     *Options
	dest     *os.File      // sstable 对应的磁盘文件
	lz4Buf   *bytes.Buffer // 缓冲区
	dataBuf  *bytes.Buffer // 缓冲区
	fileName string
}

func NewSSTWriter(fileName string, opts *Options) (*SSTWriter, error) {
	fp, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &SSTWriter{
		dest:     fp,
		opts:     opts,
		lz4Buf:   bytes.NewBuffer(nil),
		dataBuf:  bytes.NewBuffer(nil),
		fileName: fileName,
	}, nil
}
func (w *SSTWriter) Close() {
	_ = w.dest.Close()
}

// 对数据进行落盘操作
func (w *SSTWriter) syncMemTable(mem *MemTable) error {
	//实现一个最简单的数据落盘 直接将所有数据落盘即可
	memData := mem.Bytes()
	writer := lz4.NewWriter(w.lz4Buf)
	if _, err := writer.Write(memData); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	io.Copy(w.dest, w.lz4Buf)
	return nil
}
func (w *SSTWriter) SyncMemTable(mem *MemTable) ([]*SparseIndex, error) {
	var sparseIndex []*SparseIndex
	records := mem.GetRecords()
	divRecs := divRecords(records, w.opts.tableNum)

	offset := 0
	for i, res := range divRecs {
		temp := bytes.NewBuffer(nil)
		for _, re := range res {
			length, data := re.Bytes()
			count, err := temp.Write(data)
			if err != nil {
				return nil, fmt.Errorf("failed to write record data: %w", err)
			}
			if length != count {
				return nil, errors.New("writer error: length mismatch")
			}
		}

		w.lz4Buf.Reset()
		lz4w := lz4.NewWriter(w.lz4Buf)
		if _, err := lz4w.Write(temp.Bytes()); err != nil {
			return nil, fmt.Errorf("failed to compress data: %w", err)
		}
		if err := lz4w.Close(); err != nil {
			return nil, fmt.Errorf("failed to close lz4 writer: %w", err)
		}

		blockSize := w.lz4Buf.Len()
		if err := binary.Write(w.dest, binary.LittleEndian, uint32(blockSize)); err != nil {
			return nil, fmt.Errorf("failed to write block size: %w", err)
		}
		n, err := io.Copy(w.dest, w.lz4Buf)
		if err != nil {
			return nil, fmt.Errorf("failed to write compressed data: %w", err)
		}
		if n != int64(blockSize) {
			return nil, fmt.Errorf("write compress data err: data length err %d,%d", n, int64(blockSize))
		}

		sparseIndex = append(sparseIndex, &SparseIndex{
			MinKey:     res[0].Key,
			MaxKey:     res[len(res)-1].Key,
			BlockIndex: uint32(i),
			DataOffset: uint32(offset),
			FileName:   w.fileName,
		})
		offset += blockSize + 4
	}

	metaInfo := SSTableMetaInfo{
		DataOffset:    0,
		DataLength:    uint64(offset),
		IndexOffset:   uint64(offset),
		BlockKeyNum:   uint16(len(records)),
		TableBlockNum: uint16(w.opts.tableNum),
		Version:       0,
	}
	for i := range sparseIndex {
		n, body := sparseIndex[i].Bytes()
		if err := binary.Write(w.dest, binary.LittleEndian, uint32(n)); err != nil {
			return nil, fmt.Errorf("failed to write sparse index size: %w", err)
		}
		if _, err := w.dest.Write(body); err != nil {
			return nil, fmt.Errorf("failed to write sparse index body: %w", err)
		}
		metaInfo.IndexLength += uint64(n) + 4
	}

	if _, err := w.dest.Write(metaInfo.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to write meta info: %w", err)
	}

	if err := w.dest.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync destination file: %w", err)
	}
	if err := w.dest.Close(); err != nil {
		return nil, fmt.Errorf("failed to close destination file: %w", err)
	}

	return sparseIndex, nil
}

// divRecords 对于records进行划分 步长为nums
func divRecords(records []*Record, nums int) [][]*Record {
	var result [][]*Record
	for i := 0; i < len(records); i += nums {
		end := i + nums
		if end > len(records) {
			end = len(records)
		}
		result = append(result, records[i:end])
	}
	return result
}

type SSTReader struct {
	dest     *os.File      // sstable 对应的磁盘文件
	lz4Buf   *bytes.Buffer // 缓冲区
	dataBuf  *bytes.Buffer // 缓冲区
	fileName string        // sstable 对应的文件名
}

func (r *SSTReader) Close() error {
	return r.dest.Close()
}
func NewSSTReader(fileName string) (*SSTReader, error) {
	fp, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &SSTReader{
		dest:     fp,
		lz4Buf:   bytes.NewBuffer(nil),
		dataBuf:  bytes.NewBuffer(nil),
		fileName: fileName,
	}, nil
}
func (r *SSTReader) Restore(mem *MemTable) error {
	reader := lz4.NewReader(r.dest)
	if _, err := r.dataBuf.ReadFrom(reader); err != nil {
		return err
	}
	// 会得到数据 这部分的数据 需要恢复到memtable之中
	if err := mem.Restore(r.dataBuf.Bytes()); err != nil {
		return err
	}
	return nil
}
func (r *SSTReader) ReadBlock() ([]*SparseIndex, error) {
	var ans []*SparseIndex
	r.dest.Seek(-int64(SizeOfMetaInfo), io.SeekEnd)
	data := make([]byte, SizeOfMetaInfo)
	nn, err := r.dest.Read(data)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if nn != int(SizeOfMetaInfo) {
		return nil, fmt.Errorf("read metainfo length error: %d", nn)
	}

	metaInfo := new(SSTableMetaInfo)
	metaInfo.Restore(data)

	r.dest.Seek(int64(metaInfo.IndexOffset), io.SeekStart)
	var n uint32
	var offset uint64
	for offset < metaInfo.IndexLength {
		if err = binary.Read(r.dest, binary.LittleEndian, &n); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if n == 0 {
			break
		}
		if cap(data) < int(n) {
			data = make([]byte, n)
		} else {
			data = data[:n]
		}
		if _, err = r.dest.Read(data); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		sparseIndex := &SparseIndex{FileName: r.fileName}
		sparseIndex.Restore(data)
		ans = append(ans, sparseIndex)
		offset += 4 + uint64(n)
	}

	return ans, nil
}

// 读取对应的block进行数据查找
func (r *SSTReader) readSSTBlock(blockOffset uint32) (*MemTable, error) {
	var n uint32

	if _, err := r.dest.Seek(int64(blockOffset), io.SeekStart); err != nil {
		return nil, err
	}

	if err := binary.Read(r.dest, binary.LittleEndian, &n); err != nil {
		return nil, err
	}
	data := make([]byte, n)
	count, err := io.ReadFull(r.dest, data)
	if err != nil {
		return nil, err
	}
	if count != int(n) {
		return nil, fmt.Errorf("read metainfo length error: %d", count)
	}

	lz4r := lz4.NewReader(bytes.NewReader(data))
	var decompressedData bytes.Buffer
	if _, err := io.Copy(&decompressedData, lz4r); err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}
	buf := &decompressedData
	mem := NewMemTable()
	var (
		rType RecordType
	)
	for {

		if err := binary.Read(buf, binary.LittleEndian, &rType); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if err := binary.Read(buf, binary.LittleEndian, &n); err != nil {
			return nil, err
		}
		Key := string(buf.Next(int(n)))
		record := &Record{Key: Key, RType: rType}
		if rType == RecordUpdate {
			if err := binary.Read(buf, binary.LittleEndian, &n); err != nil {
				return nil, err
			}
			value := string(buf.Next(int(n)))
			record.Value = value
		}
		mem.Set(record)
	}

	return mem, nil
}
