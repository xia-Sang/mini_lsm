package lsm

import (
	"encoding/binary"
	"io"
	"os"
)

type WalWriter struct {
	fileName string
	dest     *os.File
}

func (w *WalWriter) WriteAndSync(record *Record) error {
	if _, err := w.Write(record); err != nil {
		return err
	}
	return w.dest.Sync()
}

func NewWalWriter(fileName string) (*WalWriter, error) {
	fp, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &WalWriter{fileName: fileName, dest: fp}, nil
}

func (w *WalWriter) Write(record *Record) (int, error) {
	n, body := record.Bytes()
	err := binary.Write(w.dest, binary.LittleEndian, uint32(n))
	if err != nil {
		return 0, err
	}
	if _, err := w.dest.Write(body); err != nil {
		return 0, err
	}
	return n, nil
}
func (w *WalWriter) Close() {
	_ = w.dest.Close()
}

type WalReader struct {
	fileName string
	src      *os.File
}

func (w *WalReader) Close() {
	_ = w.src.Close()
}

func NewWalReader(fileName string) (*WalReader, error) {
	fp, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &WalReader{
		fileName: fileName,
		src:      fp,
	}, nil
}

// RestoreToMemTable 可以将数据恢复到memtable之中
func (w *WalReader) RestoreToMemTable(mem *MemTable) error {
	records, err := readWal(w.src)
	if err != nil {
		return err
	}
	for _, v := range records {
		mem.Set(v)
	}
	return nil
}

// 读取wal信息返回[]*record即可
func readWal(f io.ReadSeeker) ([]*Record, error) {
	var n uint32
	var err error
	var data []byte
	var records []*Record
	for {
		if err = binary.Read(f, binary.LittleEndian, &n); err != nil {
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

		if _, err = f.Read(data); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		record := &Record{}
		record.Restore(data)
		records = append(records, record)
	}

	return records, nil
}
