package lsm

import (
	"github.com/xia-Sang/lsm_go/util"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWal_Writer(t *testing.T) {
	walWriter, err := NewWalWriter("1.wal")
	assert.Nil(t, err)
	m := NewMemTable()
	dict := map[string]string{}
	for i := range 100 {
		key, value := util.GenerateKeyString(i), util.GenerateValueString(12)
		r := &Record{
			Key:   key,
			Value: value,
			RType: RecordUpdate,
		}
		m.Set(r)
		dict[key] = value
		_, err := walWriter.Write(r)
		assert.Nil(t, err)
	}
	walWriter.Close()

	walReader, err := NewWalReader("1.wal")
	assert.Nil(t, err)
	nb := NewMemTable()
	walReader.RestoreToMemTable(nb)
	for i := range 100 {
		key := util.GenerateKeyString(i)
		re := nb.Query(key)
		assert.Equal(t, re.Value, dict[key])
	}
	nb.Show()
}
