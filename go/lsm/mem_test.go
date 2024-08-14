package lsm

import (
	"github.com/xia-Sang/lsm_go/util"
	"testing"
)

func TestMemTable_Get(t *testing.T) {
	m := NewMemTable()
	for i := range 100 {
		key, value := util.GenerateKeyString(i), util.GenerateValueString(12)
		r := &Record{
			Key:   key,
			Value: value,
			RType: RecordUpdate,
		}
		m.Set(r)
	}

	for i := range 100 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)

		value := m.Query(key)
		t.Log(value)
	}
}
