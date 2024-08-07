package lsm

import (
	"fmt"
	"github.com/xia-Sang/lsm_go/util"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSSTReader_case1(t *testing.T) {
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
	}
	opts, err := NewOptions("./test")
	assert.Nil(t, err)
	w, err := NewSSTWriter("1.sst", opts)
	assert.Nil(t, err)
	err = w.syncMemTable(m)
	assert.Nil(t, err)
	w.Close()

	r, err := NewSSTReader("1.sst")
	assert.Nil(t, err)
	nb := NewMemTable()
	r.Restore(nb)
	for i := range 100 {
		key := util.GenerateKeyString(i)
		re := nb.Query(key)
		assert.Equal(t, re.Value, dict[key])
	}
}
func TestNewSSTReader(t *testing.T) {
	//for i := 0; i < 17; i++ {
	//	r, err := NewSSTReader(fmt.Sprintf("./data/000000_%02d.sst", i))
	//	assert.Nil(t, err)
	//	index, err := r.ReadBlock()
	//	assert.Nil(t, err)
	//	for _, v := range index {
	//		m, err := r.readSSTBlock(v.DataOffset)
	//		assert.Nil(t, err)
	//		m.Show()
	//	}
	//}
	r, err := NewSSTReader(fmt.Sprintf("./data/000000_%02d.sst", 16))
	assert.Nil(t, err)
	index, err := r.ReadBlock()
	assert.Nil(t, err)
	for _, v := range index {
		m, err := r.readSSTBlock(v.DataOffset)
		assert.Nil(t, err)
		m.Show()
	}
}
func TestSSTReader_Restore(t *testing.T) {
	m := NewMemTable()
	dict := map[string]string{}
	for i := range 90 {
		key, value := util.GenerateKeyString(i), util.GenerateValueString(12)
		r := &Record{
			Key:   key,
			Value: value,
			RType: RecordUpdate,
		}
		m.Set(r)
		dict[key] = value
	}
	opts, err := NewOptions("./test")
	assert.Nil(t, err)
	w, err := NewSSTWriter("2.sst", opts)
	assert.Nil(t, err)
	_, err = w.SyncMemTable(m)
	assert.Nil(t, err)
	w.Close()

	r, err := NewSSTReader("2.sst")
	assert.Nil(t, err)
	sparseIndex, err := r.ReadBlock()
	assert.Nil(t, err)
	for k, v := range sparseIndex {
		t.Logf("(%d:%s)\n", k, v.Show())
	}
	t.Log(sparseIndex)
}
func TestSSTReader_Restore0(t *testing.T) {
	r, err := NewSSTReader("2.sst")
	assert.Nil(t, err)
	sparseIndex, err := r.ReadBlock()
	assert.Nil(t, err)
	for k, v := range sparseIndex {
		t.Logf("(%d:%s)\n", k, v.Show())
		m, err := r.readSSTBlock(v.DataOffset)
		assert.Nil(t, err)
		m.Show()
	}
	t.Log(sparseIndex)
}
func TestSSTReader_Restore1(t *testing.T) {
	m := NewMemTable()
	dict := map[string]string{}
	for i := range 99 {
		key, value := util.GenerateKeyString(i), util.GenerateValueString(12)
		r := &Record{
			Key:   key,
			Value: value,
			RType: RecordUpdate,
		}
		m.Set(r)
		dict[key] = value
	}
	for i := range 78 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)
		r := &Record{
			Key:   key,
			RType: RecordDelete,
		}
		m.Set(r)
	}
	opts, err := NewOptions("./test")
	assert.Nil(t, err)
	w, err := NewSSTWriter("3.sst", opts)
	assert.Nil(t, err)
	_, err = w.SyncMemTable(m)
	assert.Nil(t, err)
	w.Close()

	r, err := NewSSTReader("3.sst")
	assert.Nil(t, err)
	sparseIndex, err := r.ReadBlock()
	assert.Nil(t, err)
	for k, v := range sparseIndex {
		t.Logf("(%d:%s)\n", k, v.Show())
	}
	t.Log(sparseIndex)
}
func TestSSTReader_Restore2(t *testing.T) {
	r, err := NewSSTReader("3.sst")
	assert.Nil(t, err)
	sparseIndex, err := r.ReadBlock()
	assert.Nil(t, err)
	for k, v := range sparseIndex {
		t.Logf("(%d:%s)\n", k, v.Show())
		m, err := r.readSSTBlock(v.DataOffset)
		assert.Nil(t, err)
		m.Show()
	}
	t.Log(sparseIndex)
}
