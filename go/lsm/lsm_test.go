package lsm

import (
	"testing"
	"time"

	"github.com/xia-Sang/lsm_go/util"

	"github.com/stretchr/testify/assert"
)

func TestMemTable_Set(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db := NewLsm(opts)
	for i := range 100 {
		key, value := util.GenerateKeyString(i), util.GenerateValueString(12)
		err := db.Put(key, value)
		assert.Nil(t, err)
	}
	for i := range 109 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)

		val, err := db.Query(key)
		if i < 100 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
		t.Logf("(%s:%s)", key, val)
	}
}
func (t *Lsm) Show() {
	t.memTable.Show()
	for i := len(t.rOnlyMemTable) - 1; i > 0; i-- {
		t.rOnlyMemTable[i].memTable.Show()
	}
}
func TestSST(t *testing.T) {
	s, err := NewSSTReader("./data/000000000.sst")
	assert.Nil(t, err)
	nb := NewMemTable()
	s.Restore(nb)
	nb.Show()
}
func TestMemTable_Set1(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db := NewLsm(opts)
	m := map[string]string{}
	for i := range 100 {
		key, value := util.GenerateKeyString(i), util.GenerateValueString(12)
		err := db.Put(key, value)
		m[key] = value
		assert.Nil(t, err)
	}

	for i := range 109 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)

		val, err := db.Query(key)
		if i < 100 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
	}
	for i := range 90 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)
		err := db.Delete(key)
		assert.Nil(t, err)
	}
	for i := range 109 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)

		val, err := db.Query(key)
		if i >= 90 && i < 100 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
			assert.Equal(t, val, m[key])
		} else {
			assert.Equal(t, err, ErrorNotExist)
		}
		t.Logf("(%s:%s:%v)", key, val, err)
	}
}
func TestMemTable_Set2(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db := NewLsm(opts)
	m := map[string]string{}
	for i := range 500 {
		key, value := util.GenerateKeyString(i), util.GenerateValueString(12)
		err := db.Put(key, value)
		m[key] = value
		assert.Nil(t, err)
	}
	time.Sleep(2 * time.Second)
	for i := range 509 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)

		val, err := db.Query(key)
		if i < 500 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
		} else {
			assert.NotNil(t, err)
			assert.Equal(t, err, ErrorNotExist)
		}
	}
	time.Sleep(2 * time.Second)
	for i := range 400 {
		key, _ := util.GenerateKeyString(100+i), util.GenerateValueString(12)
		err := db.Delete(key)
		assert.Nil(t, err)
	}
	time.Sleep(2 * time.Second)
	for i := range 509 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)

		val, err := db.Query(key)
		if i < 100 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
			assert.Equal(t, val, m[key])
		} else {
			assert.Equal(t, err, ErrorNotExist)
		}
		//t.Logf("(%s:%s:%v)", key, val, err)
	}
	for i, nodes := range db.nodes {
		for j, node := range nodes {
			t.Log(i, j, node)
		}
	}
}

func TestMemTable_Set3(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db := NewLsm(opts)
	m := map[string]string{}
	for i := range 5900 {
		key, value := util.GenerateKeyString(i), util.GenerateValueString(12)
		err := db.Put(key, value)
		m[key] = value
		assert.Nil(t, err)
	}
	for i := 100; i < 5900; i++ {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)
		err := db.Delete(key)
		assert.Nil(t, err)
	}

	for i := range 5900 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)
		val, err := db.Query(key)
		if i < 100 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
			assert.Equal(t, val, m[key])
		} else {
			assert.Equal(t, err, ErrorNotExist)
		}
		//t.Logf("(%s:%s:%v)", key, val, err)
	}
	// key, _ := util.GenerateKeyString(5900), util.GenerateValueString(12)
	// val, err := db.Query(key)
	// t.Log(key, val, err)
	// t.Log(db.nodes)
	// for i, nodes := range db.nodes {
	// 	for j, node := range nodes {
	// 		t.Log(i, j, node.fileName)
	// 	}
	// }
	// t.Log(db.nodes)
	// t.Log("db.sstSeq", db.sstSeq)
}
func TestMemTable_Get3(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db := NewLsm(opts)
	for i := range 809 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)

		val, err := db.Query(key)
		if i < 100 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
			t.Logf("(%s:%s:%v)", key, val, err)
		} else {
			assert.Equal(t, err, ErrorNotExist)
		}
	}
	t.Log(db.nodes)
	t.Log("db.sstSeq", db.sstSeq)
}
func TestMemTable_Get0(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db := NewLsm(opts)

	for i := range 209 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)

		val, err := db.Query(key)
		if i < 100 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
		} else {
			assert.Equal(t, err, ErrorNotExist)
		}
		//t.Logf("(%s:%s:%v)", key, val, err)
	}
}
func TestMemTable_Set4(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db := NewLsm(opts)
	//m := map[string]string{}
	//for i := range 200 {
	//	key, value := util.GenerateKeyString(i), util.GenerateValueString(12)
	//	err := db.Put(key, value)
	//	m[key] = value
	//	assert.Nil(t, err)
	//}

	for i := range 200 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)
		value, err := db.Query(key) //表示wal里面的数据是ok的
		t.Log(key, value, err)
	}
	for i, nodes := range db.nodes {
		for j := len(nodes) - 1; j >= 0; j-- {
			t.Log(nodes[j].startKey, nodes[j].endKey, i, j)
			nodes[j].Show()
		}
	}
}
func TestMemTable_Set5(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db := NewLsm(opts)
	t.Log(db)
}
func TestMemTable_Get1(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db := NewLsm(opts)

	for i := range 209 {
		key, _ := util.GenerateKeyString(i), util.GenerateValueString(12)

		val, err := db.Query(key)
		if i < 100 {
			assert.Nil(t, err)
			assert.Equal(t, len(val), 12)
		} else {
			assert.Equal(t, err, ErrorNotExist)
		}
		//t.Logf("(%s:%s:%v)", key, val, err)
	}
}
func TestMemTable_Get4(t *testing.T) {
	opts, err := NewOptions("./data")
	assert.Nil(t, err)
	db := NewLsm(opts)
	t.Log(db.nodes)
}
