package index

import (
	"BitcaskDB/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBtree()
	//插入一个边界数据
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})

	assert.True(t, res)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

}

func TestBTree_Get(t *testing.T) {
	bt := NewBtree()
	//插入一个边界数据
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)
	//测试key=nil获得相应的数据
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, uint64(100), pos1.Offset)

	//测试对一个key的重复使用获得的数据
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)
	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, uint64(3), pos2.Offset)

}

func TestBTree_Delete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)
	//删除一个nil对象
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.True(t, res3)
	//删除一个aaa对象
	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)

}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBtree()
	//1.BTree为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	//2.BTree有数据的情况
	bt1.Put([]byte("abcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	//3.BTree有多条数据的情况
	bt1.Put([]byte("cccd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("asgh"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("fakh"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("mlas"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(true)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	//4.测试 seek
	iter4 := bt1.Iterator(true)
	for iter4.Seek([]byte("bb")); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

}
