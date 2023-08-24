package index

import (
	"FlexDB/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBtree()
	//插入一个边界数据
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	//前面没有数据，所以旧的数据应该是空
	assert.Nil(t, res)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.NotNil(t, res3)
	assert.Equal(t, uint32(1), res3.Fid)
	assert.Equal(t, uint64(2), res3.Offset)

}

func TestBTree_Get(t *testing.T) {
	bt := NewBtree()
	//插入一个边界数据
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res)
	//测试key=nil获得相应的数据
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, uint64(100), pos1.Offset)

	//测试对一个key的重复使用获得的数据
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Equal(t, uint32(1), res3.Fid)
	assert.Equal(t, uint64(2), res3.Offset)
	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, uint64(3), pos2.Offset)

}

func TestBTree_Delete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	//删除一个nil对象
	_, ok1 := bt.Delete(nil)
	assert.True(t, ok1)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)
	//删除一个aaa对象
	res4, ok2 := bt.Delete([]byte("aaa"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(22), res4.Fid)
	assert.Equal(t, uint64(33), res4.Offset)

}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBtree()
	//1.BTree为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())
	iter1.Close()

	//2.BTree有数据的情况
	bt1.Put([]byte("abcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())
	iter2.Close()

	//3.BTree有多条数据的情况
	bt1.Put([]byte("cccd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("asgh"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("fakh"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("mlas"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(true)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close() //新开一个的话，前面的就要关掉

	//4.测试 seek
	iter4 := bt1.Iterator(false)
	for iter4.Seek([]byte("bb")); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}
	//如果已经无效了，就不能再使用
	iter4.Seek([]byte("bb"))
	assert.False(t, iter4.Valid())
	//
	iter4.Rewind()
	iter4.Seek([]byte("bb"))
	key1 := iter4.Key()
	t.Log(string(iter4.Key()))
	iter4.Next()
	iter4.Seek([]byte("bb"))
	key2 := iter4.Key()
	assert.NotEqual(t, key1, key2)
	t.Log(string(iter4.Key()))
	iter4.Next()
	//TODO 如果是按大到小遍历的，使用seek会超出界限，需要修复bug,所有的索引都需要
	iter4.Seek([]byte("bb"))
	t.Log(string(iter4.Key()))
}
