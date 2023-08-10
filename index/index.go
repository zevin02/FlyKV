package index

import (
	"BitcaskDB/data"
	"bytes"
	"github.com/google/btree"
)

//Indexer 定义一个抽象索引接口(内存索引)
//Get拿到索引的位置信息
type Indexer interface {
	//Put向索引中添加key对应的位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	//Get根据Key获得相应的位置信息
	Get(key []byte) *data.LogRecordPos
	//根据key在位置数据
	Delete(key []byte) bool
}
type IndexType = int8

const (
	//Btree索引
	Btree IndexType = itoa + 1
	//ART自适应基数树
	ART
)

//初始化类型索引
func NewIndex(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBtree()
	case ART:
		return nil
	default:
		panic("unsupported index type")
	}
}

//BTree中使用到了Item的抽象方法,所以这里需要实现一个接口来实现相应的方法,插入到btree的时候实际上就是插入这个数据结构
type Item struct {
	key []byte             //key
	pos *data.LogRecordPos //对应的数据
}

//Less里面是btree的Item对象,该方法按照从小到大的顺序,=-1说明第一个key小于第二个key
func (ai *Item) Less(bi btree.Item) bool {
	//bi.(*Item)是将bi转化成为*Item类型，类型断言，就是将接口类型转化成为具体的类型
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
