package index

import (
	"FlexDB/data"
	"bytes"
	"github.com/google/btree"
)

//TODO使用哈希来构建使用多个索引，减小索引锁的粒度

//Indexer 定义一个抽象索引接口(内存索引)
//Get拿到索引的位置信息
type Indexer interface {
	//Put 向索引中添加key对应的位置信息,并且将旧的数据返回出去
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	//Get 根据Key获得相应的位置信息
	Get(key []byte) *data.LogRecordPos
	//Delete 根据key删除位置数据，并且将旧的数据返回出去
	Delete(key []byte) (*data.LogRecordPos, bool)
	//Iterator 索引迭代器
	Iterator(reverse bool) Iterator
	//Size 索引中保存的数据个数
	Size() int
	//Close 关闭索引,避免阻塞，以及释放资源
	Close() error
}
type IndexType = int8

const (
	//Btree索引
	Btree IndexType = iota
	//ART自适应基数树
	ART
	BPT
)

//NewIndex 工厂函数，用来创建不同类新的索引
func NewIndex(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBtree()
	case ART:
		return NewART()
	case BPT:
		return NewBPT(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

//Item BTree中使用到了Item的抽象方法,所以这里需要实现一个接口来实现相应的方法,插入到btree的时候实际上就是插入这个数据结构
type Item struct {
	key []byte             //key
	pos *data.LogRecordPos //对应的数据
}

//Less里面是btree的Item对象,该方法按照从小到大的顺序,=-1说明第一个key小于第二个key
func (ai *Item) Less(bi btree.Item) bool {
	//bi.(*Item)是将bi转化成为*Item类型，类型断言，就是将接口类型转化成为具体的类型
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

type Iterator interface {
	//Rewind 重新回到迭代器的起点，即第一个位置
	Rewind()
	//Seek 根据传入的Key查找到第一个大于等于的目标key，根据从这个key开始遍历
	Seek(key []byte)
	//Next 跳转到下一个key
	Next()
	//Valid 是否有效，即时有已经遍历完了所有的Key，用来退出遍历
	Valid() bool
	//Key 当前遍历位置的key数据
	Key() []byte
	//Value 当前遍历位置的value数据
	Value() *data.LogRecordPos
	//Close 关闭迭代器，释放相应的资源
	Close()
}
