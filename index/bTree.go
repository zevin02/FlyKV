package index

import (
	"BitcaskDB/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

//BTree 索引，封装google的btree库,读操作是并发安全的，写操作并发不安全（加锁）
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex //使用读写锁保证并发安全，读取资源的时候可以多个线程并发访问，写的时候只有一个线程允许
}

//NewBtree 初始化BTree索引结构
func NewBtree() *BTree {
	return &BTree{
		//控制btree叶子节点的数量
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

//给BTRee实现这些接口，主要是调用BTree的一些功能和相关的方法
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos} //构造数据进行插入，获得指针
	bt.lock.Lock()

	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	//获得的还是一个接口
	//这个地方支持并发读取，所以不用上锁
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	//如果查找的不为空，就转化成为我们自己设计的Item
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	//会获得删除前的元素，来检查要删除的元素原来是否存在
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	//oldItem存在，则删除成功，否则就删除失败
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true

}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
}

//定义一个BTree的索引迭代器
type btreeIterator struct {
	currIndex int     //遍历到数组的哪一个下标
	reverse   bool    //是否是一个反向的遍历
	value     []*Item //key位置索引信息
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item) //将it类型转化成*item类型
		idx++
		return true
	}
	if reverse {
		//逆序存储value
		tree.Descend(saveValues)
	} else {
		//顺序存储value
		tree.Ascend(saveValues)
	}
	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		value:     values,
	}
}

//Rewind 重新回到迭代器的起点，即第一个位置
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

//Seek 根据传入的Key查找到第一个大于等于的目标key，根据从这个key开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.value), func(i int) bool {
			return bytes.Compare(bti.value[i].key, key) <= 0
		})
	} else {
		//指定比较的规则
		bti.currIndex = sort.Search(len(bti.value), func(i int) bool {
			return bytes.Compare(bti.value[i].key, key) >= 0
		})
	}
}

//Next 跳转到下一个key
func (bti *btreeIterator) Next() {
	bti.currIndex++
}

//Valid 是否有效，即时有已经遍历完了所有的Key，用来退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.value)
}

//Key 当前遍历位置的key数据
func (bti *btreeIterator) Key() []byte {
	return bti.value[bti.currIndex].key
}

//Value 当前遍历位置的value数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.value[bti.currIndex].pos

}

//Close 关闭迭代器，释放相应的资源
func (bti *btreeIterator) Close() {
	bti.value = nil
}
