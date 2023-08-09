package index

import (
	"BitcaskDB/data"
	"github.com/google/btree"
	"sync"
)

//BTree索引，封装google的btree库,读操作是并发安全的，写操作并发不安全（加锁）
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex //使用读写锁保证并发安全，读取资源的时候可以多个线程并发访问，写的时候只有一个线程允许
}

//NewBtree初始化BTree索引结构
func NewBtree() *BTree {
	return &BTree{
		//控制btree叶子节点的数量
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

//给BTRee实现这些接口，主要是调用BTree的一些功能和相关的方法
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos} //构造数据进行插入，获得指针
	bt.lock.Lock()

	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
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

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	//会获得删除前的元素，来检查要删除的元素原来是否存在
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true

}
