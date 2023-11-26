package mvcc

import (
	"bytes"
	"github.com/google/btree"
	"sync"
)

//ItemM BTree中使用到了Item的抽象方法,所以这里需要实现一个接口来实现相应的方法,插入到btree的时候实际上就是插入这个数据结构
type ItemM struct {
	key []byte    //key
	ki  *KeyIndex //对应的数据
}

//Less里面是btree的Item对象,该方法按照从小到大的顺序,=-1说明第一个key小于第二个key
func (ai *ItemM) Less(bi btree.Item) bool {
	//bi.(*ItemM)是将bi转化成为*Item类型，类型断言，就是将接口类型转化成为具体的类型
	return bytes.Compare(ai.key, bi.(*ItemM).key) == -1
}

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex //使用读写锁保证并发安全，读取资源的时候可以多个线程并发访问，写的时候只有一个线程允许
}

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

//Get 在当前的btree中通过key获得相应的KeyIndex数据结构
func (bt *BTree) Get(key []byte) *KeyIndex {
	it := &ItemM{key: key}
	//获得的还是一个接口
	//这个地方支持并发读取，所以不用上锁
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	//如果查找的不为空，就转化成为我们自己设计的Item

	return btreeItem.(*ItemM).ki
}

//Put 给btree中插入一个key的数据和其对应的keyIndex信息
func (bt *BTree) Put(key []byte, ki *KeyIndex) *KeyIndex {
	it := &ItemM{key: key, ki: ki} //构造数据进行插入，获得指针
	bt.lock.Lock()

	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*ItemM).ki
}
