package mvcc

import (
	"sync"
)

//treeIndex 使用btree来存储，index中的key存储的是就是key,value里面存储的就是一个keyIndex对象
type treeIndex struct {
	tree *BTree //这里直接使用btree
	lock sync.RWMutex
}

//NewTreeIndex 初始化一个treeIndex
func NewTreeIndex() *treeIndex {
	return &treeIndex{
		tree: NewBtree(),
	}
}

//Get 从treeIndex中获得当前key对应的符合条件的revision信息
func (ti *treeIndex) Get(key []byte, rev int64) (*Revision, error) {
	ti.lock.RLock()
	defer ti.lock.RUnlock()
	//先从btree中的得到他对应的keyIndex对象
	ki := ti.tree.Get(key)
	if ki == nil {
		return nil, ErrRevisionNotFound
	}
	return ki.get(rev), nil
}

//Put 在当前的treeIndex中给key插入一个revision信息
func (ti *treeIndex) Put(key []byte, rev Revision) {
	ti.lock.Lock()
	defer ti.lock.Unlock()
	ki := ti.tree.Get(key) //先从btree中的得到他对应的keyIndex对象
	if ki == nil {
		//说明当前是第一次进来,给这个ki进行初始化一下
		ki = &KeyIndex{key: key}
	}
	//这个地方说明他成功得到了，就可以直接进行插入了
	ki.put(rev.Main, rev.Sub)
	//更新索引
	ti.tree.Put(key, ki)
}
