package index

import (
	"BitcaskDB/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树索引
//主要封装https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex //使用读写锁保证并发安全，读取资源的时候可以多个线程并发访问，写的时候只有一个线程允许
}

// NewART 初始化自适应基数树索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(), //初始化art树
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldItem, _ := art.tree.Insert(key, pos) //这里的value是type Value interface{}，可以存储任何类型
	art.lock.Unlock()
	if oldItem == nil {
		return nil
	}

	return oldItem.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos) //我们需要强转为我们需要的类型
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	//key存在并且删除成功，deleted=true
	//key不存在则删除失败，deleted=false
	value, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if value == nil {
		return nil, false
	}
	return value.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art.tree == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

// Close art不需要进行释放资源
func (art *AdaptiveRadixTree) Close() error {
	return nil
}

//定义一个ART的索引迭代器
type artIterator struct {
	currIndex int     //遍历到数组的哪一个下标
	reverse   bool    //是否是一个反向的遍历
	value     []*Item //存储全部的key位置索引信息
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	//如果逆向的话，idx就从最后一个开始遍历
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues) //将所有的key和value通过上面的回调函数来保存到values数组中

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		value:     values,
	}
}

//Rewind 重新回到迭代器的起点，即第一个位置
func (ai *artIterator) Rewind() {
	ai.currIndex = 0
}

//Seek 根据传入的Key查找到第一个大于等于的目标key，根据从这个key开始遍历
func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.value), func(i int) bool {
			return bytes.Compare(ai.value[i].key, key) <= 0
		})
	} else {
		//指定比较的规则
		ai.currIndex = sort.Search(len(ai.value), func(i int) bool {
			return bytes.Compare(ai.value[i].key, key) >= 0
		})
	}
}

//Next 跳转到下一个key
func (ai *artIterator) Next() {
	ai.currIndex++
}

//Valid 是否有效，即时有已经遍历完了所有的Key，用来退出遍历
func (ai *artIterator) Valid() bool {
	return ai.currIndex < len(ai.value)
}

//Key 当前遍历位置的key数据
func (ai *artIterator) Key() []byte {
	return ai.value[ai.currIndex].key
}

//Value 当前遍历位置的value数据
func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.value[ai.currIndex].pos

}

//Close 关闭迭代器，释放相应的资源
func (ai *artIterator) Close() {
	ai.value = nil
}
