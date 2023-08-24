package FlexDB

import (
	"FlexDB/index"
	"bytes"
	"container/heap"
)

var maxHeap bool

//供用户使用的迭代器
type Iterator struct {
	options    IteratorOptions
	db         *DB
	iters      ItemHeap //最小堆
	indexIters map[string]index.Iterator
}

//定义一个结构体，用来存储最小堆中的数据
type Node struct {
	key  []byte         //这个key的数据
	iter index.Iterator //这个key所在的索引位置
}

type ItemHeap []*Node

func (h ItemHeap) Len() int {
	return len(h)
}

func (h ItemHeap) Less(i, j int) bool {
	if maxHeap {
		return bytes.Compare(h[i].key, h[j].key) >= 0

	} else {
		return bytes.Compare(h[i].key, h[j].key) <= 0
	}
}
func (h ItemHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *ItemHeap) Push(item interface{}) {
	*h = append(*h, item.(*Node))
}
func (h *ItemHeap) Pop() interface{} {
	old := *h
	n := len(old)    //获得元素的个数
	item := old[n-1] //
	old[n-1] = nil   //避免内存泄漏
	*h = old[0 : n-1]
	return item
}

//初始化迭代器
func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	var iters ItemHeap
	//更新迭代器
	indexIters := make(map[string]index.Iterator, db.options.indexNum)
	maxHeap = options.Reverse
	for name, index := range db.index {
		indexIter := index.Iterator(options.Reverse)
		indexIter.Rewind() //将每个迭代器进行初始化
		if indexIter.Valid() {
			item := &Node{
				key:  indexIter.Key(),
				iter: indexIter,
			}
			indexIters[name] = indexIter
			iters = append(iters, item) //把数据添加到最小堆中,先添加几个元素到最小堆里面
		}
	}

	//初始化该小堆
	heap.Init(&iters)

	return &Iterator{
		db:         db,
		iters:      iters,
		options:    options,
		indexIters: indexIters,
	}
}

//Rewind 重新回到迭代器的起点，即第一个位置
func (it *Iterator) Rewind() {
	//将每个迭代器都进行初始化，设置成首个元素
	for _, indexIter := range it.indexIters {
		indexIter.Rewind()
	}
	//如果有指定前缀，就需要直接跳转到指定位置，否则就没有操作
	it.skipToNext()
}

//Seek 根据传入的Key查找到第一个大于等于的目标key，根据从这个key开始遍历
func (it *Iterator) Seek(key []byte) {

	for _, indexIter := range it.indexIters {
		indexIter.Seek(key) //每个元素都往后走一个位置，并且把这个元素添加进去,并且把比他小的全部
		//既要有效，同时前缀还要相同才能放到最小堆里面
		if indexIter.Valid() {
			item := &Node{
				key:  indexIter.Key(),
				iter: indexIter,
			}
			it.iters = append(it.iters, item) //把数据添加到最小堆中,先添加几个元素到最小堆里面
		}

		//如果有指定前缀，就需要直接跳转到指定位置，否则就没有操作
		//it.skipToNext()
	}
	//找到的话，就要把前面的删除掉
	//如果有指定前缀，就需要直接跳转到指定位置，否则就没有操作
	it.skipToNext()
}

//Next 跳转到下一个key
func (it *Iterator) Next() {
	//当前没有元素的话，直接
	if len(it.indexIters) == 0 {
		return
	}
	heap.Pop(&it.iters) //把里面的元素删除掉
	//再新增加元素进去
	for _, indexIter := range it.indexIters {
		indexIter.Next() //每个元素都往后走一个位置，并且把这个元素添加进去
		if indexIter.Valid() {
			item := &Node{
				key:  indexIter.Key(),
				iter: indexIter,
			}
			it.iters = append(it.iters, item) //把数据添加到最小堆中,先添加几个元素到最小堆里面
		}

		//如果有指定前缀，就需要直接跳转到指定位置，否则就没有操作
		it.skipToNext()
	}
}

//Valid 是否有效，即时有已经遍历完了所有的Key，用来退出遍历
func (it *Iterator) Valid() bool {
	return len(it.iters) > 0 //最小堆里面存在元素即有效
}

//Key 当前遍历位置的key数据
func (it *Iterator) Key() []byte {
	return it.iters[0].key
}

//Value 当前遍历位置的value数据
func (it *Iterator) Value() []byte {
	node := it.iters[0] //获得堆顶的节点
	logRecordPos := node.iter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	val, err := it.db.getValueByPos(logRecordPos)
	if err != nil {
		return nil
	}
	return val
}

//Close 关闭迭代器，释放相应的资源
func (it *Iterator) Close() {
	for _, indexIter := range it.indexIters {
		indexIter.Close()
	}
}

//用于如果指定prefix的话，需要找到指定前缀的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for len(it.iters) > 0 {
		item := it.iters[0] //获得堆顶部的元素
		key := item.key
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			//前缀符合要求就可以跳出查找了
			break
		}
		it.Next()
	}
}
