package FlexDB

import (
	"FlexDB/data"
	"FlexDB/index"
	"bytes"
	"container/heap"
)

var maxHeap bool //用来判断用户的迭代器是希望正序还是倒序，由此创建小堆或者大堆

/*
	由于我们原来的索引有多个，所以我们现在需要对多个索引数据进行有序迭代，这里我们使用最小堆来实现，每次取出迭代器中的第一个元素加入到堆顶中，因为堆顶的成功最小的元素，所以我们可以保证每次取出的元素都是最小的元素，这样就可以实现多个索引的有序迭代

*/

// Iterator 供用户使用的迭代器
type Iterator struct {
	options    IteratorOptions
	db         *DB
	iters      ItemHeap //最小堆，里面维护了多个索引的迭代器
	indexIters map[string]index.Iterator
}

// Node 定义一个结构体，用来存储堆中的数据
type Node struct {
	key  []byte         //这个key的数据
	iter index.Iterator //这个key所在的索引迭代器，当前迭代器中存储了这个索引中的所有元素
}

type ItemHeap []*Node

func (h ItemHeap) Len() int {
	return len(h)
}

// Less 根据用户指定的reverse与否来决定是大堆还是小堆
func (h ItemHeap) Less(i, j int) bool {
	if maxHeap {
		//大堆
		return bytes.Compare(h[i].key, h[j].key) >= 0
	} else {
		//小堆
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

// NewIterator 初始化迭代器
func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	//更新迭代器
	indexIters := make(map[string]index.Iterator, db.options.indexNum)
	maxHeap = options.Reverse
	for name, index := range db.index {
		indexIter := index.Iterator(options.Reverse) //获得索引的迭代器
		indexIter.Rewind()                           //将每个迭代器进行初始化
		indexIters[name] = indexIter
	}

	resiter := &Iterator{
		db:         db,
		options:    options,
		indexIters: indexIters,
	}
	resiter.Rewind()
	heap.Init(&resiter.iters)

	return resiter

}

//Rewind 重新回到迭代器的起点，即第一个位置,清空迭代器中的元素，并且添加一些元素进去
func (it *Iterator) Rewind() {
	//先清空堆里面的元素
	for it.Valid() {
		heap.Pop(&it.iters)
	}
	//将每个迭代器都进行初始化，设置成首个元素,并且添加每个迭代器的首元素进入到堆中
	for _, indexIter := range it.indexIters {
		indexIter.Rewind() //将每个索引迭代器都回到起点位置
		//添加节点到堆中
		it.addNode(indexIter)
	}
	//将堆里面的数据清空
	//如果有指定前缀，就需要直接跳转到指定位置，否则就没有操作
	it.skipToNext()
}

//addNode  向heap中添加数据
func (it *Iterator) addNode(indexIter index.Iterator) {
	if indexIter.Valid() {
		item := &Node{
			key:  indexIter.Key(),
			iter: indexIter,
		}
		heap.Push(&it.iters, item)
	}
}

//adjustHeapBeforeSeek 在seek执行前删除干扰的元素
func (it *Iterator) adjustHeapBeforeSeek(key []byte) {
	for it.Valid() {
		if it.options.Reverse {
			//如果是从大到小，那么
			if bytes.Compare(it.Key(), key) >= 0 {
				//可以删除堆里面的元素了
				heap.Pop(&it.iters)
			} else {
				break
			}
		} else {
			//如果是从小到大，那么堆顶的，比key小的都可以删除
			if bytes.Compare(it.Key(), key) <= 0 {
				//可以删除堆里面的元素了
				heap.Pop(&it.iters)
			} else {
				break
			}
		}
	}
}

//Seek 根据传入的Key查找到第一个大于等于的目标key，根据从这个key开始遍历
func (it *Iterator) Seek(key []byte) {
	//需要将堆里面不符合小于key的都先删除掉
	it.adjustHeapBeforeSeek(key)

	for _, indexIter := range it.indexIters {
		var valueBefore *data.LogRecordPos
		if !indexIter.Valid() {
			//如果之前不合理就直接跳过
			continue
		}
		//到这里的值，之前的一定都跳过
		valueBefore = indexIter.Value()
		indexIter.Seek(key) //每个元素都往后走一个位置，并且把这个元素添加进去,并且把比他小的全部
		//在indexiter合理的情况之下fid不同可以直接加入，如果fid相同，那么offset不同也可以加入
		if indexIter.Valid() && (valueBefore.Fid != indexIter.Value().Fid || valueBefore.Offset != indexIter.Value().Offset) {
			//seek之前和seek之后的坐标不同才能插入这个元素
			it.addNode(indexIter)
		}
	}

	//找到的话，就要把前面的删除掉
	//如果有指定前缀，就需要直接跳转到指定位置，否则就没有操作
	it.skipToNext()
}

//Next 跳转到下一个key
func (it *Iterator) Next() {
	//当前没有元素的话，直接
	if !it.Valid() {
		return
	}
	node := heap.Pop(&it.iters).(*Node) //把里面的元素删除掉
	//b+树的这个有问题，插入了相同位置
	node.iter.Next()
	it.addNode(node.iter)

	//	//如果有指定前缀，就需要直接跳转到指定位置，否则就没有操作
	it.skipToNext()
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

//skipToNext 用于如果指定prefix的话，需要找到指定前缀的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for len(it.iters) > 0 {
		key := it.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			//前缀符合要求就可以跳出查找了
			break
		}
		//前缀相同才可以
		it.Next()
	}
}
