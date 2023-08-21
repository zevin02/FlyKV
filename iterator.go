package FlexDB

import (
	"FlexDB/index"
	"bytes"
)

//供用户使用的迭代器
type Iterator struct {
	options   IteratorOptions
	indexIter index.Iterator //索引迭代器
	db        *DB
}

func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(options.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   options,
	}
}

//Rewind 重新回到迭代器的起点，即第一个位置
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	//如果有指定前缀，就需要直接跳转到指定位置，否则就没有操作
	it.skipToNext()
}

//Seek 根据传入的Key查找到第一个大于等于的目标key，根据从这个key开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	//如果有指定前缀，就需要直接跳转到指定位置，否则就没有操作
	it.skipToNext()
}

//Next 跳转到下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	//如果有指定前缀，就需要直接跳转到指定位置，否则就没有操作
	it.skipToNext()

}

//Valid 是否有效，即时有已经遍历完了所有的Key，用来退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

//Key 当前遍历位置的key数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

//Value 当前遍历位置的value数据
func (it *Iterator) Value() []byte {
	logRecordPos := it.indexIter.Value()
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
	it.indexIter.Close()
}

//用于如果指定prefix的话，需要找到指定前缀的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			//前缀符合要求就可以跳出查找了
			break
		}
	}
}
