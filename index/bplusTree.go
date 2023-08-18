package index

import (
	"BitcaskDB/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

//b+树索引,将索引存储在磁盘中,其本身也是一个存储引擎
//主要封装了go.etcd.io/bbolt
const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("index")

type BPlusTree struct {
	tree *bbolt.DB //内部封转了锁，可以实现并发访问
}

func NewBPT(dirPath string, syncWrite bool) *BPlusTree {
	//打开一个文件来存储这些数据,先保证这个目录是存在的
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrite
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		return nil
	}
	//创建一个bucket，就可以通过这个bucket实现事务的读写
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("fail to create bucket in bptree")
	}
	return &BPlusTree{
		tree: bptree,
	}
}

// Put 将位置索引存储在磁盘中
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		//拿到我们前面定义的bucket，并将key and value进行编码存入磁盘
		bucket := tx.Bucket(indexBucketName)
		//先根据key获得之前的数据
		oldValue = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))

	}); err != nil {
		panic("fail to put value in bptree")
	}
	if len(oldValue) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldValue)
}

//Get 根据Key获得相应的位置信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	//只读事务
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("fail to get value in bptree")
	}
	return pos

}

//Delete 根据key在位置数据
func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldVal []byte //获取旧的数据
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		//先判断是否存在
		if oldVal = bucket.Get(key); len(oldVal) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("fail to delete value in bptree")
	}
	if len(oldVal) == 0 {
		//没有旧的数据，删除失败
		return nil, false
	}
	//得到了旧的数据，删除成功
	return data.DecodeLogRecordPos(oldVal), true
}

//Iterator 索引迭代器
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

//Size 索引中保存的数据个数
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("fail to get size in bptree")
	}
	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

type bptreeIterator struct {
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor //游标，使用这个就可以进行迭代
	reverse bool
	currKey []byte
	currVal []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	//手动的打开一个事务
	tx, err := tree.Begin(false)
	if err != nil {
		panic("fail to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind() //先进行初始化
	return bpi
}

//Rewind 重新回到迭代器的起点，即第一个位置
func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.currKey, bpi.currVal = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currVal = bpi.cursor.First()
	}
}

//Seek 根据传入的Key查找到第一个大于等于的目标key，根据从这个key开始遍历
func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.currKey, bpi.currVal = bpi.cursor.Seek(key)
}

//Next 跳转到下一个key
func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.currKey, bpi.currVal = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currVal = bpi.cursor.Next()
	}
}

//Valid 是否有效，即时有已经遍历完了所有的Key，用来退出遍历
func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.currKey) != 0
}

//Key 当前遍历位置的key数据
func (bpi *bptreeIterator) Key() []byte {
	return bpi.currKey
}

//Value 当前遍历位置的value数据
func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.currVal)
}

//Close 关闭迭代器，释放相应的资源
func (bpi *bptreeIterator) Close() {
	//将事务进行提交
	_ = bpi.tx.Rollback()
}
