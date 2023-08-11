package BitcaskDB

type Options struct {
	DirPath   string    //数据库数据目录
	FileSize  uint64    //活跃文件的阈值
	SyncWrite bool      //是否在每次写都进行持久化
	IndexType IndexType //索引类型
}

type IndexType = int8

const (
	//Btree索引
	Btree IndexType = iota + 1
	//ART自适应基数树
	ART
)
