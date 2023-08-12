package BitcaskDB

type Options struct {
	DirPath   string    //数据库数据目录
	FileSize  uint64    //活跃文件的阈值
	SyncWrite bool      //是否在每次写都进行持久化
	IndexType IndexType //索引类型
}

type IndexType = int8

const (
	//Btree 索引
	Btree IndexType = iota
	//ART 自适应基数树
	ART
)

var DefaultOperations = Options{
	DirPath:   string("/home/zevin/githubmanage/program/BitcaskDB/storefile"),
	FileSize:  256 * 1024 * 1024, //256MB
	SyncWrite: false,
	IndexType: Btree,
}

//IteratorOptions 索引迭代器的配置项
type IteratorOptions struct {
	//遍历前缀为指定的Key，默认为空
	Prefix []byte
	//是否为反向遍历
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
