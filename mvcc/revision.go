package mvcc

//Revision 每次操作的版本号信息
type Revision struct {
	Main int64 //指定当前是哪个事务
	Sub  int64 //当用户启动事务的时候，当前才会递增
}

//找到比当前main小的最新的一个revision
