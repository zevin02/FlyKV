package FlexDB

//TXN 实现一个事务
type TXN struct {
	writeView *WriteBatch //批量的写
	beginRev  int64       //当前事务启动时候的版本号
	nextSub   int64       //下一次可以加入的Sub,子版本号信息
}

func (db *DB) NewTXN(options WriteBatchOptions) *TXN {
	return &TXN{
		writeView: db.NewWriteBatch(options),
		beginRev:  db.lastestRevison, //当前事务启动时候的版本号
		nextSub:   0,
	}

}
