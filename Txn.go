package FlexDB

//TXN 实现一个事务
type TXN struct {
	writeView *WriteBatch //批量的写
	beginRev  int64       //当前事务启动时候的版本号
	nextSub   int64       //下一次可以加入的Sub,子版本号信息
}

func (t *TXN) Put(key []byte, value []byte) error {
	//调用当前的写视图来进行写入
	t.writeView.Put(key, value, t.nextSub)
	t.nextSub++
	return nil
}

func (db *DB) NewTXN(options WriteBatchOptions) *TXN {
	txn := &TXN{
		beginRev:  db.latestRevison, //当前事务启动时候的版本号
		nextSub:   0,
		writeView: db.NewWriteBatch(options, db.latestRevison),
	}
	//初始化完当前一个事务之后，db的latestRevison就会自增1
	db.latestRevison++
	return txn
}

//Get 读视图，使用当前的事务进行读取
func (t *TXN) Get(key []byte) ([]byte, error) {
	val, ok := t.writeView.Get(key)
	if ok {
		return val, nil
	}
	val, err := t.writeView.db.GetVal(key, t.beginRev)
	if err != nil {
		return nil, err
	}
	return val, nil
	//当前不存在，就需要去使用db来进行访问,使用当前的事务的版本号去访问

}

func (t *TXN) Commit() {
	t.writeView.Commit()
}
