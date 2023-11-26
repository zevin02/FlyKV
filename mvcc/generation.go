package mvcc

//generation 当前从创建到删除的key的所有的版本信息
type generation struct {
	created Revision   //当前generation中的第一个版本号信息
	revs    []Revision //当前key从创建到删除的所有版本号信息
	//如果当前的generation已经没用了，就会在最后添加一个tomb信息
}

//findRevision 在当前版本下找到比当前版本小的最大版本,因为查找的都是最大，所以
func (g *generation) findRevision(rev int64) *Revision {
	//从最后一个版本号开始往前遍历
	for i := len(g.revs) - 1; i >= 0; i-- {
		if g.revs[i].Main <= rev {
			return &g.revs[i]
		}
	}
	return nil
}

//IsEmpty 如果当前的generation是空的，就说明当前的key已经被删除了
func (g *generation) IsEmpty() bool {
	return g == nil || len(g.revs) == 0
}
