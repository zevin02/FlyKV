package mvcc

import "errors"

type KeyIndex struct {
	key         []byte       //原始的key
	modified    Revision     //最新修改的revision信息
	generations []generation //当前key的generation信息
}

var (
	ErrRevisionNotFound = errors.New("got an unexpected empty keyIndex")
)

//在keyIndex中查找符合条件的generation
//在generation中查找符合条件的revision
//findRevision 给定一个版本号,找到小于当前版本号的最大generation
func (KI *KeyIndex) findGenration(rev int64) *generation {
	//从最后一个开始遍历
	lastg := len(KI.generations) - 1

	for i := len(KI.generations) - 1; i >= 0; i-- {
		//当前的版本下一个revision也没有
		if len(KI.generations[i].revs) == 0 {
			continue
		}
		if i != lastg {
			//说明不是最后一个generation，所以当前generation的最后一个revision是一个tomb
			if tomb := KI.generations[i].revs[len(KI.generations[i].revs)-1].Main; tomb < rev {
				return nil
			}
		}
		if KI.generations[i].created.Main <= rev {
			return &KI.generations[i]
		}
	}
	return nil
}

//get 在当前的rev下查找到符合条件的revision，这个地方一次只能得到一个符合条件的revision
func (KI *KeyIndex) get(rev int64) *Revision {
	g := KI.findGenration(rev) //先找到一个符合条件的generation
	//如果当前的是空的generation，说明当前就没有符合条件的revision进行返回
	if g.IsEmpty() {
		return nil
	}
	revison := g.findRevision(rev)
	if revison != nil {
		//如果当前的revision不为空，就返回当前的revision
		return revison
	}
	return nil
}

//put 给当前的keyIndex中添加一个revision
func (KI *KeyIndex) put(main, sub int64) {
	rev := Revision{main, sub}
	if rev.Main <= KI.modified.Main {
		//如果当前要操作的revision小于当前keyindex的最新的revision
		//就报错
	}
	//当前的key的generation中一个generation都没有，就创建一个新的generation
	if len(KI.generations) == 0 {
		KI.generations = append(KI.generations, generation{})
	}
	//操作最新的一个generation
	latestG := &KI.generations[len(KI.generations)-1]
	if len(latestG.revs) == 0 {
		latestG.created = rev
	}
	latestG.revs = append(latestG.revs, rev) //给当前的generation中添加一个revision
	KI.modified = rev                        //更新当前的keyindex的最新的revision

}

//Tombstone 给当前插入一个revision
func (KI *KeyIndex) Tombstone(main, sub int64) (*Revision, error) {
	rev := KI.get(main)
	//当前的keyIndex中没有符合条件的revision，就返回没有找到
	if rev == nil {
		return nil, ErrRevisionNotFound
	}

	//在当前的keyindex中插入一个tombstone，再新创建一个generation
	KI.put(main, sub) //给当前的generation中插入一个tombstone
	KI.generations = append(KI.generations, generation{})
	return rev, nil
}

////compact 给定一个当前的版本号，把无效的版本号进行归总，并返回
//func (KI *KeyIndex) compact(atRev int64) []Revision {
//
//}

//doCompact
func (KI *KeyIndex) doCompact(atRev int64) (genIdx int, revIdx int) {
	genIdx, g := 0, &KI.generations[0] //genIdx是从最老的代开始寻找

	for genIdx < len(KI.generations)-1 {
		//因为最后一个generation是一个活跃的genenration
		if tomb := g.revs[len(g.revs)-1].Main; tomb < atRev {
			break
		}
		genIdx++
		g = &KI.generations[genIdx]
	}
	//TODO add code
	return 0, 0
}

//IsEmpty 如果当前的generation是空的
func (KI *KeyIndex) IsEmpty() bool {
	return len(KI.generations) == 1 && KI.generations[0].IsEmpty()
}
