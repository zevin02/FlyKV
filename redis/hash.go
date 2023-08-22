package redis

import (
	"FlexDB"
)

func (rds *RedisDataStruct) HSet(key, field, value []byte) (bool, error) {
	//查找对应的元数据
	meta, err := rds.GetMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	//构造数据库中使用的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version, //这些key的version是一样的，删除方便
		field:   field,
	}
	internalKey := hk.encode()
	//查看这个InternelKey是否存在
	var exist = true
	if _, err = rds.db.Get(internalKey); err == FlexDB.ErrKeyNotFound {
		exist = false
	}
	//构造一个writebatch进行批处理，保证数据的原子性
	wb := rds.db.NewWriteBatch(FlexDB.DefaultWriteBatchOption)

	//更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}

	_ = wb.Put(internalKey, value)
	//提交批处理
	if err := wb.Commit(); err != nil {
		return false, err
	}
	//如果已经存在了，就需要返回false，如果不存在，第一次插入才返回true
	return !exist, nil

}

func (rds *RedisDataStruct) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.GetMetaData(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		//当前key中没有数据，只就返回
		return nil, nil
	}
	//构造数据库中使用的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version, //这些key的version是一样的，删除方便
		field:   field,
	}
	internalKey := hk.encode()
	return rds.db.Get(internalKey)

}

func (rds *RedisDataStruct) HDel(key, field []byte) (bool, error) {
	meta, err := rds.GetMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	//元数据中没有元素
	if meta.size == 0 {
		//当前key中没有数据，只就返回
		return false, nil
	}
	//构造数据库中使用的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version, //这些key的version是一样的，删除方便
		field:   field,
	}
	internalKey := hk.encode()
	var exists = true
	if _, err := rds.db.Get(internalKey); err != nil {
		exists = false
	}
	if exists {
		wb := rds.db.NewWriteBatch(FlexDB.DefaultWriteBatchOption)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(internalKey)
		if err := wb.Commit(); err != nil {
			return false, err
		}
		rds.db.Delete(internalKey)

	}
	//如果存在则返回true，不存在返回false
	return exists, nil
}
