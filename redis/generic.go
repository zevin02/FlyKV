package redis

//不会影响数据的可见性
func (rds *RedisDataStruct) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStruct) Type(key []byte) (redisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, err
	}
	//第一个字节就是类型
	return encValue[0], nil
}
