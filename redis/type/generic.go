package _type

import (
	"FlexDB"
	"encoding/binary"
	"errors"
)

type redisDataType = byte

var (
	ErrWrongTypeOperation = errors.New("wrong type operation")
	ErrDbIndexOut         = errors.New("Err db index is out of range")
)

const (
	initialListMark uint64 = binary.MaxVarintLen64 / 2
)

const (
	String redisDataType = iota + 1
	Hash
	List
)

type RedisDataStruct struct {
	db *FlexDB.DB
}

// NewRedisDataStruct 初始化Redis数据结构服务
func NewRedisDataStruct(options FlexDB.Options) (*RedisDataStruct, error) {
	db, err := FlexDB.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStruct{db: db}, nil

}

func (rds RedisDataStruct) Close() error {
	return rds.db.Close()

}

//==================generic command=============================

//不会影响数据的可见性,对于hash来说，删除了元数据就获得不到元数据的version,就无法通过这version来构造key访问数据库其中
func (rds *RedisDataStruct) Del(key []byte) (bool, error) {

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
