package redis

import (
	"FlexDB"
	"encoding/binary"
	"errors"
)

type redisDataType = byte

var (
	ErrWrongTypeOperation = errors.New("wrong type operation")
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

//==================generic command=============================

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
