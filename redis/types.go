package redis

import (
	"FlexDB"
	"encoding/binary"
	"errors"
	"time"
)

type redisDataType = byte

var (
	ErrWrongTypeOperation = errors.New("wrong type operation")
)

const (
	String redisDataType = iota + 1
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

//==================string===================

func (rds *RedisDataStruct) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}
	//编码value:type +expire+payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano() //获得过期的时间戳
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value[:])
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStruct) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	//解码
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	//判断key是否已经过期了
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, err
	}
	return encValue[index:], nil
}
