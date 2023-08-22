package redis

import (
	"FlexDB"
	"encoding/binary"
	"time"
)

const (
	maxMetaDataSize     = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	maxListMetaDataSize = binary.MaxVarintLen64 * 2
)

type metadata struct {
	dataType redisDataType //数据类型
	expire   int64         //过期时间
	version  int64         //版本号
	size     uint64        //数量大小
	head     uint64        //List数据结构的头
	tail     uint64        //List数据结构的尾巴
}

func (md *metadata) encode() []byte {
	var size = maxMetaDataSize
	if md.dataType == List {
		size += maxListMetaDataSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutUvarint(buf[index:], md.size)
	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}

func decode(buf []byte) *metadata {
	dataType := buf[0]
	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n

	version, n := binary.Varint(buf[index:])
	index += n

	size, n := binary.Uvarint(buf[index:])
	index += n
	var head uint64 = 0
	var tail uint64 = 0

	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     size,
		head:     head,
		tail:     tail,
	}
}

func (rds *RedisDataStruct) GetMetaData(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != FlexDB.ErrKeyNotFound { //key没找到是可以接收的，因为我们要插入的就是没有找到的
		return nil, err
	}
	//key没有找到就要初始化
	var meta *metadata
	var exist bool = true
	if err == FlexDB.ErrKeyNotFound {
		exist = false
	} else {
		//数据存在，对他进行解码

		meta = decode(metaBuf)
		//判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		//判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			//该数据已经过期了
			exist = false
		}
	}
	if !exist {
		//当前的数据不存在,就需要进行初始化构造
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)
	var index = 0
	binary.LittleEndian.PutUint64(buf[index:], uint64(hk.version))

	index += 8
	copy(buf[index:], hk.key)
	copy(buf[index+len(hk.key):], hk.field)
	return buf

}
