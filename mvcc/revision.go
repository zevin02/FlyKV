package mvcc

import "encoding/binary"

//Revision 每次操作的版本号信息
type Revision struct {
	Main int64 //指定当前是哪个事务
	Sub  int64 //当用户启动事务的时候，当前才会递增
}

//找到比当前main小的最新的一个revision

//Encode 对当前的Revision进行一个编码,编码成一个16个字节的数组
func (r *Revision) Encode() []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:], uint64(r.Main))
	binary.BigEndian.PutUint64(buf[8:], uint64(r.Sub))
	return buf
}
