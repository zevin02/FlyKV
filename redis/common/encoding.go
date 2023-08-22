package common

func EncodeKeyWithIndex(key []byte, index byte) []byte {
	//第一个字节用来存储db的index
	buf := make([]byte, 1+len(key))
	buf[0] = index
	copy(buf[1:], key)
	return buf
}

func DecodeKey(keyWithIndex []byte) []byte {
	//第一个字节用来存储db的index
	return keyWithIndex[1:]
}
