package wal

import "errors"

var (
	ErrPayloadExceedSeg = errors.New("payload exceed segment size")
	ErrPosNotValid      = errors.New("read pos is not valid")
	ErrInvalidCrc       = errors.New("invalid crc value,log record maybe error")
)
