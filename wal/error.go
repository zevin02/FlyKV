package wal

import "errors"

var (
	ErrPayloadExceedSeg = errors.New("payload exceed segment size")
)