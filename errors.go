package BitcaskDB

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("failed to update index")
	ErrKeyNotFound       = errors.New("the key is not found in database")
	ErrDataFileNotFound  = errors.New("data file is not found in database")
	ErrDirIsInValid      = errors.New("DirPath is invalid")
	ErrFileSizeInValid   = errors.New("FileSize is invalid")
	ErrDataDirCorrupted  = errors.New("database directory maybe corrupted")
)
