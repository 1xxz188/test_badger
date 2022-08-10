package cachedb

import "errors"

var (
	// ErrEntryNotFound is an error type struct which is returned when entry was not found for provided key
	ErrEntryNotFound = errors.New("entry not found")
)

// RemoveReason is a value used to signal to the user why a particular key was removed in the OnRemove callback.
type RemoveReason uint32

const (
	// Expired means the key is past its LifeWindow.
	Expired = RemoveReason(1)
	// NoSpace means the key is the oldest and the cache size was at its maximum when Set was called, or the
	// entry exceeded the maximum shard size.
	NoSpace = RemoveReason(2)
	// Deleted means Delete was called and this key was removed as a result.
	Deleted = RemoveReason(3)

	UnknownReason = RemoveReason(4)
)

type SetCb func()
type GetCb func()

type Cache interface {
	Get(key string) ([]byte, error)
	GetCb(key string, cb GetCb) ([]byte, error)
	Set(key string, entry []byte) error
	SetCb(key string, entry []byte, cb SetCb) error
	Delete(key string) error
	Len() int
	Close() error
}

/*
1. 数据过期时保存
2. 退出程序时保存
3. 定期批量保存(默认1s)
*/
