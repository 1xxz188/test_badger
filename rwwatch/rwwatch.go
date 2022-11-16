package rwwatch

import (
	"errors"
	"sync"
)

var (
	ErrWatchVersionConflicts = errors.New("watch key conflicts")
	ErrWatchKeyNotExist      = errors.New("watch key not exist")
	ErrWatchKeyInitParameter = errors.New("watch key int parameter invalid")
)

type watchInfo struct {
	lock    sync.RWMutex
	version uint32
}

type concurrentMapShared struct {
	items map[string]*watchInfo
	lock  sync.RWMutex
}

type WatchKeyMgr struct {
	shardCount     uint32
	shardCountMask uint32
	m              []concurrentMapShared
}

func getAlign(shardCount uint32) uint32 {
	//2的幂次方对齐
	temp := uint32(1) << 1
	for i := 2; shardCount > temp; i++ {
		temp = uint32(1) << i
	}
	return temp
}

func New(shardCount uint32) (*WatchKeyMgr, error) {
	if shardCount < 1 {
		return nil, ErrWatchKeyInitParameter
	}
	needShardCount := getAlign(shardCount)
	s := &WatchKeyMgr{
		shardCount:     needShardCount,
		shardCountMask: needShardCount - 1,
		m:              make([]concurrentMapShared, needShardCount),
	}
	for i := uint32(0); i < s.shardCount; i++ {
		s.m[i].items = make(map[string]*watchInfo)
	}
	return s, nil
}

// Read will add key if not exist the key
func (s *WatchKeyMgr) Read(key string, fn func(keyVersion uint32, isNewKey bool) error) (err error) {
	shard := s.getShard(key)
	shard.lock.RLock()
	// Get item from shard.
	val, ok := shard.items[key]

	if ok {
		shard.lock.RUnlock()
		val.lock.RLock()
		defer val.lock.RUnlock() //prevent panic
		err = fn(val.version, false)
	} else {
		shard.lock.RUnlock()
		shard.lock.Lock()
		val, ok := shard.items[key]
		if !ok {
			val = &watchInfo{
				version: 1,
			}
			shard.items[key] = val
		}
		shard.lock.Unlock()
		val.lock.RLock()
		defer val.lock.RUnlock()
		err = fn(val.version, true)
	}
	return err
}

//Write fail if not exist the key
func (s *WatchKeyMgr) Write(key string, keyVersion uint32, isCheckKeyVersion bool, fn func(keyVersion uint32) error) error {
	shard := s.getShard(key)
	shard.lock.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	if !ok {
		shard.lock.RUnlock()
		return ErrWatchKeyNotExist
	}
	shard.lock.RUnlock()
	val.lock.Lock()
	defer val.lock.Unlock() //prevent panic
	if isCheckKeyVersion && keyVersion != val.version {
		return ErrWatchVersionConflicts
	}
	err := fn(val.version)
	val.version += 1
	return err
}

func (s *WatchKeyMgr) Remove(key string, keyVersion uint32, isCheckKeyVersion bool) error {
	shard := s.getShard(key)
	shard.lock.RLock()
	val, ok := shard.items[key]
	if !ok {
		shard.lock.RUnlock()
		return ErrWatchKeyNotExist
	}
	if isCheckKeyVersion && keyVersion != val.version {
		shard.lock.RUnlock()
		return ErrWatchVersionConflicts
	}
	shard.lock.RUnlock()
	shard.lock.Lock()
	defer shard.lock.Unlock()
	val, ok = shard.items[key]
	if !ok {
		return ErrWatchKeyNotExist
	}
	if isCheckKeyVersion && keyVersion != val.version {
		return ErrWatchVersionConflicts
	}
	delete(shard.items, key) //移除key
	return nil
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (s *WatchKeyMgr) getShard(key string) *concurrentMapShared {
	return &s.m[fnv32(key)&s.shardCountMask]
}
