package cachedb

import (
	"errors"
	"github.com/allegro/bigcache/v3"
	"time"
)

type BigCacheAPI struct {
	cache *bigcache.BigCache
}

func NewBigCache(fnRemove func(key string, entry []byte, reason RemoveReason)) (*BigCacheAPI, error) {
	onRemove := func(key string, entry []byte, reason bigcache.RemoveReason) {
		if fnRemove != nil {
			switch reason {
			case bigcache.Deleted:
				fnRemove(key, entry, Deleted)
			case bigcache.Expired:
				fnRemove(key, entry, Expired)
			case bigcache.NoSpace:
				fnRemove(key, entry, NoSpace)
			default:
				fnRemove(key, entry, UnknownReason)
			}
		}
	}

	config := bigcache.DefaultConfig(10 * time.Minute)
	config.Shards = 1024               //2`14
	config.HardMaxCacheSize = 1024 * 2 //2G
	config.MaxEntrySize = 1024
	config.MaxEntriesInWindow = 400000 * 6
	config.OnRemoveWithReason = onRemove
	config.CleanWindow = 7 * time.Second //不要太长，避免更新频繁导致满缓存被动淘汰 假设20w pqs --> 10s=200w条缓存
	cache, err := bigcache.NewBigCache(config)
	if err != nil {
		return nil, err
	}
	db := &BigCacheAPI{
		cache: cache,
	}
	return db, nil
}

func (db *BigCacheAPI) Close() error {
	if db == nil {
		return nil
	}
	return db.cache.Close()
}

func (db *BigCacheAPI) Get(key string) ([]byte, error) {
	if db == nil {
		return nil, errors.New("db == nil")
	}
	data, err := db.cache.Get(key)
	if err == bigcache.ErrEntryNotFound {
		return nil, ErrEntryNotFound
	}
	return data, err
}

func (db *BigCacheAPI) Set(key string, entry []byte) error {
	if db == nil {
		return errors.New("db == nil")
	}
	return db.cache.Set(key, entry)
}

func (db *BigCacheAPI) GetCache() (*bigcache.BigCache, error) {
	if db == nil {
		return nil, errors.New("db == nil")
	}
	return db.cache, nil
}
