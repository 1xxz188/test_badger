package proxy

import (
	"fmt"
	"github.com/allegro/bigcache/v3"
	"github.com/dgraph-io/badger/v3"
	"test_badger/badgerApi"
	"test_badger/cachedb"
	"test_badger/controlEXE"
)

type Opts struct {
	optBadger         badger.Options                  //badger配置, 可替换
	cache             cachedb.Cache                   //缓存层 (默认bigCache) 可替换
	fnRemoveButNotDel *func(key string, entry []byte) //缓存层移除时，需要回调做数据落库
	c                 *controlEXE.ControlEXE          //协程生命周期控制器(生产消息协程,消费协程,GC协程)
	IsUseCache        bool
}

func DefaultOptions(dbDir string) Opts {
	return DefaultBigCacheOptions(badgerApi.DefaultOptions(dbDir), cachedb.DefaultBigCacheOptions())
}

func DefaultNoCache(dbDir string) Opts {
	return Opts{
		optBadger:  badgerApi.DefaultOptions(dbDir),
		c:          controlEXE.CreateControlEXE(),
		IsUseCache: false,
	}
}

func DefaultBigCacheOptions(optBadger badger.Options, bigCacheConf bigcache.Config) Opts { //badgerApi.DefaultOptions(dbDir), cachedb.DefaultBigCacheOptions()
	fnRemoveButNotDel := new(func(key string, entry []byte))
	bigCache, err := cachedb.NewBigCache(bigCacheConf, func(key string, entry []byte, reason cachedb.RemoveReason) {
		//进入缓存淘汰，不存在移除的时候同时插入(底层有分片锁)
		if reason == cachedb.Deleted {
			panic(fmt.Sprintf("key[%s] remove by cachedb.Deleted", key)) //TODO 删除
			return                                                       //定期保存的时候自然会过滤掉已删除的key
		}
		//Call
		if (*fnRemoveButNotDel) == nil {
			panic("(*fnRemoveButNotDel) == nil")
		}
		(*fnRemoveButNotDel)(key, entry)
	})
	if err != nil {
		panic(err)
	}
	return Opts{
		optBadger:         optBadger,
		cache:             bigCache,
		fnRemoveButNotDel: fnRemoveButNotDel,
		c:                 controlEXE.CreateControlEXE(),
		IsUseCache:        true,
	}
}
