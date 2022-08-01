package main

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	cmap "github.com/orcaman/concurrent-map"
	"log"
	"sync/atomic"
	"test_badger/cachedb"
	"test_badger/controlEXE"
	"test_badger/customWatchKey"
	"time"
)

type DBProxy struct {
	db                *badger.DB
	cache             cachedb.Cache
	c                 *controlEXE.ControlEXE
	watchKeyMgr       *customWatchKey.WatchKeyMgr
	noSaveMap         cmap.ConcurrentMap //待保存列表
	cachePenetrateCnt uint32             //穿透缓存次数
}

func CreateDBProxy(db *badger.DB, c *controlEXE.ControlEXE) (*DBProxy, error) {
	if db == nil {
		return nil, errors.New("CreateDBProxy db == nil")
	}
	if c == nil {
		return nil, errors.New("CreateDBProxy c == nil")
	}

	watchKeyMgr, err := customWatchKey.New(1024)
	if err != nil {
		return nil, err
	}
	proxy := &DBProxy{
		db:          db,
		c:           c,
		watchKeyMgr: watchKeyMgr,
		noSaveMap:   cmap.New(),
	}

	cache, err := cachedb.NewBigCache(func(key string, entry []byte, reason cachedb.RemoveReason) {
		//进入缓存淘汰
		//fmt.Printf("Remove [%s], reason[%d]\n", key, reason)
		if reason == cachedb.Deleted {
			return //定期保存的时候自然会过滤掉已删除的key
		}
		//TODO watchKey
		err := watchKeyMgr.Write(key, 0, false, func(keyVersion uint32) error {
			//如果未保存则保存
			_, ok := proxy.noSaveMap.Get(key)
			if !ok {
				return nil
			}
			proxy.noSaveMap.Remove(key)
			return proxy.db.Update(func(txn *badger.Txn) error {
				fmt.Printf("warning save by remove key[%s] reason[%d]\n", key, reason)
				return txn.Set([]byte(key), entry)
			})
		})
		if err != nil {
			log.Panic(err)
		}
	})

	if err != nil {
		return nil, err
	}
	proxy.cache = cache

	c.ConsumerAdd(1)
	go func() {
		defer c.ConsumerDone()
		//interval save
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		var v []byte
		var err error

		saveData := func() {
			if proxy.noSaveMap.Count() <= 0 {
				return
			}

			wb := proxy.db.NewWriteBatch()
			now := time.Now()
			startTm := now
			toSave := proxy.noSaveMap.Items()
			snapshotMs := time.Since(now).Milliseconds()
			now = time.Now()
			for key, _ := range toSave {
				proxy.noSaveMap.Remove(key)
				v, err = cache.Get(key)
				if err != nil {
					continue
				}
				// maxBatchCount int64 // max entries in batch    ---> 10w  MaxBatchSize()
				// maxBatchSize  int64 // max batch size in bytes ---> 9.5MB MaxBatchSize()
				err = wb.Set([]byte(key), v)
				if err != nil { //批量写入事务 内部已经处理了ErrTxnTooBig
					panic(err)
				}
			}
			rangeMs := time.Since(now).Milliseconds()
			now = time.Now()
			if err = wb.Flush(); err != nil {
				panic(err)
			}
			wb.Cancel()
			fmt.Printf(">save key size[%d] snapshot[%d ms] range[%d ms] Flush[%d ms] total[%d ms]\n", len(toSave), snapshotMs, rangeMs, time.Since(now).Milliseconds(), time.Since(startTm).Milliseconds())
		}
		for {
			select {
			case _ = <-ticker.C:
				saveData()
			case <-c.CTXDone():
				//wait all data save
				c.ProducerWait() //wait all send data coroutine exit
				fmt.Println("wait all data save")
				saveData()
				fmt.Println("all data save ok")
				return
			}
		}
	}()

	return proxy, nil
}
func (proxy *DBProxy) GetCachePenetrateCnt() uint32 {
	return atomic.LoadUint32(&proxy.cachePenetrateCnt)
}

func (proxy *DBProxy) Get(txn *badger.Txn, key string) ([]byte, error) {
	data, err := proxy.cache.Get(key)
	if err == nil {
		return data, nil //命中缓存返回数据
	}
	if err != cachedb.ErrEntryNotFound {
		return nil, err
	}

	atomic.AddUint32(&proxy.cachePenetrateCnt, 1)

	//穿透到badger
	item, err := txn.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	//更新到缓存中
	err = proxy.cache.Set(key, v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (proxy *DBProxy) set(key string, entry []byte) error {
	if err := proxy.cache.Set(key, entry); err != nil {
		return err
	}
	proxy.noSaveMap.Set(key, nil)
	return nil
}

func (proxy *DBProxy) Sets(watchKey string, keys []string, entryList [][]byte) error {
	if len(keys) != len(entryList) {
		return errors.New("len(keys) != len(entryList)")
	}
	return proxy.watchKeyMgr.Write(watchKey, 0, false, func(keyVersion uint32) error {
		for i := 0; i < len(keys); i++ {
			err := proxy.set(keys[i], entryList[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}
