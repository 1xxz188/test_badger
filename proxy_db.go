package main

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/golang/groupcache/singleflight"
	cmap "github.com/orcaman/concurrent-map"
	"sync/atomic"
	"test_badger/cachedb"
	"test_badger/controlEXE"
	"time"
)

type DBProxy struct {
	db        *badger.DB
	cache     cachedb.Cache
	c         *controlEXE.ControlEXE
	noSaveMap cmap.ConcurrentMap
	g         singleflight.Group

	cachePenetrateCnt uint32 //穿透缓存次数
	loadDBCnt         uint32 //加载DB次数 TODO 分开64字节
}

func CreateDBProxy(db *badger.DB, c *controlEXE.ControlEXE) (*DBProxy, error) {
	if db == nil {
		return nil, errors.New("CreateDBProxy db == nil")
	}
	if c == nil {
		return nil, errors.New("CreateDBProxy c == nil")
	}

	proxy := &DBProxy{
		db:        db,
		c:         c,
		noSaveMap: cmap.New(),
	}

	cache, err := cachedb.NewBigCache(func(key string, entry []byte, reason cachedb.RemoveReason) {
		//fmt.Printf("Remove [%s], reason[%d]\n", key, reason)
		if reason == cachedb.Deleted {
			return //定期保存的时候自然会过滤掉已删除的key
		}
		//如果未保存则保存
		_, ok := proxy.noSaveMap.Get(key)
		if !ok {
			return
		}
		proxy.noSaveMap.Remove(key)
		err := proxy.db.Update(func(txn *badger.Txn) error {
			fmt.Printf("warning save by remove key[%s] reason[%d]\n", key, reason)
			return txn.Set([]byte(key), entry)
		})
		if err != nil {
			panic(err)
		}
	})

	if err != nil {
		return nil, err
	}
	proxy.cache = cache

	c.WG2Add(1)
	go func() {
		defer c.WG2Done()
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
				c.WGWait() //wait all send data coroutine exit
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
func (proxy *DBProxy) GetLoadDBCnt() uint32 {
	return atomic.LoadUint32(&proxy.loadDBCnt)
}

func (proxy *DBProxy) Get(txn *badger.Txn, key string) ([]byte, error) {
	data, err := proxy.cache.Get(key)
	if err != nil {
		if err != cachedb.ErrEntryNotFound {
			return nil, err
		}

		atomic.AddUint32(&proxy.cachePenetrateCnt, 1)

		//穿透到badger
		/*item, err := txn.Get([]byte(key))
		if err != nil {
			return nil, err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		//更新到缓存中
		err = proxy.cache.Set(key, data)
		if err != nil {
			return nil, err
		}
		return data, nil*/
		//TODO 查一下 badger里面是否保证了缓存击穿
		data, err := proxy.g.Do(key, func() (interface{}, error) { //TODO 改为模板封装
			atomic.AddUint32(&proxy.loadDBCnt, 1)
			//穿透到badger
			item, err := txn.Get([]byte(key))
			if err != nil {
				return nil, err
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return nil, err
			}
			return v, nil
		})
		if err != nil {
			return nil, err
		}

		//更新到缓存中
		err = proxy.cache.Set(key, data.([]byte))
		if err != nil {
			return nil, err
		}
		return data.([]byte), nil
	}
	return data, nil
}

func (proxy *DBProxy) Set(key string, entry []byte) error {
	if err := proxy.cache.Set(key, entry); err != nil {
		return err
	}
	proxy.noSaveMap.Set(key, nil)
	return nil
}
