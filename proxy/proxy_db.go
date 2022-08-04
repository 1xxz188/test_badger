package proxy

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
	C                 *controlEXE.ControlEXE      //协程生命周期控制器
	Db                *badger.DB                  //底层数据
	cache             cachedb.Cache               //缓存层
	watchKeyMgr       *customWatchKey.WatchKeyMgr //用于缓存层数据一致性控制
	noSaveMap         cmap.ConcurrentMap          //待保存列表
	cachePenetrateCnt uint64                      //缓存穿透次数
	_                 [56]byte                    //cpu cache
	cacheCnt          uint64                      //缓存命中次数
}

type KV struct {
	K   string
	V   []byte
	Err error
}

func CreateDBProxy(db *badger.DB, c *controlEXE.ControlEXE) (*DBProxy, error) {
	if db == nil {
		return nil, errors.New("CreateDBProxy Db == nil")
	}
	if c == nil {
		return nil, errors.New("CreateDBProxy C == nil")
	}

	watchKeyMgr, err := customWatchKey.New(1024)
	if err != nil {
		return nil, err
	}
	proxy := &DBProxy{
		Db:          db,
		C:           c,
		watchKeyMgr: watchKeyMgr,
		noSaveMap:   cmap.New(),
	}

	cache, err := cachedb.NewBigCache(func(key string, entry []byte, reason cachedb.RemoveReason) {
		//进入缓存淘汰
		//fmt.Printf("Remove [%s], reason[%d]\n", key, reason)
		if reason == cachedb.Deleted {
			return //定期保存的时候自然会过滤掉已删除的key
		}

		err := watchKeyMgr.Write(key, 0, false, func(keyVersion uint32) error {
			//如果未保存则保存
			_, ok := proxy.noSaveMap.Get(key)
			if !ok {
				return nil
			}
			proxy.noSaveMap.Remove(key)
			return proxy.Db.Update(func(txn *badger.Txn) error {
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

			wb := proxy.Db.NewWriteBatch()
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

func (proxy *DBProxy) GetCachePenetrateCnt() uint64 {
	return atomic.LoadUint64(&proxy.cachePenetrateCnt)
}

func (proxy *DBProxy) GetCacheCnt() uint64 {
	return atomic.LoadUint64(&proxy.cacheCnt)
}

func (proxy *DBProxy) GetCachePenetrateRate() float64 {
	return float64(proxy.GetCachePenetrateCnt() / proxy.GetCacheCnt())
}

func (proxy *DBProxy) get(txn *badger.Txn, key string) ([]byte, error) {
	data, err := proxy.cache.Get(key)
	if err == nil {
		atomic.AddUint64(&proxy.cacheCnt, 1)
		return data, nil //命中缓存返回数据
	}
	if err != cachedb.ErrEntryNotFound {
		return nil, err
	}

	atomic.AddUint64(&proxy.cachePenetrateCnt, 1)

	//穿透到badger
	item, err := txn.Get([]byte(key))
	if err != nil {
		if err == badger.ErrKeyNotFound { //转换错误
			return nil, cachedb.ErrEntryNotFound
		}
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

func (proxy *DBProxy) Gets(watchKey string, keys []string) (result []KV, version uint32) {
	if len(watchKey) != 0 {
		_ = proxy.watchKeyMgr.Read(watchKey, func(keyVersion uint32) error {
			version = keyVersion
			txn := proxy.Db.NewTransaction(false)
			defer txn.Discard()

			for _, key := range keys {
				item, err := proxy.get(txn, key)
				result = append(result, KV{
					K:   key,
					V:   item,
					Err: err,
				})
			}
			return nil
		})
		return result, version
	} else {
		txn := proxy.Db.NewTransaction(false)
		defer txn.Discard()

		for _, key := range keys {
			item, err := proxy.get(txn, key)
			result = append(result, KV{
				K:   key,
				V:   item,
				Err: err,
			})
		}
		return result, 0
	}
}
func (proxy *DBProxy) set(key string, entry []byte) error {
	if err := proxy.cache.Set(key, entry); err != nil {
		return err
	}
	proxy.noSaveMap.Set(key, nil)
	return nil
}

// Sets TODO 改protobuf结构参数
func (proxy *DBProxy) Sets(watchKey string, keys []string, entryList [][]byte) error {
	if len(keys) != len(entryList) {
		return errors.New("len(keys) != len(entryList)")
	}
	if len(watchKey) != 0 {
		return proxy.watchKeyMgr.Write(watchKey, 0, false, func(keyVersion uint32) error {
			for i := 0; i < len(keys); i++ {
				err := proxy.set(keys[i], entryList[i])
				if err != nil {
					return err
				}
			}
			return nil
		})
	} else {
		for i := 0; i < len(keys); i++ {
			err := proxy.set(keys[i], entryList[i])
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func (proxy *DBProxy) SetsByVersion(watchKey string, keyVersion uint32, keys []string, entryList [][]byte) error {
	if len(keys) != len(entryList) {
		return errors.New("len(keys) != len(entryList)")
	}
	return proxy.watchKeyMgr.Write(watchKey, keyVersion, true, func(keyVersion uint32) error {
		for i := 0; i < len(keys); i++ {
			err := proxy.set(keys[i], entryList[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}
