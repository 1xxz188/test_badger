package proxy

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	cmap "github.com/orcaman/concurrent-map"
	"log"
	"sync"
	"sync/atomic"
	"test_badger/badgerApi"
	"test_badger/cachedb"
	"test_badger/controlEXE"
	"test_badger/customWatchKey"
	"test_badger/util"
	"time"
)

type GcInfo struct {
	GCMaxMs    int64
	GCMaxMB    int64
	GCRunOKCnt int64
}

type DBProxy struct {
	C                 *controlEXE.ControlEXE //协程生命周期控制器(生产消息协程,消费协程,GC协程)
	DB                *badger.DB             //底层数据
	GCInfo            GcInfo
	dbDir             string
	cache             cachedb.Cache               //缓存层
	watchKeyMgr       *customWatchKey.WatchKeyMgr //用于缓存层数据一致性控制
	noSaveMap         cmap.ConcurrentMap          //待保存列表
	cachePenetrateCnt uint64                      //缓存穿透次数
	_                 [56]byte                    //cpu cache
	cacheCnt          uint64                      //缓存命中次数
}

type KV struct {
	K   string `json:"key"`
	V   []byte `json:"value"`
	Err error  `json:"error"`
}

func CreateDBProxy(dbDir string, c *controlEXE.ControlEXE) (*DBProxy, error) {
	db, err := badger.Open(badgerApi.DefaultOptions(dbDir))
	if err != nil {
		return nil, err
	}

	if db == nil {
		return nil, errors.New("CreateDBProxy DB == nil")
	}
	if c == nil {
		c = controlEXE.CreateControlEXE()
	}

	watchKeyMgr, err := customWatchKey.New(1024)
	if err != nil {
		return nil, err
	}
	proxy := &DBProxy{
		DB:          db,
		C:           c,
		dbDir:       dbDir,
		watchKeyMgr: watchKeyMgr,
		noSaveMap:   cmap.New(),
	}

	cache, err := cachedb.NewBigCache(func(key string, entry []byte, reason cachedb.RemoveReason) {
		//进入缓存淘汰，不存在移除的时候同时插入(底层有分片锁)
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
		_ = proxy.DB.Update(func(txn *badger.Txn) error {
			fmt.Printf("warning save by remove key[%s] reason[%d]\n", key, reason)
			return txn.Set([]byte(key), entry)
		})
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

			wb := proxy.DB.NewWriteBatch()
			defer wb.Cancel()
			now := time.Now()
			startTm := now
			toSave := proxy.noSaveMap.Items()
			snapshotMs := time.Since(now).Milliseconds()
			now = time.Now()
			for key := range toSave {
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
func (proxy *DBProxy) Close() error {
	proxy.C.ProducerWait() //等待生产消息退出
	proxy.C.ConsumerWait() //等待数据落地
	proxy.C.AllWait()      //等待GC结束
	log.Println("main() exit!!!")
	return proxy.DB.Close()
}
func (proxy *DBProxy) GetDBDir() string {
	return proxy.dbDir
}
func (proxy *DBProxy) RunGC(gcRate float64) {
	proxy.C.AllAdd(1)
	defer proxy.C.AllDone()

	originalDirSize, err := util.GetDirSize(proxy.dbDir)
	if err != nil {
		panic(err)
	}
	log.Printf("Start GC Rate[%0.6f], originalDirSize[%d MB]\n", gcRate, originalDirSize)
	gcTicker := time.NewTicker(time.Second * 10)

	runGC := func() (int64, error) {
		if proxy.DB.IsClosed() {
			return 0, errors.New("find db.IsClosed() When GC")
		}
		beforeTm := time.Now()
		beforeSize, err := util.GetDirSize(proxy.dbDir)
		if err != nil {
			panic(err)
		}

		for {
			if proxy.DB.IsClosed() {
				return 0, errors.New("find db.IsClosed() When GC")
			}
			if err := proxy.DB.RunValueLogGC(gcRate); err != nil {
				if err == badger.ErrNoRewrite || err == badger.ErrRejected {
					break
				}

				log.Println("Err GC: ", err)
			} else {
				proxy.GCInfo.GCRunOKCnt++
			}
		}
		afterSize, err := util.GetDirSize(proxy.dbDir)
		if err != nil {
			return 0, err
		}
		diffGcMB := beforeSize - afterSize
		diffGcTm := time.Since(beforeTm)
		if proxy.GCInfo.GCMaxMs < diffGcTm.Milliseconds() {
			proxy.GCInfo.GCMaxMs = diffGcTm.Milliseconds()
		}
		if proxy.GCInfo.GCMaxMB < diffGcMB {
			proxy.GCInfo.GCMaxMB = diffGcMB
		}
		if diffGcMB != 0 {
			log.Printf("GC>[cost %s] size[%d MB]->[%d MB] diff[%d MB]\n", diffGcTm.String(), beforeSize, afterSize, diffGcMB)
		}
		return diffGcMB, nil
	}

	for {
		select {
		case <-proxy.C.CTXDone():
			afterSize, err := util.GetDirSize(proxy.dbDir)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Close Before DirSize[%d MB] originalDirSize[%d MB] diffMB[%d MB]\n", afterSize, originalDirSize, afterSize-originalDirSize)
			//立即触发一次GC
			_, err = runGC()
			if err != nil {
				log.Println(err)
			}

			wgGCExit := sync.WaitGroup{}
			wgGCExit.Add(1)
			exitGCChan := make(chan struct{})
			go func() {
				defer wgGCExit.Done()
				exitGCTicker := time.NewTicker(time.Second)

				for {
					select {
					case <-exitGCTicker.C:
						_, err := runGC()
						if err != nil {
							log.Println(err)
							return
						}
					case <-exitGCChan:
						gcSize, err := runGC() //最后一次GC
						if err != nil {
							log.Println(err)
						}
						log.Println("Last gcSize: ", gcSize)
						return
					}
				}
			}()
			proxy.C.ConsumerWait() //wait all data save
			close(exitGCChan)
			wgGCExit.Wait() //等待完全GC
			return
		case <-gcTicker.C:
			_, err := runGC()
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (proxy *DBProxy) GetCachePenetrateCnt() uint64 {
	return atomic.LoadUint64(&proxy.cachePenetrateCnt)
}

func (proxy *DBProxy) GetCacheCnt() uint64 {
	return atomic.LoadUint64(&proxy.cacheCnt)
}

func (proxy *DBProxy) GetCachePenetrateRate() float64 {
	return float64(proxy.GetCachePenetrateCnt()) / float64(proxy.GetCacheCnt())
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
			txn := proxy.DB.NewTransaction(false)
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
		txn := proxy.DB.NewTransaction(false)
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
