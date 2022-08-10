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
	"test_badger/serialize"
	"test_badger/serialize/json"
	"test_badger/util"
	"time"
)

type GcInfo struct {
	GCMaxMs    int64
	GCMaxMB    int64
	GCRunOKCnt int64
}

type Proxy struct {
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
	serializer        serialize.Serializer
	_                 [56]byte //cpu
	rmButNotDelCnt    uint64   //淘汰内存次数(非正常过期)
	timerSaveCnt      uint64   //定期执行写入key数
	noSaveMapFlagCnt  uint64   //设置次数
	RmButNotFind      uint64
}

func CreateDBProxy(opt Opts) (*Proxy, error) {
	db, err := badger.Open(opt.optBadger)
	if err != nil {
		return nil, err
	}

	watchKeyMgr, err := customWatchKey.New(1024)
	if err != nil {
		return nil, err
	}
	proxy := &Proxy{
		DB:          db,
		C:           opt.c,
		cache:       opt.cache,
		dbDir:       opt.optBadger.Dir,
		watchKeyMgr: watchKeyMgr,
		noSaveMap:   cmap.New(),
		serializer:  json.NewSerializer(),
	}

	//移除缓存回调
	*opt.fnRemoveButNotDel = func(key string, entry []byte) {
		//这里尽量不阻塞! 会影响前台分片的读写锁时间
		//proxy.waitToRemoveMap.Set(key, nil)
		isRm := proxy.noSaveMap.RemoveCb(key, func(key string, _ interface{}, exists bool) bool {
			if !exists {
				return false
			}
			atomic.AddUint64(&proxy.RmButNotFind, 1) //TODO 删除
			return true
		})
		//proxy.noSaveMap.Remove(key)
		if !isRm {
			//atomic.AddUint64(&proxy.RmButNotFind, 1) //TODO 打开
			return
		}

		//TODO 添加记录日志。程序能进入到这里，说明定时保存的速度已不够(生产速度大于消费速度)。需要添加内存上限，或者优化程序，提高解码效率
		//fmt.Printf("warning save by remove key[%s]\n", key)
		atomic.AddUint64(&proxy.rmButNotDelCnt, 1)

		_ = proxy.DB.Update(func(txn *badger.Txn) error {
			//TODO 优化不用解析直接判断是否过期
			var kv badgerApi.KV
			e := badger.NewEntry([]byte(key), entry)
			err = proxy.serializer.Unmarshal(entry, &kv)
			if err != nil {
				panic(err)
			}

			if kv.ExpiresAt > 0 {
				t := time.Unix(int64(kv.ExpiresAt), 0)
				diffSec := time.Now().Sub(t).Seconds()
				if diffSec < 1 {
					return nil
				}
				e.WithTTL(time.Duration(diffSec) * time.Second)
			}
			return txn.SetEntry(e)
		})
	}

	//开启定时保存协程
	proxy.C.ConsumerAdd(1)
	go func() {
		defer proxy.C.ConsumerDone()
		//interval save
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case _ = <-ticker.C:
				proxy.SaveData()
			case <-proxy.C.CTXDone():
				//wait all data save
				proxy.C.ProducerWait() //wait all send data coroutine exit
				//time.Sleep(time.Second * 10)
				fmt.Println("wait all data save")
				proxy.SaveData()
				fmt.Println("all data save ok")
				return
			}
		}
	}()

	return proxy, nil
}

func (proxy *Proxy) SaveData() {
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

	var err error
	var kv badgerApi.KV
	var v []byte
	isGet := false
	for key := range toSave {
		isGet = false
		v, err = proxy.cache.GetCb(key, func() {
			isRm := proxy.noSaveMap.RemoveCb(key, func(key string, _ interface{}, exists bool) bool {
				if !exists {
					return false
				}
				atomic.AddUint64(&proxy.RmButNotFind, 1) //TODO 删除
				return true
			})
			//proxy.noSaveMap.Remove(key)
			if isRm {
				isGet = true
			}
		})

		if err != nil || !isGet {
			continue
		}

		// maxBatchCount int64 // max entries in batch    ---> 10w  MaxBatchSize()
		// maxBatchSize  int64 // max batch size in bytes ---> 9.5MB MaxBatchSize()
		//err = wb.Set([]byte(key), v)
		//TODO 优化不用解析直接判断是否过期
		e := badger.NewEntry([]byte(key), v)
		err = proxy.serializer.Unmarshal(v, &kv)
		if err != nil {
			panic(err)
		}

		if kv.ExpiresAt > 0 {
			t := time.Unix(int64(kv.ExpiresAt), 0)
			diffSec := now.Sub(t).Seconds()
			if diffSec < 1 {
				continue
			}
			e.WithTTL(time.Duration(diffSec) * time.Second)
		}
		atomic.AddUint64(&proxy.timerSaveCnt, 1)
		err = wb.SetEntry(e)
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

func (proxy *Proxy) Close() error {
	proxy.C.ProducerWait() //等待生产消息退出
	proxy.C.ConsumerWait() //等待数据落地
	proxy.C.AllWait()      //等待GC结束
	log.Println("main() exit!!!")
	return proxy.DB.Close()
}
func (proxy *Proxy) GetDBDir() string {
	return proxy.dbDir
}

func (proxy *Proxy) GetDBValue(key string) (*badgerApi.KV, error) {
	txn := proxy.DB.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	var kv badgerApi.KV
	err = proxy.serializer.Unmarshal(v, &kv)
	if err != nil {
		return nil, err
	}
	return &kv, nil
}

func (proxy *Proxy) GetSerializer() serialize.Serializer {
	return proxy.serializer
}

func (proxy *Proxy) RunGC(gcRate float64) {
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

func (proxy *Proxy) GetCachePenetrateCnt() uint64 {
	return atomic.LoadUint64(&proxy.cachePenetrateCnt)
}

func (proxy *Proxy) GetCacheCnt() uint64 {
	return atomic.LoadUint64(&proxy.cacheCnt)
}

func (proxy *Proxy) GetRMButNotDelCnt() uint64 {
	return atomic.LoadUint64(&proxy.rmButNotDelCnt)
}

func (proxy *Proxy) GetTimerSaveCnt() uint64 {
	return atomic.LoadUint64(&proxy.timerSaveCnt)
}
func (proxy *Proxy) GetNoSaveMapFlagCnt() uint64 {
	return atomic.LoadUint64(&proxy.noSaveMapFlagCnt)
}

func (proxy *Proxy) GetCachePenetrateRate() float64 {
	f := float64(proxy.GetCacheCnt())
	if f == 0 {
		return 0
	}
	return float64(proxy.GetCachePenetrateCnt()) / float64(proxy.GetCacheCnt())
}

func (proxy *Proxy) get(txn *badger.Txn, key string) *badgerApi.KV {
	data, err := proxy.cache.Get(key)
	//TODO 考虑池化kv对象
	var kv badgerApi.KV
	kv.K = key
	if err == nil {
		atomic.AddUint64(&proxy.cacheCnt, 1)
		err := proxy.serializer.Unmarshal(data, &kv)
		if err != nil {
			kv.Err = err.Error()
		}
		return &kv //命中缓存返回数据
	}
	if err != cachedb.ErrEntryNotFound {
		kv.Err = err.Error()
		return &kv
	}

	atomic.AddUint64(&proxy.cachePenetrateCnt, 1)

	//穿透到badger
	item, err := txn.Get([]byte(key))
	if err != nil {
		kv.Err = err.Error()
		if err == badger.ErrKeyNotFound { //转换错误
			kv.Err = cachedb.ErrEntryNotFound.Error()
		}
		return &kv
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		kv.Err = err.Error()
		return &kv
	}

	//更新到缓存中
	err = proxy.cache.Set(key, v)
	if err != nil {
		kv.Err = err.Error()
		return &kv
	}
	err = proxy.serializer.Unmarshal(v, &kv)
	if err != nil {
		kv.Err = err.Error()
	}
	return &kv
}

func (proxy *Proxy) Gets(watchKey string, keys []string) (result []*badgerApi.KV, version uint32) {
	if len(watchKey) != 0 {
		_ = proxy.watchKeyMgr.Read(watchKey, func(keyVersion uint32) error {
			version = keyVersion
			txn := proxy.DB.NewTransaction(false)
			defer txn.Discard()

			for _, key := range keys {
				result = append(result, proxy.get(txn, key))
			}
			return nil
		})
		return result, version
	} else {
		txn := proxy.DB.NewTransaction(false)
		defer txn.Discard()

		for _, key := range keys {
			result = append(result, proxy.get(txn, key))
		}
		return result, 0
	}
}
func (proxy *Proxy) set(kv *badgerApi.KV) error {
	v, err := proxy.serializer.Marshal(kv)
	if err != nil {
		return err
	}
	if err := proxy.cache.SetCb(kv.K, v, func() {
		proxy.noSaveMap.Set(kv.K, nil)
	}); err != nil {
		return err
	}
	atomic.AddUint64(&proxy.noSaveMapFlagCnt, 1)
	return nil
}

// Sets TODO 改protobuf结构参数
func (proxy *Proxy) Sets(watchKey string, kvs []badgerApi.KV) error {
	if len(watchKey) != 0 {
		return proxy.watchKeyMgr.Write(watchKey, 0, false, func(keyVersion uint32) error {
			for i := 0; i < len(kvs); i++ {
				err := proxy.set(&kvs[i])
				if err != nil {
					return err
				}
			}
			return nil
		})
	} else {
		for i := 0; i < len(kvs); i++ {
			err := proxy.set(&kvs[i])
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// SetsByVersion 注意: keyVersion为0也是有效版本,因为允许翻转
func (proxy *Proxy) SetsByVersion(watchKey string, keyVersion uint32, kvs []badgerApi.KV) error {
	return proxy.watchKeyMgr.Write(watchKey, keyVersion, true, func(keyVersion uint32) error {
		for i := 0; i < len(kvs); i++ {
			err := proxy.set(&kvs[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}