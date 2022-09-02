package main

import (
	"fmt"
	"github.com/shopspring/decimal"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"test_badger/badgerApi"
	"test_badger/customWatchKey"
	"test_badger/logger"
	"test_badger/proxy"
	"test_badger/rateLimiter"
	"test_badger/util"
	"test_badger/web"
	"time"
)

type Collect struct {
	setCount         int64
	maxMs            int64
	totalMic         int64
	bigger10MsCount  int64
	bigger100MsCount int64
	bigger300MsCount int64
	bigger1SecCount  int64
	conflictsCount   int64 //版本冲突次数
	maxRetryCount    int64
}

var revMsgCnt int64

func fnBatchUpdate2(db *proxy.Proxy, info *Collect, id int, dataLen int, version uint32, isCheckVersion bool) error {
	insertData := util.RandData(dataLen)
	flagData := []byte(fmt.Sprintf("value_%d", info.setCount))
	copy(insertData[0:len(flagData)], flagData)
	kvs := append([]badgerApi.KV(nil),
		badgerApi.KV{
			K: "Role_" + strconv.Itoa(10000000+id),
			V: insertData,
		},
		badgerApi.KV{
			K: "Item_" + strconv.Itoa(10000000+id),
			V: insertData,
		},
		badgerApi.KV{
			K: "Build_" + strconv.Itoa(10000000+id),
			V: insertData,
		},
		badgerApi.KV{
			K: "Home_" + strconv.Itoa(10000000+id),
			V: insertData,
		},
		badgerApi.KV{
			K: "Map_" + strconv.Itoa(10000000+id),
			V: insertData,
		},
	)

	beginTm := time.Now()

	if isCheckVersion {
		watchKey := "Watch_" + strconv.Itoa(10000000+id)
		err := db.SetsByWatch(watchKey, version, kvs)
		if err != nil {
			return err
		}
	} else {
		err := db.Sets(kvs)
		if err != nil {
			return err
		}
	}

	diffMS := time.Since(beginTm).Milliseconds()
	diffMic := time.Since(beginTm).Microseconds()
	info.totalMic += diffMic
	if diffMS > info.maxMs {
		info.maxMs = diffMS
	}
	if diffMS >= 10 {
		info.bigger10MsCount++

		if diffMS >= 100 {
			info.bigger100MsCount++

			if diffMS >= 300 {
				info.bigger300MsCount++

				if diffMS >= 1000 {
					info.bigger1SecCount++
				}
			}
		}
	}
	info.setCount++
	return nil
}
func fnBatchRead2(db *proxy.Proxy, info *Collect, id int, isCheckVersion bool) (uint32, error) { //return (version, err)
	keyList := append([]string(nil),
		"Role_"+strconv.Itoa(10000000+id),
		"Item_"+strconv.Itoa(10000000+id),
		"Build_"+strconv.Itoa(10000000+id),
		"Home_"+strconv.Itoa(10000000+id),
		"Map_"+strconv.Itoa(10000000+id),
	)

	watchKey := "Watch_" + strconv.Itoa(10000000+id)
	beginTm := time.Now()

	var items []*badgerApi.KV
	var version uint32
	if isCheckVersion {
		items, version = db.GetsByWatch(watchKey, keyList)
		if len(items) != len(keyList) {
			panic("len(items) != len(keyList)")
		}
	} else {
		items = db.Gets(keyList)
		if len(items) != len(keyList) {
			panic("len(items) != len(keyList)")
		}
	}
	for _, item := range items {
		if item.Err != "" {
			logger.Log.Fatalf("id[%d] key[%s] %s", id, item.K, item.Err)
		}
	}

	diffMS := time.Since(beginTm).Milliseconds()
	diffMic := time.Since(beginTm).Microseconds()
	info.totalMic += diffMic
	if diffMS > info.maxMs {
		info.maxMs = diffMS
	}
	if diffMS >= 10 {
		info.bigger10MsCount++

		if diffMS >= 100 {
			info.bigger100MsCount++

			if diffMS >= 300 {
				info.bigger300MsCount++

				if diffMS >= 1000 {
					info.bigger1SecCount++
				}
			}
		}
	}
	info.setCount++
	return version, nil
}

//work 处理数据
func work(proxyDB *proxy.Proxy, coroutines int, op string, chId chan int, dataLen int, totalSendCnt *int64) {
	for i := 0; i < coroutines; i++ {
		proxyDB.C.ProducerAdd(1)
		go func(sendId int) {
			chClose := make(chan struct{})

			defer func() {
				close(chClose)
				proxyDB.C.ProducerDone()
			}()

			var updateInfo Collect
			var getInfo Collect
			go func() {
				ticker := time.NewTicker(time.Second * 10)
				for { //循环
					select {
					case <-ticker.C:
						if updateInfo.setCount > 0 {
							var b float64
							if updateInfo.conflictsCount > 0 {
								b, _ = decimal.NewFromFloat(float64(updateInfo.conflictsCount) / float64(updateInfo.setCount)).Round(6).Float64()
							}

							log.Printf("[%d]updateInfo> sendCnt[%d]-conflicts[%d] conflictsRate[%f] maxRetryCount[%d] avg[%d us] max[%d ms] >=10ms[%d] >=100ms[%d] >=300ms[%d] >=1000ms[%d]\n",
								sendId, updateInfo.setCount, updateInfo.conflictsCount, b, updateInfo.maxRetryCount, updateInfo.totalMic/updateInfo.setCount, updateInfo.maxMs, updateInfo.bigger10MsCount, updateInfo.bigger100MsCount, updateInfo.bigger300MsCount, updateInfo.bigger1SecCount)
						}

						if getInfo.setCount > 0 {
							log.Printf("[%d]getInfo> sendCnt[%d] avg[%d us] max[%d ms] >=10ms[%d] >=100ms[%d] >=300ms[%d] >=1000ms[%d]\n",
								sendId, getInfo.setCount, getInfo.totalMic/getInfo.setCount, getInfo.maxMs, getInfo.bigger10MsCount, getInfo.bigger100MsCount, getInfo.bigger300MsCount, getInfo.bigger1SecCount)
						}
					case <-chClose:
						return
					}
				}
			}()

			if op == "insert" || op == "set" {
				for id := range chId {
					atomic.AddInt64(&revMsgCnt, 1)
					if err := fnBatchUpdate2(proxyDB, &updateInfo, id, dataLen, 0, false); err != nil {
						panic(err)
					}
				}
			} else if op == "get" {
				for id := range chId {
					if _, err := fnBatchRead2(proxyDB, &getInfo, id, false); err != nil {
						panic(err)
					}
				}
			} else {
				retryCount := int64(0)
				for id := range chId {
					retryCount = 0
					for {
						version, err := fnBatchRead2(proxyDB, &getInfo, id, true)
						if err != nil {
							panic(err)
						}
						//fmt.Printf("sendId[%d] read id: %d, version: %d\n", sendId, id, version)
						if err := fnBatchUpdate2(proxyDB, &updateInfo, id, dataLen, version, true); err != nil {
							if err == customWatchKey.ErrWatchVersionConflicts {
								updateInfo.conflictsCount++
								retryCount++
								if retryCount > updateInfo.maxRetryCount { //此次测试应许无限次重试
									updateInfo.maxRetryCount = retryCount
								}
								continue
							} else {
								panic(fmt.Sprintf("sendId[%d] err: %s, id: %d, version: %d", sendId, err, id, version))
							}
						}
						break
					}
				}
			}

			if updateInfo.setCount > 0 {
				var b float64
				if updateInfo.conflictsCount > 0 {
					b, _ = decimal.NewFromFloat(float64(updateInfo.conflictsCount) / float64(updateInfo.setCount)).Round(6).Float64()
				}

				log.Printf("[%d]updateInfo> over sendCnt[%d]-conflicts[%d] conflictsRate[%f] maxRetryCount[%d] avg[%d us] max[%d ms] >=10ms[%d] >=100ms[%d] >=300ms[%d] >=1000ms[%d]\n",
					sendId, updateInfo.setCount, updateInfo.conflictsCount, b, updateInfo.maxRetryCount, updateInfo.totalMic/updateInfo.setCount, updateInfo.maxMs, updateInfo.bigger10MsCount, updateInfo.bigger100MsCount, updateInfo.bigger300MsCount, updateInfo.bigger1SecCount)
			}
			if getInfo.setCount > 0 {
				log.Printf("[%d]getInfo> over sendCnt[%d] avg[%d us] max[%d ms] >=10ms[%d] >=100ms[%d] >=300ms[%d] >=1000ms[%d]\n",
					sendId, getInfo.setCount, getInfo.totalMic/getInfo.setCount, getInfo.maxMs, getInfo.bigger10MsCount, getInfo.bigger100MsCount, getInfo.bigger300MsCount, getInfo.bigger1SecCount)
			}
			atomic.AddInt64(totalSendCnt, updateInfo.setCount)
			atomic.AddInt64(totalSendCnt, getInfo.setCount)
		}(i)
	}
}

func main() {
	op := kingpin.Flag("op", "[insert] [get] [set] [get-set]").Default("get-set").String()
	lsmMaxValue := kingpin.Flag("lsmMaxValue", "with value threshold for lsm").Default("65").Int64() //大于指针大小即可
	coroutines := kingpin.Flag("coroutines", "logic coroutines for client").Short('c').Default("4").Int()
	dataSize := kingpin.Flag("dataSize", "data size of send").Default("128").Int()
	kGcRate := kingpin.Flag("gcRate", "gc for value log").Default("0.7").Float64()
	kFlushDbSec := kingpin.Flag("flushDbSec", "flush db data second").Default("10").Int32()

	//insert
	kInsertNum := kingpin.Flag("insertNum", "[insert] insert num").Default("2000000").Int()

	//get-set or get
	kOnlineNum := kingpin.Flag("onlineNum", "[get-set] online num").Default("10000").Int()
	kStepId := kingpin.Flag("stepId", "[get-set] id increase per round").Default("1000").Int()
	kStayTm := kingpin.Flag("stayTm", "[get-set] stay time[ns, us, ms, s, m, h]").Default("1m30s").String()
	kLimitStepCnt := kingpin.Flag("limitStepCnt", "[get-set] total limit step cnt").Default("5").Int()
	kCurBeginId := kingpin.Flag("beginId", "[get-set] begin id").Default("10000").Int()
	kSendLimit := kingpin.Flag("sendLimit", "[get-set] send count for per ms").Default("1").Int()
	kNoUseCache := kingpin.Flag("noUseCache", "not use cache").Bool()

	kPprof := kingpin.Flag("pprof", "ip:port of pprof listen addr").Default("0.0.0.0:11000").String()
	kWebAddr := kingpin.Flag("web", "ip:port of web listen addr").Default("0.0.0.0:11002").String()

	kingpin.HelpFlag.Short('h')
	kingpin.Version("v0.0.1")
	kingpin.Parse()

	if *dataSize <= 64 {
		panic("dataSize should be >= 0")
	}
	dataLen := *dataSize

	go func() {
		fmt.Println(http.ListenAndServe(*kPprof, nil))
	}()

	openTm := time.Now()
	log.Printf("openTm[%d ms] dataLen[%d], lsmMaxValue[%d] noUseCache[%t] flushDbSec[%d sec]\n", time.Since(openTm).Milliseconds(), dataLen, *lsmMaxValue, *kNoUseCache, *kFlushDbSec)

	var proxyDB *proxy.Proxy
	var err error
	if *kNoUseCache {
		proxyDB, err = proxy.CreateDBProxy(proxy.DefaultNoCache("./data").WithFlushDbSec(*kFlushDbSec))
		if err != nil {
			panic(err)
		}
	} else {
		proxyDB, err = proxy.CreateDBProxy(proxy.DefaultOptions("./data").WithFlushDbSec(*kFlushDbSec))
		if err != nil {
			panic(err)
		}
	}

	defer func() {
		err := proxyDB.Close()
		if err != nil {
			panic(err)
		}
	}()

	//开启GC
	go proxyDB.RunGC(*kGcRate)
	//开启网页查询
	go web.RunWeb(*kWebAddr, proxyDB)

	chId := make(chan int, 1024*80)
	totalSendCnt := int64(0)
	var isClose atomic.Value
	isClose.Store(false)

	//消费协程
	work(proxyDB, *coroutines, *op, chId, dataLen, &totalSendCnt)

	//生产协程
	sendFn := func() {
		defer close(chId)
		onlineNum := *kOnlineNum //同时在线人数
		if onlineNum <= 0 {
			panic("onlineNum <= 0")
		}

		stepId := *kStepId
		stayTm, err := time.ParseDuration(*kStayTm) //步长
		if err != nil {
			panic(err)
		}
		//stayTm := time.Minute //* 5 //步长持续时间
		limitStepCnt := *kLimitStepCnt //2000000 //总限制
		if limitStepCnt <= 0 {
			panic("limitStepCnt <= 0")
		}
		curStepCnt := 1
		curBeginId := *kCurBeginId
		if curBeginId < 0 {
			panic("curBeginId < 0")
		}
		/*r := &RateLimiter{
			limit:        *kSendLimit,
			interval:     time.Millisecond,
			forceDisable: false,
		}
		r.times.Init()*/
		r := rateLimiter.NewBucket(time.Millisecond, 500) //允许500个毫秒的突发抖动
		log.Printf("limitStepCnt[%d] onlineNum[%d], curBeginId[%d] stepId[%d] stayTm[%s], rate[%d qps]", limitStepCnt, onlineNum, curBeginId, stepId, stayTm.String(), int(time.Second/time.Millisecond)*(*kSendLimit))

		beginTm := time.Now()
		r.Wait(1)
		limit := *kSendLimit
		cntShould := limit
		for {
			for id := curBeginId; id < curBeginId+onlineNum; id++ {
				if isClose.Load().(bool) {
					log.Println("send check was closed!...")
					return
				}
				chId <- id //放入执行队列
				cntShould--
				if cntShould <= 0 {
					r.Wait(1)
					cntShould = limit
				}
			}
			//是否达到一轮时间
			if time.Since(beginTm) < stayTm {
				continue
			}
			//是否全部执行完毕
			if curStepCnt >= limitStepCnt {
				log.Printf("all executed! curStepCnt[%d] >= limitStepCnt[%d]\n", curStepCnt, limitStepCnt)
				proxyDB.C.CTXCancel()
				return
			}
			curStepCnt++
			curBeginId += stepId
			beginTm = time.Now()
			log.Printf("new turn curBeginId[%d] Step[%d/%d]\n", curBeginId, curStepCnt, limitStepCnt)
		}
	}
	switch *op {
	case "insert":
		//全力插入
		go func() {
			defer close(chId)
			log.Printf("begin insert num[%d]\n", *kInsertNum)
			curBeginId := *kCurBeginId
			for i := 0; i < *kInsertNum; i++ {
				if isClose.Load().(bool) {
					return
				}
				chId <- curBeginId + i
			}
			proxyDB.C.CTXCancel()
		}()
	case "get-set":
		fallthrough
	case "get":
		fallthrough
	case "set":
		go sendFn()
	default:
		log.Println("Err op")
		return
	}

	sg := make(chan os.Signal, 1)

	chExitInfo := make(chan struct{})
	go func() {
		<-proxyDB.C.CTXDone()
		proxyDB.C.ProducerWait()
		diffExeTm := time.Since(openTm)
		log.Printf("EXE Tm[%s] totalSendCnt: %d, QPS: %0.3f --> %0.3f, gcMax[%d ms] gcMax[%d MB] gcRunOKCnt[%d]\n", diffExeTm.String(), totalSendCnt, float64(totalSendCnt)/diffExeTm.Seconds(), float64(totalSendCnt)/diffExeTm.Seconds()/2, proxyDB.GCInfo.GCMaxMs, proxyDB.GCInfo.GCMaxMB, proxyDB.GCInfo.GCRunOKCnt)

		proxyDB.C.ConsumerWait()
		diffWG2Tm := time.Since(openTm)
		log.Printf("WG2Wait All EXE Tm[%s]", diffWG2Tm.String())

		proxyDB.C.AllWait()
		diffWG3Tm := time.Since(openTm)
		log.Printf("WG3Wait All EXE Tm[%s]", diffWG3Tm.String())

		sg <- syscall.SIGINT
		close(chExitInfo)
	}()

	signal.Notify(sg, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)
	select {
	case <-sg:
		fmt.Println("the app will shutdown.")
		proxyDB.C.CTXCancel()
		isClose.Store(true)
		<-chExitInfo
	}

	now := time.Now()
	fmt.Printf("DBCount: %d, cost: %s\n", badgerApi.GetDBCount(proxyDB.DB), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Role_ cost: %s\n", badgerApi.GetPreDBCount(proxyDB.DB, "Role_"), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Item_ cost: %s\n", badgerApi.GetPreDBCount(proxyDB.DB, "Item_"), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Build_ cost: %s\n", badgerApi.GetPreDBCount(proxyDB.DB, "Build_"), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Home_ cost: %s\n", badgerApi.GetPreDBCount(proxyDB.DB, "Home_"), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Map_ cost: %s\n", badgerApi.GetPreDBCount(proxyDB.DB, "Map_"), time.Since(now).String())

	fmt.Printf("GetCachePenetrateCnt: %d\n", proxyDB.GetCachePenetrateCnt())
	fmt.Printf("revMsgCnt: %d\n", atomic.LoadInt64(&revMsgCnt))
	fmt.Printf("RMButNotDelCnt: %d\n", proxyDB.GetRMButNotDelCnt())
	fmt.Printf("timerSaveCnt: %d\n", proxyDB.GetTimerSaveCnt())
	fmt.Printf("RmButNotFind: %d\n", atomic.LoadUint64(&proxyDB.RmButNotFind))
}
