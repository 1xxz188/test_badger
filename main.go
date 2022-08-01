package main

import (
	"container/list"
	"errors"
	"fmt"
	badger "github.com/dgraph-io/badger/v3"
	cmap "github.com/orcaman/concurrent-map"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"test_badger/controlEXE"
	"test_badger/rateLimiter"
	"time"
	"unsafe"
)

type Collect struct {
	setCount         int64
	maxMs            int64
	totalMic         int64
	bigger10MsCount  int64
	bigger100MsCount int64
	bigger300MsCount int64
	bigger1SecCount  int64
}

var dataLen int

func StringToByteSlice(s string) []byte {
	tmp1 := (*[2]uintptr)(unsafe.Pointer(&s))
	tmp2 := [3]uintptr{tmp1[0], tmp1[1], tmp1[1]}
	return *(*[]byte)(unsafe.Pointer(&tmp2))
}

func GetDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size >> 20, err
}

func ByteSliceToString(bytes []byte) string {
	return *(*string)(unsafe.Pointer(&bytes))
}
func noUse(val []byte) {
}
func fnBatchUpdate(db *badger.DB, info *Collect, id int) error {
	wb := db.NewWriteBatch()
	defer wb.Cancel()

	data := func(l int) []byte {
		m := make([]byte, l)
		_, err := rand.Read(m)
		if err != nil {
			panic(err)
		}
		return m
	}
	insertData := data(dataLen)
	beginTm := time.Now()
	if err := wb.Set([]byte("Role_"+strconv.Itoa(10000000+id)), insertData); err != nil {
		return err
	}
	if err := wb.Set([]byte("Item_"+strconv.Itoa(10000000+id)), insertData); err != nil {
		return err
	}
	if err := wb.Set([]byte("Build_"+strconv.Itoa(10000000+id)), insertData); err != nil {
		return err
	}
	if err := wb.Set([]byte("Home_"+strconv.Itoa(10000000+id)), insertData); err != nil {
		return err
	}
	if err := wb.Set([]byte("Map_"+strconv.Itoa(10000000+id)), insertData); err != nil {
		return err
	}
	if err := wb.Flush(); err != nil {
		return err
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

func fnBatchRead(db *badger.DB, info *Collect, id int) error {
	txn := db.NewTransaction(false)
	defer txn.Discard()

	key1 := []byte("Role_" + strconv.Itoa(10000000+id))
	key2 := []byte("Item_" + strconv.Itoa(10000000+id))
	key3 := []byte("Build_" + strconv.Itoa(10000000+id))
	key4 := []byte("Home_" + strconv.Itoa(10000000+id))
	key5 := []byte("Map_" + strconv.Itoa(10000000+id))

	beginTm := time.Now()
	var val []byte
	item, err := txn.Get(key1)
	if err != nil {
		return err
	}
	val, err = item.ValueCopy(nil)
	item, err = txn.Get(key2)
	if err != nil {
		return err
	}
	val, err = item.ValueCopy(nil)
	item, err = txn.Get(key3)
	if err != nil {
		return err
	}
	val, err = item.ValueCopy(nil)
	item, err = txn.Get(key4)
	if err != nil {
		return err
	}
	val, err = item.ValueCopy(nil)
	item, err = txn.Get(key5)
	if err != nil {
		return err
	}
	val, err = item.ValueCopy(nil)
	if err != nil {
		return err
	}
	noUse(val)
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
func fnBatchUpdate2(db *DBProxy, info *Collect, id int) error {
	data := func(l int) []byte {
		m := make([]byte, l)
		_, err := rand.Read(m)
		if err != nil {
			panic(err)
		}
		return m
	}
	insertData := data(dataLen)

	var keyList []string
	keyList = append(keyList, "Role_"+strconv.Itoa(10000000+id))
	keyList = append(keyList, "Item_"+strconv.Itoa(10000000+id))
	keyList = append(keyList, "Build_"+strconv.Itoa(10000000+id))
	keyList = append(keyList, "Home_"+strconv.Itoa(10000000+id))
	keyList = append(keyList, "Map_"+strconv.Itoa(10000000+id))

	var valueList [][]byte
	valueList = append(valueList, insertData)
	valueList = append(valueList, insertData)
	valueList = append(valueList, insertData)
	valueList = append(valueList, insertData)
	valueList = append(valueList, insertData)
	beginTm := time.Now()

	watchKey := "Watch_" + strconv.Itoa(10000000+id)
	err := db.Sets(watchKey, keyList, valueList)
	if err != nil {
		return err
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

func fnBatchRead2(db *DBProxy, info *Collect, id int) error {
	key1 := "Role_" + strconv.Itoa(10000000+id)
	key2 := "Item_" + strconv.Itoa(10000000+id)
	key3 := "Build_" + strconv.Itoa(10000000+id)
	key4 := "Home_" + strconv.Itoa(10000000+id)
	key5 := "Map_" + strconv.Itoa(10000000+id)

	txn := db.db.NewTransaction(false)
	defer txn.Discard()

	beginTm := time.Now()
	item, err := db.Get(txn, key1)
	if err != nil {
		return err
	}
	item, err = db.Get(txn, key2)
	if err != nil {
		return err
	}
	item, err = db.Get(txn, key3)
	if err != nil {
		return err
	}
	item, err = db.Get(txn, key4)
	if err != nil {
		return err
	}
	item, err = db.Get(txn, key5)
	if err != nil {
		return err
	}
	noUse(item)
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
func Print(db *badger.DB) {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	itr := txn.NewIterator(badger.DefaultIteratorOptions)
	defer itr.Close()
	count := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		count++
		k := itr.Item().Key()
		v, err := itr.Item().ValueCopy(nil)
		if err != nil {
			panic(err)
		}
		log.Printf("%s: v_len[%d]\n", string(k), len(v))
	}
	log.Printf("all keys: %d\n", count)
}
func GetDBCount(db *badger.DB) uint64 {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = false
	itr := txn.NewIterator(opt)
	defer itr.Close()
	count := uint64(0)
	for itr.Rewind(); itr.Valid(); itr.Next() {
		count++
	}
	return count
}
func GetPreDBCount(db *badger.DB, prefix string) uint64 {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = false
	opt.Prefix = []byte(prefix)
	itr := txn.NewIterator(opt)
	defer itr.Close()
	count := uint64(0)
	for itr.Rewind(); itr.Valid(); itr.Next() {
		count++
	}
	return count
}
func PrintV(db *badger.DB) {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	itr := txn.NewIterator(badger.DefaultIteratorOptions)
	defer itr.Close()
	count := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		count++
		k := itr.Item().Key()
		v, err := itr.Item().ValueCopy(nil)
		if err != nil {
			panic(err)
		}
		log.Printf("[%d] [version: %d]> %s: %s UserMeta[%d]\n", count, itr.Item().Version(), string(k), string(v), itr.Item().UserMeta())
	}
}

type RateLimiter struct {
	limit        int
	interval     time.Duration
	times        list.List
	forceDisable bool
}

// shouldRateLimit saves the now as time taken or returns an error if
// in the limit of rate limiting
func (r *RateLimiter) shouldRateLimit(now time.Time) bool {
	if r.times.Len() < r.limit {
		r.times.PushBack(now)
		return false
	}

	front := r.times.Front()
	if diff := now.Sub(front.Value.(time.Time)); diff < r.interval {
		return true
	}

	front.Value = now
	r.times.MoveToBack(front)
	return false
}
func (r *RateLimiter) RateWait() {
	if r.forceDisable {
		return
	}
	for {
		now := time.Now()
		if r.shouldRateLimit(now) {
			time.Sleep(time.Millisecond)
			//runtime.Gosched()
			continue
		}
		break
	}
}

func main() {
	go func() {
		fmt.Println(http.ListenAndServe("0.0.0.0:58000", nil))
	}()
	cmap.SHARD_COUNT = 1024
	//./go_badger --op="get-set" -c 12 --stayTm="1m" --totalLimit=20000 --beginId=10000 --sendLimit=1 --dataSize=128
	op := kingpin.Flag("op", "[insert] [get] [set] [get-set]").Default("get-set").String()
	lsmMaxValue := kingpin.Flag("lsmMaxValue", "with value threshold for lsm").Default("65").Int64() //大于指针大小即可
	coroutines := kingpin.Flag("coroutines", "logic coroutines for client").Short('c').Default("4").Int()
	dataSize := kingpin.Flag("dataSize", "data size of send").Default("128").Int()
	kGcRate := kingpin.Flag("gcRate", "gc for value log").Default("0.7").Float64()

	//insert
	kInsertNum := kingpin.Flag("insertNum", "[insert] insert num").Default("2000000").Int()

	//get-set or get
	kOnlineNum := kingpin.Flag("onlineNum", "[get-set] online num").Default("10000").Int()
	kStep := kingpin.Flag("step", "[get-set] step").Default("1000").Int()
	kStayTm := kingpin.Flag("stayTm", "[get-set] stay time[ns, us, ms, s, m, h]").Default("1m30s").String()
	kLimitStepCnt := kingpin.Flag("limitStepCnt", "[get-set] total limit step cnt").Default("5").Int()
	kCurBeginId := kingpin.Flag("beginId", "[get-set] begin id").Default("10000").Int()
	kSendLimit := kingpin.Flag("sendLimit", "[get-set] send count for per ms").Default("1").Int()

	kingpin.HelpFlag.Short('h')
	kingpin.Version("v0.0.1")
	kingpin.Parse()

	//insert> ./go_badger --op="insert" -c 6 --insertNum=2000000
	//get-set> ./go_badger --op="get-set" -c 6 --stayTm="1m" --totalLimit=20000 --beginId=10000 --sendLimit=40

	//insert> ./go_badger --op="insert" -c 6 --insertNum=2000000 --lsmMaxValue=512
	if *dataSize <= 0 {
		panic("dataSize should be >= 0")
	}

	dataLen = *dataSize

	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	openTm := time.Now()
	dir := "./data"
	opt := badger.DefaultOptions(dir).
		WithCompactL0OnClose(true).      //退出处理LO压缩
		WithDetectConflicts(false).      //禁用版本冲突(由业务层保障)
		WithBlockCacheSize(2 << 30).     //如果加密和压缩开启时，需要开启，否则关闭
		WithValueThreshold(*lsmMaxValue) // 小值放LSM树，默认1MB，  游戏业务或许用2KB 会比较好？
	db, err := badger.Open(opt)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		log.Println("main() exit!!!")
		db.Close()
	}()

	originalDirSize, err := GetDirSize(dir)
	if err != nil {
		panic(err)
	}
	log.Printf("openTm[%d ms] dataLen[%d], lsmMaxValue[%d] originalDirSize[%d MB]\n", time.Since(openTm).Milliseconds(), dataLen, *lsmMaxValue, originalDirSize)

	if err != nil {
		panic(err)
	}
	proxyDB, err := CreateDBProxy(db, controlEXE.CreateControlEXE())
	if err != nil {
		panic(err)
	}
	chId := make(chan int, 1024*20)

	goSendCnt := *coroutines
	totalSendCnt := int64(0)
	isClose := false

	for i := 0; i < goSendCnt; i++ {
		proxyDB.c.ProducerAdd(1)
		go func(sendId int) {
			chClose := make(chan struct{})

			defer func() {
				close(chClose)
				proxyDB.c.ProducerDone()
			}()

			var updateInfo Collect
			var getInfo Collect
			go func() {
				ticker := time.NewTicker(time.Second * 10)
				for { //循环
					select {
					case <-ticker.C:
						if updateInfo.setCount > 0 {
							log.Printf("[%d]updateInfo> sendCnt[%d] avg[%d us] max[%d ms] >=10ms[%d] >=100ms[%d] >=300ms[%d] >=1000ms[%d]\n",
								sendId, updateInfo.setCount, updateInfo.totalMic/updateInfo.setCount, updateInfo.maxMs, updateInfo.bigger10MsCount, updateInfo.bigger100MsCount, updateInfo.bigger300MsCount, updateInfo.bigger1SecCount)
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

			if *op == "insert" || *op == "set" {
				for id := range chId {
					if err := fnBatchUpdate2(proxyDB, &updateInfo, id); err != nil {
						panic(err)
					}
				}
			} else if *op == "get" {
				for id := range chId {
					if err := fnBatchRead2(proxyDB, &getInfo, id); err != nil {
						panic(err)
					}
				}
			} else {
				for id := range chId {
					if err := fnBatchRead2(proxyDB, &getInfo, id); err != nil {
						panic(err)
					}
					if err := fnBatchUpdate2(proxyDB, &updateInfo, id); err != nil {
						panic(err)
					}
				}
			}

			if updateInfo.setCount > 0 {
				log.Printf("[%d]updateInfo> over sendCnt[%d] avg[%d us] max[%d ms] >=10ms[%d] >=100ms[%d] >=300ms[%d] >=1000ms[%d]\n",
					sendId, updateInfo.setCount, updateInfo.totalMic/updateInfo.setCount, updateInfo.maxMs, updateInfo.bigger10MsCount, updateInfo.bigger100MsCount, updateInfo.bigger300MsCount, updateInfo.bigger1SecCount)
			}
			if getInfo.setCount > 0 {
				log.Printf("[%d]getInfo> over sendCnt[%d] avg[%d us] max[%d ms] >=10ms[%d] >=100ms[%d] >=300ms[%d] >=1000ms[%d]\n",
					sendId, getInfo.setCount, getInfo.totalMic/getInfo.setCount, getInfo.maxMs, getInfo.bigger10MsCount, getInfo.bigger100MsCount, getInfo.bigger300MsCount, getInfo.bigger1SecCount)
			}
			atomic.AddInt64(&totalSendCnt, updateInfo.setCount)
			atomic.AddInt64(&totalSendCnt, getInfo.setCount)
		}(i)
	}

	sendFn := func() {
		defer close(chId)
		onlineNum := *kOnlineNum //同时在线人数
		if onlineNum <= 0 {
			panic("onlineNum <= 0")
		}

		step := *kStep
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
		r := rateLimiter.NewBucket(time.Millisecond, 500)
		log.Printf("limitStepCnt[%d] onlineNum[%d], curBeginId[%d] step[%d] stayTm[%s], rate[%d qps]", limitStepCnt, onlineNum, curBeginId, step, stayTm.String(), int(time.Second/time.Millisecond)*(*kSendLimit))

		beginTm := time.Now()
		r.Wait(1)
		limit := *kSendLimit
		cntShould := limit
		for {
			for id := curBeginId; id < curBeginId+onlineNum; id++ {
				if isClose {
					log.Println("检测到提前停止, 结束发送")
					proxyDB.c.CTXCancel()
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
				log.Printf("全部执行完毕! curStepCnt[%d] >= limitStepCnt[%d]\n", curStepCnt, limitStepCnt)
				proxyDB.c.CTXCancel()
				return
			}
			curStepCnt++
			curBeginId += step
			beginTm = time.Now()
			log.Printf("新一轮开始 curBeginId[%d] Step[%d/%d]\n", curBeginId, curStepCnt, limitStepCnt)
		}
	}
	switch *op {
	case "insert":
		//全力插入
		go func() {
			defer close(chId)
			log.Printf("begin insert num[%d]\n", *kInsertNum)
			for i := 0; i < *kInsertNum; i++ {
				if isClose {
					proxyDB.c.CTXCancel()
					return
				}
				chId <- i
			}
			proxyDB.c.CTXCancel()
		}()
	case "get-set":
		go sendFn()
	case "get":
		go sendFn()
	case "set":
		go sendFn()
	default:
		log.Println("Err op")
		return
	}

	var gcMaxMs int64
	var gcMaxMB int64
	var gcRunOKCnt int64
	proxyDB.c.AllAdd(1)
	go func() {
		defer proxyDB.c.AllDone()

		gcRate := *kGcRate
		log.Printf("Start GC Rate[%0.6f]\n", gcRate)
		gcTicker := time.NewTicker(time.Second * 10)

		runGC := func() (int64, error) {
			if db.IsClosed() {
				return 0, errors.New("find db.IsClosed() When GC")
			}
			beforeTm := time.Now()
			beforeSize, err := GetDirSize(dir)
			if err != nil {
				panic(err)
			}

			for {
				if db.IsClosed() {
					return 0, errors.New("find db.IsClosed() When GC")
				}
				if err := db.RunValueLogGC(gcRate); err != nil {
					if err == badger.ErrNoRewrite || err == badger.ErrRejected {
						break
					}

					log.Println("Err GC: ", err)
				} else {
					gcRunOKCnt++
				}
			}
			afterSize, err := GetDirSize(dir)
			if err != nil {
				return 0, err
			}
			diffGcMB := beforeSize - afterSize
			diffGcTm := time.Since(beforeTm)
			if gcMaxMs < diffGcTm.Milliseconds() {
				gcMaxMs = diffGcTm.Milliseconds()
			}
			if gcMaxMB < diffGcMB {
				gcMaxMB = diffGcMB
			}
			if diffGcMB != 0 {
				log.Printf("GC>[cost %s] size[%d MB]->[%d MB] diff[%d MB]\n", diffGcTm.String(), beforeSize, afterSize, diffGcMB)
			}
			return diffGcMB, nil
		}

		for {
			select {
			case <-proxyDB.c.CTXDone():
				afterSize, err := GetDirSize(dir)
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
				proxyDB.c.ConsumerWait() //wait all data save
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
	}()
	sg := make(chan os.Signal, 1)
	go func() {
		<-proxyDB.c.CTXDone()
		proxyDB.c.ProducerWait()
		diffExeTm := time.Since(openTm)
		log.Printf("EXE Tm[%s] totalSendCnt: %d, QPS: %0.3f --> %0.3f, gcMax[%d ms] gcMax[%d MB] gcRunOKCnt[%d]\n", diffExeTm.String(), totalSendCnt, float64(totalSendCnt)/diffExeTm.Seconds(), float64(totalSendCnt)/diffExeTm.Seconds()/2, gcMaxMs, gcMaxMB, gcRunOKCnt)

		proxyDB.c.ConsumerWait()
		diffWG2Tm := time.Since(openTm)
		log.Printf("WG2Wait All EXE Tm[%s]", diffWG2Tm.String())

		proxyDB.c.AllWait()
		diffWG3Tm := time.Since(openTm)
		log.Printf("WG3Wait All EXE Tm[%s]", diffWG3Tm.String())

		sg <- syscall.SIGINT
	}()

	signal.Notify(sg, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)
	select {
	case <-sg:
		fmt.Println("the app will shutdown.")
		proxyDB.c.CTXCancel()
		isClose = true
		proxyDB.c.AllWait()
	}

	now := time.Now()
	fmt.Printf("DBCount: %d, cost: %s\n", GetDBCount(db), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Role_ cost: %s\n", GetPreDBCount(db, "Role_"), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Item_ cost: %s\n", GetPreDBCount(db, "Item_"), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Build_ cost: %s\n", GetPreDBCount(db, "Build_"), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Home_ cost: %s\n", GetPreDBCount(db, "Home_"), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Map_ cost: %s\n", GetPreDBCount(db, "Map_"), time.Since(now).String())

	fmt.Printf("GetCachePenetrateCnt: %d\n", proxyDB.GetCachePenetrateCnt())
}
