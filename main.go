package main

import (
	"container/list"
	"fmt"
	badger "github.com/dgraph-io/badger/v3"
	cmap "github.com/orcaman/concurrent-map"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"math/rand"
	"test_badger/controlEXE"

	//"net/http"
	//_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
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
	beginTm := time.Now()
	if err := db.Set("Role_"+strconv.Itoa(10000000+id), insertData); err != nil {
		return err
	}
	if err := db.Set("Item_"+strconv.Itoa(10000000+id), insertData); err != nil {
		return err
	}
	if err := db.Set("Build_"+strconv.Itoa(10000000+id), insertData); err != nil {
		return err
	}
	if err := db.Set("Home_"+strconv.Itoa(10000000+id), insertData); err != nil {
		return err
	}
	if err := db.Set("Map_"+strconv.Itoa(10000000+id), insertData); err != nil {
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
func PrintDBCount(db *badger.DB) {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	itr := txn.NewIterator(badger.DefaultIteratorOptions)
	defer itr.Close()
	count := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		count++
	}
	log.Printf("all keys: %d\n", count)
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
	kTotalLimit := kingpin.Flag("totalLimit", "[get-set] total limit time sec").Default("20000").Int()
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
	defer db.Close()

	log.Printf("openTm[%d ms] dataLen[%d], lsmMaxValue[%d]\n", time.Since(openTm).Milliseconds(), dataLen, *lsmMaxValue)

	proxyDB, err := CreateDBProxy(db, controlEXE.CreateControlEXE())

	chId := make(chan int, 1024*20)

	goSendCnt := *coroutines
	totalSendCnt := int64(0)
	isClose := false

	for i := 0; i < goSendCnt; i++ {
		proxyDB.c.WGAdd(1)
		go func(sendId int) {
			chClose := make(chan struct{})

			defer func() {
				close(chClose)
				proxyDB.c.WGDone()
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
		totalLimit := *kTotalLimit //2000000 //总限制
		if totalLimit <= 0 {
			panic("totalLimit <= 0")
		}
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
		log.Printf("totalLimit[%d] onlineNum[%d], curBeginId[%d] step[%d] stayTm[%s], rate[%d qps]", totalLimit, onlineNum, curBeginId, step, stayTm.String(), int(time.Second/time.Millisecond)*(*kSendLimit))

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
				if id >= totalLimit {
					break
				}

				//放入执行队列
				chId <- id
				cntShould--
				if cntShould <= 0 {
					r.Wait(1)
					cntShould = limit
				}
			}

			//是否全部执行完毕
			if curBeginId >= totalLimit {
				log.Println("全部执行完毕!")
				proxyDB.c.CTXCancel()
				return
			}

			//是否达到一轮时间
			if time.Since(beginTm) < stayTm {
				continue
			}

			curBeginId += step
			beginTm = time.Now()
			log.Printf("新一轮开始 curBeginId[%d]\n", curBeginId)
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
	go func() {
		gcRate := *kGcRate
		log.Printf("Start GC Rate[%0.6f]\n", gcRate)
		for !db.IsClosed() {
			beforeTm := time.Now()
			beforeSize, err := GetDirSize(dir)
			if err != nil {
				panic(err)
			}

			for {
				if db.IsClosed() {
					return
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
				panic(err)
			}
			diffGcMB := beforeSize - afterSize
			diffGcTm := time.Since(beforeTm)
			if gcMaxMs < diffGcTm.Milliseconds() {
				gcMaxMs = diffGcTm.Milliseconds()
			}
			if gcMaxMB < diffGcMB {
				gcMaxMB = diffGcMB
			}
			log.Printf("GC>[cost %s] size[%d MB]->[%d MB] diff[%d MB]\n", diffGcTm.String(), beforeSize, afterSize, diffGcMB)
			time.Sleep(time.Second * 10)
		}
	}()
	sg := make(chan os.Signal)
	go func() {
		<-proxyDB.c.CTXDone()
		proxyDB.c.WGWait()
		diffExeTm := time.Since(openTm)
		log.Printf("EXE Tm[%s] totalSendCnt: %d, QPS: %0.3f --> %0.3f, gcMax[%d ms] gcMax[%d MB] gcRunOKCnt[%d]\n", diffExeTm.String(), totalSendCnt, float64(totalSendCnt)/diffExeTm.Seconds(), float64(totalSendCnt)/diffExeTm.Seconds()/2, gcMaxMs, gcMaxMB, gcRunOKCnt)

		proxyDB.c.WG2Wait()
		diffSaveTm := time.Since(openTm)
		log.Printf("All EXE Tm[%s]", diffSaveTm.String())
		sg <- syscall.SIGINT
	}()

	signal.Notify(sg, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)
	select {
	case <-sg:
		fmt.Println("the app will shutdown.")
		proxyDB.c.CTXCancel()
		isClose = true
		proxyDB.c.WG2Wait()
	}
	//Print(db)
	// Your code here…
}
