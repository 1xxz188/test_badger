package main

import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"test_badger/badgerApi"
	"test_badger/proxy"
	"test_badger/util"
	"testing"
	"time"
)

func getTestOptions(dir string) badger.Options {
	opt := badger.DefaultOptions(dir).
		WithSyncWrites(false).
		WithLoggingLevel(badger.WARNING).
		WithDetectConflicts(false)
	return opt
}
func removeDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
}

func TestInsert(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("./data"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	var info Collect

	if err := fnBatchUpdate(db, &info, 1, 128); err != nil {
		t.Error(err)
	}
	if err := fnBatchUpdate(db, &info, 2, 128); err != nil {
		t.Error(err)
	}
	if err := fnBatchUpdate(db, &info, 2, 128); err != nil {
		t.Error(err)
	}
}

func TestPrintVersion(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("./myDb"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	var info Collect
	if err := fnBatchRead(db, &info, 1); err != nil {
		t.Error(err)
	}
	if err := fnBatchRead(db, &info, 2); err != nil {
		t.Error(err)
	}
	if err := fnBatchRead(db, &info, 100010); err != nil {
		t.Error(err)
	}
}

func TestInsertVlog(t *testing.T) {
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%d%10d", i, i))
	}
	data := func(l int) []byte {
		m := make([]byte, l)
		_, err := rand.Read(m)
		require.NoError(t, err)
		return m
	}

	dir := "./myDb"
	opt := badger.DefaultOptions(dir).
		WithCompactL0OnClose(true).  //退出处理LO压缩
		WithDetectConflicts(false).  //禁用版本冲突(由业务层保障)
		WithBlockCacheSize(1 << 30). //如果加密和压缩开启时，需要开启，否则关闭
		WithValueThreshold(1)        // 小值放LSM树，默认1MB，  游戏业务或许用2KB 会比较好？
	db, err := badger.Open(opt)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	for y := 0; y < 0; y++ {
		for i := 0; i < 10; i++ {
			err = db.Update(func(txn *badger.Txn) error {
				return txn.SetEntry(badger.NewEntry(key(i), data(2048)))
			})
		}
	}

	require.NoError(t, err)

	size, err := util.GetDirSize(dir)
	require.NoError(t, err)
	log.Printf("Before run GC DB Size = %dMB\n", size)

	beforeTm := time.Now()
	gcRate := 0.6
	successfulCnt := 0
	for {
		if db.IsClosed() {
			return
		}
		if err := db.RunValueLogGC(gcRate); err != nil {
			//if err == badger.ErrNoRewrite || err == badger.ErrRejected {
			//log.Println("Err GC: ", err)
			break
		} else {
			successfulCnt++
			continue
		}
	}
	afterSize, err := util.GetDirSize(dir)
	require.NoError(t, err)
	log.Printf("GC>[cost %s] successfulCnt[%d] db_size[%d MB] GC[%d MB]\n", time.Since(beforeTm).String(), successfulCnt, size, size-afterSize)
}

func TestGC(t *testing.T) {
	dir, err := ioutil.TempDir("./", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	// Do not change any of the options below unless it's necessary.
	opts := getTestOptions(dir)
	opts.NumLevelZeroTables = 50
	opts.NumLevelZeroTablesStall = 51
	opts.ValueLogMaxEntries = 2
	opts.ValueThreshold = 2
	// Setting LoadingMode to mmap seems to cause segmentation fault while closing DB.

	db1, err := badger.Open(opts)
	require.NoError(t, err)
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%10d", i))
	}
	val := []byte{1, 1, 1, 1, 1, 1, 1, 1}
	// Insert 100 entries. This will create about 50*3 vlog files and 6 SST files.
	for i := 0; i < 3; i++ {
		for j := 0; j < 100; j++ {
			err = db1.Update(func(txn *badger.Txn) error {
				return txn.SetEntry(badger.NewEntry(key(j), val))
			})
			require.NoError(t, err)
		}
	}
	// Run value log GC multiple times. This would ensure at least
	// one value log file is garbage collected.
	success := 0
	for i := 0; i < 10; i++ {
		err := db1.RunValueLogGC(0.01)
		if err == nil {
			success++
		}
		if err != nil && err != badger.ErrNoRewrite {
			t.Fatalf(err.Error())
		}
	}
	// Ensure alteast one GC call was successful.
	fmt.Println("success:", success)
	//require.NotZero(t, success)
}

func TestBackup(t *testing.T) {
	//srcDir, err := os.MkdirTemp("./", "data")
	//require.NoError(t, err)
	//defer removeDir(srcDir)
	db, err := badger.Open(getTestOptions("./data"))
	require.NoError(t, err)

	// Write some stuff
	entries := []struct {
		key      []byte
		val      []byte
		userMeta byte
		version  uint64
	}{
		{key: []byte("answer11"), val: []byte("41"), version: 1},
		{key: []byte("answer22"), val: []byte("42"), userMeta: 111, version: 2},
		{key: []byte("answer33"), val: []byte("43"), userMeta: 222, version: 3},
	}

	err = db.Update(func(txn *badger.Txn) error {
		e := entries[0]
		err := txn.SetEntry(badger.NewEntry(e.key, e.val).WithMeta(e.userMeta))
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(txn *badger.Txn) error {
		e := entries[1]
		err = txn.SetEntry(badger.NewEntry(e.key, e.val).WithMeta(e.userMeta))
		if err != nil {
			return err
		}
		return nil
	})
	err = db.Update(func(txn *badger.Txn) error {
		e := entries[2]
		err = txn.SetEntry(badger.NewEntry(e.key, e.val).WithMeta(e.userMeta))
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)
	badgerApi.PrintV(db)

	// Use different directory.
	//dirBak, err := os.MkdirTemp("./", "badgerbak-test")
	//require.NoError(t, err)
	//defer removeDir(srcDir)
	//bakFile, err := ioutil.TempFile(dirBak, "badgerbak")
	//require.NoError(t, err)
	_ = os.Mkdir("./badgerbak-test", 0777)
	//如果文件是以追加打开O_APPEND，则Backup的since参数应该也要和最后一次同步的version保持一致，保持增量。否则数据会有副本(但不会出错就是了)，就是浪费空间而已。
	//如果文件是新建O_TRUNC，则Backup的since参数=0
	//备份是按版本号来存的，如果原库版本号小于备份库里的 则数据会被忽略掉。只有大于备份库里的版本号才会被更新
	bakFile, err := os.OpenFile("./badgerbak-test/badgerbak", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0777)
	require.NoError(t, err)

	//bakFile可以换成socket
	//远程备份逻辑：
	// 1)两端建立tcp连接
	// 2)原服务定期通过socket发送增量数据给备份服
	// 3)备份服通过socket直接调用load()
	// 4)备份服务器负责后台的查询以及定期生成backup文件
	// 5)原服务器关服前确保同步备份到备份服
	lastVersion, err := db.Backup(bakFile, 0)

	require.NoError(t, err)
	require.NoError(t, bakFile.Close())
	require.NoError(t, db.Close())
	fmt.Printf("backup> bakFile.Name[%s] LastVersion[%d]\n", bakFile.Name(), lastVersion)

	db, err = badger.Open(getTestOptions("./badgerbak-test/data"))
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	bakNew, err := os.Open(bakFile.Name())
	require.NoError(t, err)

	defer func() {
		_ = bakNew.Close()
	}()
	require.NoError(t, db.Load(bakNew, 16))
	badgerApi.PrintV(db)
}

func TestDBCount(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("./data"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close()
	}()

	now := time.Now()
	fmt.Printf("DBCount: %d, cost: %s\n", badgerApi.GetDBCount(db), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Item_ cost: %s\n", badgerApi.GetPreDBCount(db, "Item_"), time.Since(now).String())

	now = time.Now()
	fmt.Printf("[%d] Role_ cost: %s\n", badgerApi.GetPreDBCount(db, "Role_"), time.Since(now).String())
}

func TestPrint(t *testing.T) {
	proxyDB, err := proxy.CreateDBProxy("./data", nil)
	require.NoError(t, err)
	defer func() {
		err := proxyDB.Close()
		require.NoError(t, err)
	}()

	badgerApi.Print(proxyDB.DB)
}
