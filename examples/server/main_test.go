package main

import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"test_badger/badgerApi"
	"testing"
	"time"
)

func TestDBCount(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("./data"))
	if err != nil {
		t.Error(err)
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
