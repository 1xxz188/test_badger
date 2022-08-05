package badgerApi

/* -----ristetto 配置说明-----
type Config struct {
	// 可以简单理解成key的数量，他是用来保存key被点击次数的，但实际数量并不是这个设置的值，而是最靠近并且大于或等于该值的2的n次方值减1
	// 比如
	// 设置成 1，2^0=1,2^1=2,这时候2^0次方等于1，所以最终的值就是2^0-1=0
	// 设置成 2，2^0=1,2^1=2,这时候2^1次方等于2，所以最终的值就是2^1-1=1
	// 设置成 3，2^0=1,2^1=2,2^2=4,这时候2^2次方大于等于3，所以最终的值就是2^2-1=3
	// 设置成 6，2^0=1,2^1=2,2^2=4,2^3=8,这时候2^3次方大于等于6，所以最终的值就是2^3-1=7
	// 设置成 20，2^0=1,2^1=2,2^2=4,2^3=8,...,2^4=16,2^5=32,这时候2^5次方大于等于20，所以最终的值就是2^5-1=31
	// 官方建议设置成你想要设置key数量的10倍，因为这样会具有更高的准确性
	// 根据这个值，可以知道计数器要用的内存 NumCounters / 1024 / 1024 * 4 MB
	NumCounters int64
	// 单位是可以随意的，例如你想限制内存最大为100MB,你可以把MaxCost设置为100,000,000，那么每个key的成本cost就是bytes
	MaxCost int64
	// BufferItems决定获取缓冲区的大小。除非您有一个罕见的用例，否则使用'64'作为BufferItems值可以 获得良好的性能。
	// BufferItems 决定了Get缓冲区的大小,在有损环形缓冲区ringBuffer中，当数据达到这个值，就会去对这批key进行点击数量统计
	BufferItems int64
	// 设置为true，就会统计操作类型的次数，设置为true会消耗成本，建议在测试的时候才开启
	Metrics bool
	// 当cache被清除的时候调用，过期清除 还有 策略清除
	OnEvict func(item *Item)
	// 设置一个key失败的时候调用，失败的条件一般有，已经存在key，再次add,或者cost不足
	OnReject func(item *Item)
	// 删除一个值的时候调用，可以用做手动回收内存。
	OnExit func(val interface{})
	// 计算key的hash函数
	KeyToHash func(key interface{}) (uint64, uint64)
	// 计算成本的函数，没有设置成本的时候用这个来计算成本
	Cost func(value interface{}) int64
	//set(k,v,)
	// 设置为true的时候 不计内部结构的成本,使用用户自定义成本, Set的时候设置每个key的成本值，默认是计算的，用于存储key-value 结构。
	// type storeItem struct {
	//    key        uint64
	//    conflict   uint64
	//    value      interface{}
	//    expiration int64
	// }
	IgnoreInternalCost bool
}*/
import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
)

type KV struct {
	K         string `json:"key"`
	V         []byte `json:"value"`
	Version   uint64 `json:"version"`
	ExpiresAt uint64 `json:"expiresAt"`
	UserMeta  byte   `json:"userMeta"`
}

func DefaultOptions(path string) badger.Options {
	return badger.DefaultOptions(path).
		WithCompactL0OnClose(true).  //退出时处理LO压缩
		WithDetectConflicts(false).  //禁用版本冲突(由业务层保障)
		WithBlockCacheSize(2 << 30). //如果加密和压缩开启时，需要开启，否则关闭
		WithValueThreshold(65)       //不需要太大,kv分离可以减少树大小,稳定读写性能，减少写放大，减少L0阻塞风险
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
func GetValue(db *badger.DB, key string) (*KV, error) {
	txn := db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return &KV{
		K:         key,
		V:         v,
		Version:   item.Version(),
		UserMeta:  item.UserMeta(),
		ExpiresAt: item.ExpiresAt(),
	}, nil
}

// GetRange 指定范围迭代
func GetRange(db *badger.DB, begin int, end int) (result []KV) {
	txn := db.NewTransaction(false)
	defer txn.Discard()
	itr := txn.NewIterator(badger.DefaultIteratorOptions)
	defer itr.Close()
	count := 0
	for itr.Rewind(); itr.Valid(); itr.Next() {
		count++
		if count <= begin {
			continue
		}

		k := itr.Item().Key()
		v, err := itr.Item().ValueCopy(nil)
		if err != nil {
			panic(err)
		}
		result = append(result, KV{
			K:         string(k),
			V:         v,
			Version:   itr.Item().Version(),
			UserMeta:  itr.Item().UserMeta(),
			ExpiresAt: itr.Item().ExpiresAt(),
		})
		if count > end {
			break
		}
	}
	return result
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
		fmt.Printf("%s: v_len[%d]\n", string(k), len(v))
	}
	fmt.Printf("all keys: %d\n", count)
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
		fmt.Printf("[%d] [version: %d]> %s: %s UserMeta[%d]\n", count, itr.Item().Version(), string(k), string(v), itr.Item().UserMeta())
	}
}
