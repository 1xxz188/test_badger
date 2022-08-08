package web

import (
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	json "github.com/json-iterator/go"
	"github.com/shopspring/decimal"
	"strconv"
	"test_badger/badgerApi"
	"test_badger/logger"
	"test_badger/proxy"
	"test_badger/util"
	"time"
)

var thisProxy *proxy.Proxy

func RunWeb(listenAddr string, proxy *proxy.Proxy) { //"0.0.0.0:4000"
	thisProxy = proxy
	app := fiber.New(fiber.Config{
		EnablePrintRoutes: true,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
	})

	//这个限制是针对同一个ip
	confLimiter := limiter.ConfigDefault
	confLimiter.Max = 10
	confLimiter.Expiration = 3 * time.Second
	confLimiter.LimitReached = func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusTooManyRequests)
	}
	app.Use(limiter.New(confLimiter))
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*", //"*"允许从任何网页请求引用过来。或者具体限制从哪些网页过来 "https://gofiber.io, https://gofiber.net, http://127.0.0.1:4000"
	}))

	app.Get("/get/:key", getKey())                   //指定查询, 通过缓存读取
	app.Get("/getDB/:key", getDBKey())               //指定查询, 直接拉取底层badger数据
	app.Get("/getDBRange/:begin-:end", getDBRange()) //范围查询,直接拉取底层badger数据
	app.Get("/metrics", getMetrics)                  //指标统计信息

	launchError := app.Listen(listenAddr)
	if launchError != nil {
		logger.Log.Fatal(launchError.Error())
	}
}

func getDBKey() func(c *fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		key := c.Params("key")
		resp := badgerApi.KV{
			K: key,
		}
		fnResp := func(errMsg string) []byte {
			resp.Err = errMsg
			result, err := json.Marshal(resp)
			if err != nil {
				return util.StringToByteSlice("json.Marshal error")
			}
			return result
		}
		if key == "" {
			return c.Send(fnResp("key parameter illegal"))
		}
		v, err := thisProxy.GetDBValue(key)
		if err != nil {
			return c.Send(fnResp(err.Error()))
		}
		resp = *v
		return c.Send(fnResp(resp.Err))
	}
}
func getKey() func(c *fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		key := c.Params("key")
		resp := badgerApi.KV{
			K: key,
		}
		fnResp := func(errMsg string) []byte {
			resp.Err = errMsg
			result, err := json.Marshal(resp)
			if err != nil {
				return util.StringToByteSlice("json.Marshal error")
			}
			return result
		}

		if key == "" {
			return c.Send(fnResp("key parameter illegal"))
		}

		if thisProxy == nil {
			return c.Send(fnResp("db not init"))
		}

		kv, _ := thisProxy.Gets("", []string{key})
		if len(kv) != 1 {
			return c.Send(fnResp("server internal logic error"))
		}
		resp = *kv[0]
		return c.Send(fnResp(resp.Err))
	}
}

func getDBRange() func(c *fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		type KVList struct {
			Result []badgerApi.KV `json:"kvList"`
			Err    string         `json:"error"`
		}
		resp := KVList{}
		fnResp := func(errMsg string) []byte {
			resp.Err = errMsg
			result, err := json.Marshal(resp)
			if err != nil {
				return util.StringToByteSlice("json.Marshal error")
			}
			return result
		}
		if thisProxy == nil {
			return c.Send(fnResp("db not init"))
		}
		begin, err := strconv.Atoi(c.Params("begin"))
		if err != nil {
			return c.Send(fnResp(err.Error()))
		}
		end, err := strconv.Atoi(c.Params("end"))
		if err != nil {
			return c.Send(fnResp(err.Error()))
		}
		if begin < 0 || end < 0 {
			return c.Send(fnResp("parameter err: begin < 0 || end < 0"))
		}
		if begin > end {
			return c.Send(fnResp(fmt.Sprintf("parameter begin[%d] > end[%d]", begin, end)))
		}
		diff := end - begin
		if diff > 1000 {
			return c.Send(fnResp(fmt.Sprintf("parameter begin[%d]-end[%d]=[%d] should be limited <= 1000", begin, end, diff)))
		}

		resp.Result = badgerApi.GetRange(thisProxy.DB, begin, end)
		return c.Send(fnResp(""))
	}
}

func getMetrics(c *fiber.Ctx) error {
	type Metrics struct {
		Warning               string  `json:"warning"`
		BlockCache            string  `json:"blockCache"`
		IndexCache            string  `json:"indexCache"`
		Err                   string  `json:"error"`
		CacheCount            uint64  `json:"cacheCount"`
		CachePenetrateCount   uint64  `json:"cachePenetrateCount"`
		CachePenetrateCntRate float64 `json:"cachePenetrateCntRate"`
	}

	resp := Metrics{}
	fnResp := func(errMsg string) []byte {
		resp.Err = errMsg
		result, err := json.Marshal(resp)
		if err != nil {
			return util.StringToByteSlice("json.Marshal error")
		}
		return result
	}

	if thisProxy == nil {
		return c.Send(fnResp("db not init"))
	}

	analyze := func(name *string, metrics *ristretto.Metrics) {
		// If the mean life expectancy is less than 10 seconds, the cache
		// might be too small.
		le := metrics.LifeExpectancySeconds()
		if le == nil {
			return
		}

		lifeTooShort := le.Count > 0 && float64(le.Sum)/float64(le.Count) < 10
		hitRatioTooLow := metrics.Ratio() > 0 && metrics.Ratio() < 0.4
		if lifeTooShort && hitRatioTooLow {
			resp.Warning = fmt.Sprintf("%s might be too small. Metrics: %s, Cache life expectancy (in seconds): %+v", *name, metrics, le)
		}

		*name = metrics.String()
	}

	analyze(&resp.BlockCache, thisProxy.DB.BlockCacheMetrics())
	analyze(&resp.IndexCache, thisProxy.DB.IndexCacheMetrics())

	resp.CacheCount = thisProxy.GetCacheCnt()
	resp.CachePenetrateCount = thisProxy.GetCachePenetrateCnt()

	b, _ := decimal.NewFromFloat(thisProxy.GetCachePenetrateRate()).Round(6).Float64()
	resp.CachePenetrateCntRate = b //strconv.FormatFloat(b, 'f', 6, 64)
	return c.Send(fnResp(""))
}
