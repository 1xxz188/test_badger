package web

import (
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/shopspring/decimal"
	"strconv"
	"test_badger/badgerApi"
	"test_badger/logger"
	"test_badger/proxy"
	"time"
)

var thisProxy *proxy.DBProxy

func RunWeb(listenAddr string, proxy *proxy.DBProxy) { //"0.0.0.0:4000"
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

func GetErr(err string) string {
	return fmt.Sprintf("{\"error\":\"%s\"}", err)
}

func getDBKey() func(c *fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		key := c.Params("key")
		if key == "" {
			return c.SendString(GetErr("key parameter illegal"))
		}
		v, err := badgerApi.GetValue(thisProxy.DB, key)
		if err != nil {
			return c.SendString(GetErr(err.Error()))
		}

		result, err := json.Marshal(v)
		if err != nil {
			return c.SendString(GetErr(err.Error()))
		}
		return c.Send(result)
	}
}
func getKey() func(c *fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		if thisProxy == nil {
			return c.SendString(GetErr("db not init"))
		}

		key := c.Params("key")
		if key == "" {
			return c.SendString(GetErr("key parameter illegal"))
		}

		kv, _ := thisProxy.Gets("", []string{key})
		if len(kv) != 1 {
			return c.SendString(GetErr("server internal logic error"))
		}

		result, err := json.Marshal(kv[0])
		if err != nil {
			return c.SendString(GetErr(err.Error()))
		}
		return c.Send(result)
	}
}

func getDBRange() func(c *fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		if thisProxy == nil {
			return c.SendString(GetErr("db not init"))
		}
		begin, err := strconv.Atoi(c.Params("begin"))
		if err != nil {
			return c.SendString(GetErr(err.Error()))
		}
		end, err := strconv.Atoi(c.Params("end"))
		if err != nil {
			return c.SendString(GetErr(err.Error()))
		}
		if begin < 0 || end < 0 {
			return c.SendString(GetErr("parameter err: begin < 0 || end < 0"))
		}
		if begin > end {
			return c.SendString(GetErr(fmt.Sprintf("parameter begin[%d] > end[%d]", begin, end)))
		}
		diff := end - begin
		if diff > 1000 {
			return c.SendString(GetErr(fmt.Sprintf("parameter begin[%d]-end[%d]=[%d] should be limited <= 1000", begin, end, diff)))
		}

		kvList := badgerApi.GetRange(thisProxy.DB, begin, end)
		result, err := json.Marshal(kvList)
		if err != nil {
			return c.SendString(GetErr(err.Error()))
		}
		return c.Send(result)
	}
}

func getMetrics(c *fiber.Ctx) error {
	if thisProxy == nil {
		return c.SendString(GetErr("db not init"))
	}

	result := make(map[string]interface{}, 8)
	analyze := func(name string, metrics *ristretto.Metrics) {
		// If the mean life expectancy is less than 10 seconds, the cache
		// might be too small.
		le := metrics.LifeExpectancySeconds()
		if le == nil {
			return
		}

		lifeTooShort := le.Count > 0 && float64(le.Sum)/float64(le.Count) < 10
		hitRatioTooLow := metrics.Ratio() > 0 && metrics.Ratio() < 0.4
		if lifeTooShort && hitRatioTooLow {
			result["warning"] = fmt.Sprintf("%s might be too small. Metrics: %s, Cache life expectancy (in seconds): %+v", name, metrics, le)
		}

		result[name] = metrics.String()
	}

	analyze("blockCache", thisProxy.DB.BlockCacheMetrics())
	analyze("indexCache", thisProxy.DB.IndexCacheMetrics())

	result["cacheCount"] = thisProxy.GetCacheCnt()
	result["cachePenetrateCount"] = thisProxy.GetCachePenetrateCnt()

	b, _ := decimal.NewFromFloat(thisProxy.GetCachePenetrateRate()).Round(6).Float64()
	result["cachePenetrateCntRate"] = b //strconv.FormatFloat(b, 'f', 6, 64)

	v, err := json.Marshal(result)
	if err != nil {
		return c.SendString(GetErr(err.Error()))
	}
	return c.Send(v)
}
