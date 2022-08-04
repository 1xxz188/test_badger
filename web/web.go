package web

import (
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"strconv"
	"test_badger/badgerApi"
	"test_badger/logger"
	"test_badger/proxy"
	"test_badger/util"
	"time"
)

var thisProxy *proxy.DBProxy

func RunWeb(listenAddr string, proxy *proxy.DBProxy) { //"0.0.0.0:4000"
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

	app.Get("/get/:key", getKey())                    //通过缓存读取
	app.Get("/getDB/:key", getDBKey())                //直接拉取底层badger数据
	app.Get("/getDB-range/:begin-:end", getDBRange()) //直接拉取底层badger数据

	app.Get("/metrics", getMetrics)

	launchError := app.Listen(listenAddr)
	if launchError != nil {
		logger.Log.Fatal(launchError.Error())
	}
	thisProxy = proxy
}

func GetErr(errMsg string) string {
	return "Err: " + errMsg
}

func getDBKey() func(c *fiber.Ctx) error {
	return func(c *fiber.Ctx) error {
		key := c.Params("key")
		if key == "" {
			return c.SendString(GetErr("key parameter illegal"))
		}
		v, err := badgerApi.GetValue(thisProxy.Db, key)
		if err != nil {
			return c.SendString(GetErr(err.Error()))
		}
		return c.SendString(fmt.Sprintf("Key: %s\nValue:%s", c.Params("key"), v))
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
		if kv[0].Err != nil {
			return c.SendString(GetErr(kv[0].Err.Error()))
		}
		return c.SendString(fmt.Sprintf("Key: %s\nValue:%s", key, kv[0].V))
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

		now := time.Now()
		var result string
		kvList := badgerApi.GetRange(thisProxy.Db, begin, end)
		for i, kv := range kvList {
			result += fmt.Sprintf("[%d] [version: %d]> %s: %s UserMeta[%d] expires_at[%d]\n", i+1, kv.Version, kv.K, util.ByteSliceToString(kv.V), kv.UserMeta, kv.ExpiresAt)
		}
		result += "Cost: " + time.Now().Sub(now).String()
		return c.SendString(result)
	}
}

func getMetrics(c *fiber.Ctx) error {
	if thisProxy == nil {
		return c.SendString(GetErr("db not init"))
	}

	var result string
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
			result += fmt.Sprintf("%s might be too small. Metrics: %s\n", name, metrics)
			result += fmt.Sprintf("Cache life expectancy (in seconds): %+v\n", le)
		}

		result += fmt.Sprintf("%s metrics: %s\n", name, metrics)
	}

	analyze("Block cache", thisProxy.Db.BlockCacheMetrics())
	analyze("Index cache", thisProxy.Db.IndexCacheMetrics())

	result += "Cache Cnt: " + strconv.FormatUint(thisProxy.GetCacheCnt(), 10)
	result += "\nCache Penetrate Cnt: " + strconv.FormatUint(thisProxy.GetCachePenetrateCnt(), 10)
	result += "\nCache PenetrateCnt Rate: " + strconv.FormatFloat(thisProxy.GetCachePenetrateRate(), 'g', 3, 32)

	return c.SendString(result)
}
