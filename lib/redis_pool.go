package lib

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"time"
)


func RedisConnPoolFactory(name string) (*redis.Pool, error) {
	if ConfRedisMap != nil && ConfRedisMap.List != nil {
		for confName, cfg := range ConfRedisMap.List {
			if name == confName {
				randHost := cfg.ProxyList[rand.Intn(len(cfg.ProxyList))]
				if cfg.ConnTimeout == 0 {
					cfg.ConnTimeout = 50
				}
				if cfg.ReadTimeout == 0 {
					cfg.ReadTimeout = 100
				}
				if cfg.WriteTimeout == 0 {
					cfg.WriteTimeout = 100
				}
				if cfg.RedisMaxIdle == 0{
					cfg.RedisMaxIdle = 3
				}
				if cfg.RedisIdleTimeoutSec == 0{
					cfg.RedisIdleTimeoutSec = 240
				}

				redisPool := &redis.Pool{
					MaxIdle:     cfg.RedisMaxIdle,
					IdleTimeout: time.Duration(cfg.RedisIdleTimeoutSec) * time.Second,
					Dial: func() (redis.Conn, error) {
						c, err := redis.Dial(
							"tcp",
							randHost,
							redis.DialConnectTimeout(time.Duration(cfg.ConnTimeout)*time.Millisecond),
							redis.DialReadTimeout(time.Duration(cfg.ReadTimeout)*time.Millisecond),
							redis.DialWriteTimeout(time.Duration(cfg.WriteTimeout)*time.Millisecond))
						if err != nil {
							return nil, err
						}
						if cfg.Password != "" {
							if _, err := c.Do("AUTH", cfg.Password); err != nil {
								c.Close()
								return nil, err
							}
						}
						if cfg.Db != 0 {
							if _, err := c.Do("SELECT", cfg.Db); err != nil {
								c.Close()
								return nil, err
							}
						}
						return c, nil
					},
					TestOnBorrow: func(c redis.Conn, t time.Time) error {
						_, err := c.Do("PING")
						if err != nil {
							return fmt.Errorf("ping redis error: %s", err)
						}
						return nil
					},
				}

				RedisMapPool[name] = redisPool
			}
		}
	}
	return nil, errors.New("create redis conn fail")
}

func RedisPoolLogDo(trace *TraceContext, c redis.Conn, commandName string, args ...interface{}) (interface{}, error) {
	startExecTime := time.Now()
	reply, err := c.Do(commandName, args...)
	endExecTime := time.Now()
	if err != nil {
		Log.TagError(trace, "_com_redis_failure", map[string]interface{}{
			"method":    commandName,
			"err":       err,
			"bind":      args,
			"proc_time": fmt.Sprintf("%fs", endExecTime.Sub(startExecTime).Seconds()),
		})
	} else {
		replyStr, _ := redis.String(reply, nil)
		Log.TagInfo(trace, "_com_redis_success", map[string]interface{}{
			"method":    commandName,
			"bind":      args,
			"reply":     replyStr,
			"proc_time": fmt.Sprintf("%fs", endExecTime.Sub(startExecTime).Seconds()),
		})
	}
	return reply, err
}

//通过配置 执行redis
func RedisPoolConfDo(trace *TraceContext, name string, commandName string, args ...interface{}) (interface{}, error) {
	c, err := RedisConnPoolFactory(name)
	if err != nil {
		Log.TagError(trace, "_com_redis_failure", map[string]interface{}{
			"method": commandName,
			"err":    errors.New("RedisConnFactory_error:" + name),
			"bind":   args,
		})
		return nil, err
	}
	defer c.Close()

	startExecTime := time.Now()
	reply, err := c.Get().Do(commandName, args...)
	endExecTime := time.Now()
	if err != nil {
		Log.TagError(trace, "_com_redis_failure", map[string]interface{}{
			"method":    commandName,
			"err":       err,
			"bind":      args,
			"proc_time": fmt.Sprintf("%fs", endExecTime.Sub(startExecTime).Seconds()),
		})
	} else {
		replyStr, _ := redis.String(reply, nil)
		Log.TagInfo(trace, "_com_redis_success", map[string]interface{}{
			"method":    commandName,
			"bind":      args,
			"reply":     replyStr,
			"proc_time": fmt.Sprintf("%fs", endExecTime.Sub(startExecTime).Seconds()),
		})
	}
	return reply, err
}

