package redisWriter

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

var RedisChan chan map[string]interface{}

func init() {
	RedisChan = make(chan map[string]interface{}, 100_000)
}

func RedisExec(pipe redis.Pipeliner, ctx context.Context) error {

	// nothing to send
	if pipe.Len() == 0 {
		return nil
	}

	_, err := pipe.Exec(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func RedisWriter() {
	ctx := context.Background()

	log.Println("connecting to redis endpoint", util.Config.RedisEndpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: util.Config.RedisEndpoint,
	})

	// test redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("error", err)
	}
	log.Printf("successfully connected to %v\n", util.Config.RedisEndpoint)

	pipe := rdb.Pipeline()
	timeout := time.Duration(util.Config.BatchTimeout) * time.Millisecond
	timer := time.NewTimer(timeout)

	for {
		select {
		case <-timer.C:
			err := RedisExec(pipe, ctx)
			if err != nil {
				fmt.Println("error sending to redis", err)
			}
			timer.Reset(timeout)
		case item := <-RedisChan:
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: item["T"].(string),
				ID:     "*",
				Values: item,
			})
			if pipe.Len() == util.Config.BatchMaxSize {
				err := RedisExec(pipe, ctx)
				if err != nil {
					fmt.Println("error sending to redis", err)
				}

				pipe = rdb.Pipeline()
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(timeout)
			}
		}
	}
}
