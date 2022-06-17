package redisWriter

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

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

func RedisWriter(streamChan chan util.StreamItem) {
	ctx := context.Background()

	log.Println("connecting to redis endpoint", util.Config.RedisEndpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: util.Config.RedisEndpoint,
	})
	// test redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		// TODO: use a backoff pattern to try to recover?
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
		case t := <-streamChan:
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: t.Stream,
				ID:     "*",
				Values: t.Values,
			})
			// if t.Stream == "trades" {
			// 	pipe.Do(ctx,
			// 		"TS.ADD",
			// 		//key
			// 		fmt.Sprintf("trades:%v:price", t.Values["S"]),
			// 		//time
			// 		t.Values["t"],
			// 		//value
			// 		t.Values["p"],
			// 		"ON_DUPLICATE",
			// 		"FIRST",
			// 		"LABELS",
			// 		"type",
			// 		"stock",
			// 	)
			// }
			if pipe.Len() == util.Config.BatchMaxSize {
				err := RedisExec(pipe, ctx)
				if err != nil {
					fmt.Println("error sending to redis", err)
				}

				pipe = rdb.Pipeline()
				// https://pkg.go.dev/time#Timer.Reset
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(timeout)
			}
		}
	}
}
