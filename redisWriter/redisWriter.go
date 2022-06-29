package redisWriter

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RedisChan          = make(chan map[string]interface{}, util.Config.ChannelQueueSize)
	redisWorkerCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alpaca_receiver_redis_writer_worker_total",
		Help: "number of events sent to redis by a single worker",
	}, []string{"worker"})
)

func RedisWriter(id int) {
	fmt.Println("starting redis writer", id)

	ctx := context.Background()

	fmt.Println("connecting to redis endpoint", util.Config.RedisEndpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: util.Config.RedisEndpoint,
	})

	err := rdb.Ping(ctx).Err()
	if err != nil {
		log.Fatal("could not ping redis server", err)
	}

	pipe := rdb.Pipeline()
	timeout := time.Duration(util.Config.RedisBatchTimeout) * time.Millisecond
	timer := time.NewTimer(timeout)
	flush := func() error {
		// nothing to send
		if pipe.Len() == 0 {
			return nil
		}

		// pipe.Exec clears out the len, so emit to prometheus here
		redisWorkerCounter.WithLabelValues(fmt.Sprintf("%v", id)).Add(float64(pipe.Len()))

		_, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}

		return nil
	}

	for {
		select {
		case <-timer.C:
			err := flush()
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
			if pipe.Len() >= util.Config.RedisBatchMaxSize {
				err := flush()
				if err != nil {
					fmt.Println("error sending to redis", err)
				}

				if !timer.Stop() {
					<-timer.C
				}

				timer.Reset(timeout)
			}
		}
	}
}
