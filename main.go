package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/go-redis/redis/v8"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

var websocketCount int32
var redisCount int32

func redisWriter(config util.Config, tradeChan chan stream.Trade) {
	ctx := context.Background()

	log.Println("connecting to redis endpoint", config.RedisEndpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: config.RedisEndpoint,
	})
	// test redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("error", err)
	}
	log.Printf("successfully connected to %v\n", config.RedisEndpoint)

	pipe := rdb.Pipeline()
	var pipePayload int32
	var conditions []byte

	start := time.Now()
	for t := range tradeChan {
		conditions, _ = json.Marshal(t.Conditions)

		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: "trades",
			ID:     "*",
			Values: map[string]string{
				"t": fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
				"S": t.Symbol,
				"p": fmt.Sprintf("%v", t.Price),
				"i": fmt.Sprintf("%v", t.ID),
				"s": fmt.Sprintf("%v", t.Size),
				"c": fmt.Sprintf("%s", conditions),
				"z": t.Tape,
			},
		})
		pipePayload++

		if pipePayload >= config.BatchSize {
			redisCount += pipePayload
			go pipe.Exec(ctx)
			// log.Printf("reached %v items in payload, sent %v trades", config.BatchSize, pipePayload)
			start = time.Now()
			pipePayload = 0
		}

		//TODO: there has to be a more performant design pattern here
		if time.Since(start).Milliseconds() >= config.BatchTime {
			redisCount += pipePayload
			go pipe.Exec(ctx)
			// log.Printf("reached %v milliseconds, sent %v trades", config.BatchTime, pipePayload)
			start = time.Now()
			pipePayload = 0
		}
	}
}

func writeStats(config util.Config) {
	ctx := context.Background()

	log.Println("connecting to redis endpoint", config.RedisEndpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: config.RedisEndpoint,
	})
	// test redis connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("error", err)
	}
	log.Printf("successfully connected to %v\n", config.RedisEndpoint)

	for {
		time.Sleep(1 * time.Second)
		log.Println("websockets", websocketCount, "redis", redisCount)
		rdb.Do(ctx,
			"TS.ADD",
			//key
			"stats:trades:alpaca",
			//time
			"*",
			//value
			fmt.Sprintf("%v", websocketCount),
		)
		rdb.Do(ctx,
			"TS.ADD",
			//key
			"stats:trades:streams",
			//time
			"*",
			//value
			fmt.Sprintf("%v", redisCount),
		)
		websocketCount = 0
		redisCount = 0
	}
}

func main() {
	// load config
	config, err := util.LoadConfig(".")
	if err != nil {
		log.Fatal("cannot load configuration", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// catch control+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		cancel()
	}()

	tradeChan := make(chan stream.Trade, 1_000_000)
	tradeHandler := func(t stream.Trade) {
		tradeChan <- t
		atomic.AddInt32(&websocketCount, 1)
	}

	symbols := strings.Fields(config.Symbols)
	log.Printf("creating Alpaca websockets client with %d symbols", len(symbols))
	wsc := stream.NewStocksClient(
		"sip",
		stream.WithTrades(tradeHandler, symbols...),
	)

	if err := wsc.Connect(ctx); err != nil {
		log.Fatalf("could not connect to alpaca: %s", err)
	}

	log.Println("successfully connected to alpaca")
	// starting a goroutine that checks whether the client has terminated
	go func() {
		err := <-wsc.Terminated()
		if err != nil {
			log.Fatalf("terminated with error: %s", err)
		}
		log.Println("exiting")
		os.Exit(0)
	}()

	go redisWriter(config, tradeChan)
	go writeStats(config)

	<-signalChan
	fmt.Print("received termination signal")
	os.Exit(0)
}
