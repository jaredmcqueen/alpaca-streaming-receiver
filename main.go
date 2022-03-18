package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/go-redis/redis/v8"
	"github.com/jaredmcqueen/tick-receiver/util"
)

func redisWriter(config util.Config, tradeChan chan stream.Trade) {
	ctx := context.Background()

	log.Println("connecting to redis endpoint", config.RedisEndpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: config.RedisEndpoint,
	})
	// test redis connection
	r, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("error", err)
	}
	log.Printf("%v successfully connected to %v\n", r, config.RedisEndpoint)

	// clear out the db
	if config.FlushDB {
		log.Println("flushing redis DB")
		rdb.FlushAll(ctx)
	}

	pipe := rdb.Pipeline()
	var pipePayload int32

	time.Sleep(10 * time.Second)
	start := time.Now()
	for t := range tradeChan {
		_ = t
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: fmt.Sprintf("trades.%v", t.Symbol),
			ID:     "*",
			Values: map[string]string{
				"i": fmt.Sprintf("%v", t.ID),
				"S": t.Symbol,
				"x": t.Exchange,
				"p": fmt.Sprintf("%v", t.Price),
				"s": fmt.Sprintf("%v", t.Size),
				"t": fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
				"c": fmt.Sprintf("%v", t.Conditions),
				"z": t.Tape,
			},
		})
		pipePayload++

		if pipePayload >= 10_000 {
			go pipe.Exec(ctx)
			log.Printf("reached %v items in payload, sent %v trades", config.BatchSize, pipePayload)
			start = time.Now()
			pipePayload = 0
		}

		//TODO: there has to be a more performant design pattern here
		if time.Since(start).Milliseconds() >= config.BatchTime {
			go pipe.Exec(ctx)
			log.Printf("reached %v milliseconds, sent %v trades", config.BatchTime, pipePayload)
			start = time.Now()
			pipePayload = 0
		}
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
	}

	// alpaca websocket client
	wsc := stream.NewStocksClient(
		"sip",
		stream.WithTrades(tradeHandler, "*"),
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

	go func() {
		for {
			time.Sleep(1 * time.Second)
			log.Println("buffer length", len(tradeChan))
		}
	}()

	go redisWriter(config, tradeChan)

	<-signalChan
	fmt.Print("received termination signal")
	os.Exit(0)
}
