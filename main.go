package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/go-redis/redis/v8"
	"github.com/jaredmcqueen/tick-receiver/util"
)

var rdb *redis.Client
var pipe redis.Pipeliner
var ctx context.Context

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

	log.Println("connecting to redis endpoint", config.RedisEndpoint)
	rdb = redis.NewClient(&redis.Options{
		Addr: config.RedisEndpoint,
	})

	// test redis connection
	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("error", err)
	}
	log.Println("successfully connected to", config.RedisEndpoint)

	// clear out the db
	// TODO: make this an envar
	// if config.FlushDB {
	// 	log.Println("flushing redis DB")
	// 	rdb.FlushAll(ctx)
	// }

	// ID:2820
	// Symbol:TSLA
	// Exchange:X
	// Price:850.76
	// Size:1
	// Timestamp:2022-03-09 13:52:04.967257986 -0500 EST
	// Conditions:[@ I]
	// Tape:C

	var tradeCount int32

	tradeHandler := func(t stream.Trade) {
		atomic.AddInt32(&tradeCount, 1)
		rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: fmt.Sprintf("trades.%v", t.Symbol),
			ID:     "*",
			Values: map[string]string{
				"ID":         fmt.Sprintf("%v", t.ID),
				"Symbol":     t.Symbol,
				"Exchange":   t.Exchange,
				"Price":      fmt.Sprintf("%v", t.Price),
				"Size":       fmt.Sprintf("%v", t.Size),
				"Timestamp":  fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
				"Conditions": fmt.Sprintf("%v", t.Conditions),
				"Tape":       t.Tape,
			},
		})
	}

	// barHandler := func(b stream.Bar) {
	// 	log.Printf("%+v", b)
	// }

	c := stream.NewStocksClient(
		"sip",
		stream.WithTrades(tradeHandler, "*"),
	)

	if err := c.Connect(ctx); err != nil {
		log.Fatalf("could not connect to alpaca: %s", err)
	}
	log.Println("successfully connected to alpaca")
	// starting a goroutine that checks whether the client has terminated
	go func() {
		err := <-c.Terminated()
		if err != nil {
			log.Fatalf("terminated with error: %s", err)
		}
		log.Println("exiting")
		os.Exit(0)
	}()

	<-signalChan
	fmt.Print("received termination signal")
	os.Exit(0)

}
