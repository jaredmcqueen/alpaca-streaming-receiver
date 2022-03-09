package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/go-redis/redis/v8"
	"github.com/jaredmcqueen/tick-receiver/util"
)

func main() {
	// load config
	config, err := util.LoadConfig(".")
	if err != nil {
		log.Fatal("cannot load configuration", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tradeHandler := func(t stream.Trade) {
		log.Printf("%+v", t)
	}

	// catch control+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		cancel()
	}()

	log.Println("connecting to redis endpoint", config.RedisEndpoint)
	rdb := redis.NewClient(&redis.Options{
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
	if config.FlushDB {
		log.Println("flushing redis DB")
		rdb.FlushAll(ctx)
	}

	c := stream.NewStocksClient(
		"sip",
		stream.WithTrades(tradeHandler, "TSLA"),
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
