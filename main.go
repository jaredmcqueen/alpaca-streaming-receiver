package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/handlers"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// catch control+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		cancel()
	}()

	// symbols := strings.Fields(config.Symbols)
	symbols := strings.Fields(util.Config.Symbols)
	log.Printf("creating Alpaca websockets client with %d symbols", len(symbols))
	wsc := stream.NewStocksClient(
		"sip",
		stream.WithTrades(handlers.TradeHandler, symbols...),
		stream.WithBars(handlers.MinuteBarHandler, symbols...),
		// stream.WithDailyBars(handlers.DailyBarHandler, symbols...),
		stream.WithStatuses(handlers.StatusHandler, symbols...),
		// stream.WithQuotes(handlers.QuoteHandler, []string{"AAPL"}...),
		// stream.WithQuotes(handlers.QuoteHandler, symbols...),
		// TODO: there is something wrong in symbols
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

	for i := 0; i < util.Config.RedisWorkers; i++ {
		go redisWriter.RedisWriter(util.StreamChan)
	}

	<-signalChan
	fmt.Print("received termination signal")
	os.Exit(0)
}
