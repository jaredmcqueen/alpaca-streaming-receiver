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

	wsc := stream.NewStocksClient(
		"sip",
		stream.WithBars(handlers.BarHandler, strings.Fields(util.Config.BarSymbols)...),
		stream.WithStatuses(handlers.StatusHandler, strings.Fields(util.Config.StatusSymbols)...),
		stream.WithQuotes(handlers.QuoteHandler, strings.Fields(util.Config.QuoteSymbols)...),
		stream.WithTrades(handlers.TradeHandler, strings.Fields(util.Config.TradeSymbols)...),
	)
	if err := wsc.Connect(ctx); err != nil {
		log.Fatalf("could not connect to alpaca: %s", err)
	}
	log.Println("successfully connected to alpaca")

	// control+C
	go func() {
		err := <-wsc.Terminated()
		if err != nil {
			log.Fatalf("terminated with error: %s", err)
		}
		log.Println("exiting")
		os.Exit(0)
	}()

	// start the processors
	go handlers.ProcessBars()
	go handlers.ProcessQuotes()
	go handlers.ProcessStatuses()
	go handlers.ProcessTrades()

	// start the redis writer
	for i := 0; i < util.Config.RedisWorkers; i++ {
		go redisWriter.RedisWriter()
	}

	<-signalChan
	fmt.Print("received termination signal")
	os.Exit(0)
}
