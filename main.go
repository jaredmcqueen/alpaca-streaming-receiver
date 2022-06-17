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
	tradeSymbols := strings.Fields(util.Config.TradeSymbols)
	barSymbols := strings.Fields(util.Config.BarSymbols)
	statusSymbols := strings.Fields(util.Config.StatusSymbols)
	quoteSymbols := strings.Fields(util.Config.QuoteSymbols)
	wsc := stream.NewStocksClient(
		"sip",
		stream.WithTrades(handlers.TradeHandler, tradeSymbols...),
		stream.WithBars(handlers.MinuteBarHandler, barSymbols...),
		stream.WithStatuses(handlers.StatusHandler, statusSymbols...),
		stream.WithQuotes(handlers.QuoteHandler, quoteSymbols...),
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
