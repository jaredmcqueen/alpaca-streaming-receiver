package alpaca

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/handlers"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

func WebsocketReceiver() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := stream.NewStocksClient(
		"sip",
	)

	if err := c.Connect(ctx); err != nil {
		log.Fatalf("could not connect to alpaca: %s", err)
	}
	fmt.Println("successfully connected to alpaca")

	if util.Config.EnableBars {
		if err := c.SubscribeToBars(handlers.BarHandler, strings.Fields(util.Config.SymbolsBars)...); err != nil {
			log.Fatalf("error during subscribing: %s", err)
		}
		fmt.Println("subscribed to bars", util.Config.SymbolsBars)
	}

	if util.Config.EnableQuotes {
		if err := c.SubscribeToQuotes(handlers.QuoteHandler, strings.Fields(util.Config.SymbolsQuotes)...); err != nil {
			log.Fatalf("error during subscribing: %s", err)
		}
		fmt.Println("subscribed to quotes", util.Config.SymbolsQuotes)
	}

	if util.Config.EnableStatuses {
		if err := c.SubscribeToStatuses(handlers.StatusHandler, strings.Fields(util.Config.SymbolsStatuses)...); err != nil {
			log.Fatalf("error during subscribing: %s", err)
		}
		fmt.Println("subscribed to statuses", util.Config.SymbolsStatuses)
	}

	if util.Config.EnableTrades {
		if err := c.SubscribeToTrades(handlers.TradeHandler, strings.Fields(util.Config.SymbolsTrades)...); err != nil {
			log.Fatalf("error during subscribing: %s", err)
		}
		fmt.Println("subscribed to trades", util.Config.SymbolsTrades)
	}

	// quit app if web socket connection is terminated
	err := <-c.Terminated()
	if err != nil {
		log.Fatalf("terminated with error: %s", err)
	}
	log.Println("exiting")
	os.Exit(0)

}
