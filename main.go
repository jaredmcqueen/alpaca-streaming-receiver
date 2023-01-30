package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/jaredmcqueen/alpaca-streaming-receiver/client"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	// prom metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(":9100", nil)
	}()
}

func main() {
	log.Println("starting alpaca-streaming-receiver")

	// catch control+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
	}()

	flag.PrintDefaults()
	natsEndpoint := flag.String("natsEndpoint", "nats.nats:4222", "nats endpoint")
	alpacaFeed := flag.String("feed", "sip", "alpaca feed")
	enableBars := flag.Bool("bars", true, "enable bars")
	enableQuotes := flag.Bool("quotes", true, "enable quotes")
	enableTrades := flag.Bool("trades", true, "enable trades")
	enableStatuses := flag.Bool("statuses", true, "enable trading statuses")
	channelBuffer := flag.Int("channelBuffer", 1_000, "channel buffer")
	symbols := flag.String("symbols", "*", "space separated ticker symbols")
	flag.Parse()

	symbolsSlice := strings.Split(*symbols, " ")
	log.Println("subscribed symbols are", symbolsSlice)

	log.Println("variables set:")
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("%s: %s (%s)", f.Name, f.Value, f.DefValue)
	})

	// connect to nats
	natsClient, err := client.NewNatsClient(*natsEndpoint)
	if err != nil {
		log.Fatal("connecting to nats", err)
	}

	// connect to alpaca
	alpacaClient, err := client.NewAlpacaClient(*alpacaFeed)
	if err != nil {
		log.Fatal("connecting to alpaca", err)
	}

	// enable bars
	if *enableBars {
		barChan := make(chan any, *channelBuffer)
		natsClient.AddStream("bars", []string{"bars"})
		natsClient.AttachWriter(barChan, "bars")
		alpacaClient.AddBarHandler(barChan, symbolsSlice)
	}

	// enable quotes
	if *enableQuotes {
		quoteChan := make(chan any, *channelBuffer)
		natsClient.AddStream("quotes", []string{"quotes"})
		natsClient.AttachWriter(quoteChan, "quotes")
		alpacaClient.AddQuoteHandler(quoteChan, symbolsSlice)
	}

	// enable trades
	if *enableTrades {
		tradeChan := make(chan any, *channelBuffer)
		natsClient.AddStream("trades", []string{"trades"})
		natsClient.AttachWriter(tradeChan, "trades")
		alpacaClient.AddTradeHandler(tradeChan, symbolsSlice)
	}

	// enable statuses
	if *enableStatuses {
		statusChan := make(chan any, *channelBuffer)
		natsClient.AddStream("statuses", []string{"statuses"})
		natsClient.AttachWriter(statusChan, "statuses")
		alpacaClient.AddStatusHandler(statusChan, symbolsSlice)
	}

	<-signalChan
	fmt.Print("received termination signal")
	os.Exit(0)
}
