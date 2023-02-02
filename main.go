package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

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
	natsEndpoint := flag.String("natsEndpoint", "localhost:4222", "nats endpoint")
	natsStreamAgeLimit := flag.Int("natsStreamAgeLimit", 24, "age limit for published messages in hours")
	alpacaFeed := flag.String("feed", "sip", "alpaca feed")
	enableBars := flag.Bool("bars", true, "enable bars")
	enableQuotes := flag.Bool("quotes", true, "enable quotes")
	enableTrades := flag.Bool("trades", true, "enable trades")
	enableStatuses := flag.Bool("statuses", true, "enable trading statuses")
	symbols := flag.String("symbols", "*", "space separated ticker symbols")
	flag.Parse()

	ageLimit := time.Hour * time.Duration(*natsStreamAgeLimit)
	symbolsSlice := strings.Split(*symbols, " ")
	log.Println("subscribed symbols are", symbolsSlice)

	log.Println("variables set:")
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("%s: %s (%s)", f.Name, f.Value, f.DefValue)
	})

	// connect to nats
	natsClient := client.NewNatsClient(*natsEndpoint)
	defer natsClient.Conn.Close()

	// connect to alpaca
	alpacaClient := client.NewAlpacaClient(*alpacaFeed)

	natsClient.AddStream("ALPACA", []string{"ALPACA.bars", "ALPACA.quotes", "ALPACA.trades", "ALPACA.statuses"}, ageLimit)
	// enable bars
	if *enableBars {
		subject := "ALPACA.bars"
		ch := make(chan []byte)
		natsClient.AddJSPublisher(ch, subject)
		alpacaClient.AddBarHandler(ch, symbolsSlice)
	}

	// enable quotes
	if *enableQuotes {
		subject := "ALPACA.quotes"
		ch := make(chan []byte)
		natsClient.AddJSPublisher(ch, subject)
		alpacaClient.AddQuoteHandler(ch, symbolsSlice)
	}

	// enable trades
	if *enableTrades {
		subject := "ALPACA.trades"
		ch := make(chan []byte)
		natsClient.AddJSPublisher(ch, subject)
		alpacaClient.AddTradeHandler(ch, symbolsSlice)
	}

	// enable statuses
	if *enableStatuses {
		subject := "ALPACA.statuses"
		ch := make(chan []byte)
		natsClient.AddJSPublisher(ch, subject)
		alpacaClient.AddStatusHandler(ch, symbolsSlice)
	}

	<-signalChan
	fmt.Print("received termination signal")
	os.Exit(0)
}
