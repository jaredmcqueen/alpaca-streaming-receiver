package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"

	"github.com/jaredmcqueen/alpaca-streaming-receiver/alpaca"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/handlers"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// catch control+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
	}()

	// start the websocket receiver
	go alpaca.WebsocketReceiver()

	// start the processors
	go handlers.ProcessHLV()
	go handlers.ProcessBars()
	go handlers.ProcessStatuses()

	// start the redis writer
	for i := 1; i < util.Config.RedisWorkers+1; i++ {
		go redisWriter.RedisWriter(i)
	}

	// metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9100", nil)
	}()

	<-signalChan
	fmt.Print("received termination signal")
	os.Exit(0)
}
