package handlers

import (
	"fmt"
	"strings"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

var (
	tradeChan = make(chan stream.Trade, util.Config.ChannelQueueSize)
)

func TradeHandler(t stream.Trade) {
	tradeChan <- t
	websocketCounter.WithLabelValues("trades").Inc()
}

func ProcessTrades() {
	fmt.Println("starting trades processor")

	for t := range tradeChan {
		// TODO: ignore certain conditions
		for i, c := range t.Conditions {
			if c == " " {
				t.Conditions[i] = "@"
			}
		}

		redisWriter.RedisChan <- map[string]interface{}{
			"T": "trades",
			"S": t.Symbol,
			"x": fmt.Sprintf("%v", t.Exchange),
			"p": fmt.Sprintf("%v", t.Price),
			"s": fmt.Sprintf("%v", t.Size),
			"t": fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
			"c": fmt.Sprintf("%s", strings.Join(t.Conditions, "")),
			"z": t.Tape,
		}
		redisCounter.WithLabelValues("trades").Inc()
	}
}
