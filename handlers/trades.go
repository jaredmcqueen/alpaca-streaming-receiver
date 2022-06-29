package handlers

import (
	"fmt"
	"strings"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	tradeChan          = make(chan stream.Trade, util.Config.ChannelQueueSize)
	alpacaTradeCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alpaca_receiver_websockets_trades_total",
		Help: "trades received from alpaca",
	})
	redisTradeCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alpaca_receiver_redis_writer_trades_total",
		Help: "trades written to redis streams",
	})
)

func TradeHandler(t stream.Trade) {
	tradeChan <- t
	alpacaTradeCounter.Inc()
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
		redisTradeCounter.Inc()
	}
}
