package handlers

import (
	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

var (
	tradeChan = make(chan stream.Trade, util.Config.ChannelQueueSize)
)

func TradeHandler(t stream.Trade) {
	tradeChan <- t
	websocketCounter.WithLabelValues("trades").Inc()
}
