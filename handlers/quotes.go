package handlers

import (
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

var (
	quoteChan = make(chan stream.Quote, util.Config.ChannelQueueSize)
)

func QuoteHandler(q stream.Quote) {
	quoteChan <- q
	websocketCounter.WithLabelValues("quotes").Inc()
}

func nextTick(duration time.Duration) time.Duration {
	now := time.Now()
	roundedNow := now.Round(duration)
	if roundedNow.Before(now) {
		return time.Until(roundedNow.Add(duration))
	}

	return time.Until(roundedNow)
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}

	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}

	return b
}
