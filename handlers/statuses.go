package handlers

import (
	"fmt"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
)

var statusChan chan stream.TradingStatus

func init() {
	statusChan = make(chan stream.TradingStatus, 100_000)
}

func StatusHandler(t stream.TradingStatus) {
	statusChan <- t
}

func ProcessStatuses() {
	fmt.Println("starting status processor")

	for s := range statusChan {
		redisWriter.RedisChan <- map[string]interface{}{
			"T":  "status",
			"S":  s.Symbol,
			"sc": s.StatusCode,
			"sm": s.StatusMsg,
			"rc": s.ReasonCode,
			"rm": s.ReasonMsg,
			"t":  fmt.Sprintf("%v", s.Timestamp.UnixMilli()),
			"z":  s.Tape,
		}
	}
}
