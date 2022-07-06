package handlers

import (
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

var (
	statusChan = make(chan stream.TradingStatus, util.Config.ChannelQueueSize)
)

func init() {
	go func() {
		for {
			cacheCounter.WithLabelValues("statuses").Set(float64(len(statusChan)))
			time.Sleep(time.Second)
		}
	}()
}

func StatusHandler(t stream.TradingStatus) {
	statusChan <- t
	websocketCounter.WithLabelValues("statuses").Inc()
}

func ProcessStatuses() {
	fmt.Println("starting status processor")

	for s := range statusChan {
		redisWriter.RedisChan <- map[string]interface{}{
			"T":  "statuses",
			"S":  s.Symbol,
			"sc": s.StatusCode,
			"sm": s.StatusMsg,
			"rc": s.ReasonCode,
			"rm": s.ReasonMsg,
			"t":  fmt.Sprintf("%v", s.Timestamp.UnixMilli()),
			"z":  s.Tape,
		}
		redisCounter.WithLabelValues("statuses").Inc()
	}
}
