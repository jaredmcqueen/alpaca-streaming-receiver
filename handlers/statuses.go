package handlers

import (
	"fmt"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	statusChan    = make(chan stream.TradingStatus, util.Config.ChannelQueueSize)
	statusCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alpaca_receiver_statuses_total",
		Help: "trading statuses",
	}, []string{"type"})
)

func StatusHandler(t stream.TradingStatus) {
	statusChan <- t
	statusCounter.WithLabelValues("websocket").Inc()
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
		statusCounter.WithLabelValues("redis").Inc()
	}
}
