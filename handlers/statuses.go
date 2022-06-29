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
	statusChan          = make(chan stream.TradingStatus, util.Config.ChannelQueueSize)
	alpacaStatusCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alpaca_receiver_websockets_statuses_total",
		Help: "trading statuses received from alpaca",
	})
	redisStatusCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alpaca_receiver_redis_writer_statuses_total",
		Help: "trade statuses written to redis streams",
	})
)

func StatusHandler(t stream.TradingStatus) {
	statusChan <- t
	alpacaTradeCounter.Inc()
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
		alpacaStatusCounter.Inc()
	}
}
