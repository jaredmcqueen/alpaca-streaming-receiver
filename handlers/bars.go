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
	barChan          = make(chan stream.Bar, util.Config.ChannelQueueSize)
	alpacaBarCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alpaca_receiver_websockets_bars_total",
		Help: "minute bars received from alpaca",
	})
	redisBarCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alpaca_receiver_redis_writer_bars_total",
		Help: "bars written to redis streams",
	})
)

func BarHandler(b stream.Bar) {
	barChan <- b
	alpacaBarCounter.Inc()
}

func ProcessBars() {
	fmt.Println("starting bar processor")

	for b := range barChan {
		redisWriter.RedisChan <- map[string]interface{}{
			"T": "bars",
			"S": b.Symbol,
			"o": fmt.Sprintf("%v", b.Open),
			"h": fmt.Sprintf("%v", b.High),
			"l": fmt.Sprintf("%v", b.Low),
			"c": fmt.Sprintf("%v", b.Close),
			"v": fmt.Sprintf("%v", b.Volume),
			"t": fmt.Sprintf("%v", b.Timestamp.UnixMilli()),
		}
		redisBarCounter.Inc()
	}
}
