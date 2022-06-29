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
	barChan    = make(chan stream.Bar, util.Config.ChannelQueueSize)
	barCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alpaca_receiver_bars_total",
		Help: "minute bars",
	}, []string{"type"})
)

func BarHandler(b stream.Bar) {
	barChan <- b
	barCounter.WithLabelValues("websocket").Inc()
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
		barCounter.WithLabelValues("redis").Inc()
	}
}
