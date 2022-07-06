package handlers

import (
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

var (
	barChan = make(chan stream.Bar, util.Config.ChannelQueueSize)
)

func init() {
	go func() {
		for {
			cacheCounter.WithLabelValues("bars").Set(float64(len(quoteChan)))
			time.Sleep(time.Second)
		}
	}()
}

func BarHandler(b stream.Bar) {
	barChan <- b
	websocketCounter.WithLabelValues("bars").Inc()
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
		redisCounter.WithLabelValues("bars").Inc()
	}
}
