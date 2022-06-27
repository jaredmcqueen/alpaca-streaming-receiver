package handlers

import (
	"fmt"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
)

var barChan chan stream.Bar

func init() {
	barChan = make(chan stream.Bar, 100_000)
}

func BarHandler(b stream.Bar) {
	barChan <- b
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
	}
}