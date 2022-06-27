package handlers

import (
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
)

var quoteChan chan stream.Quote

func init() {
	quoteChan = make(chan stream.Quote, 100_000)
}

type Quote struct {
	High float64
	Low  float64
}

func QuoteHandler(q stream.Quote) {

	quoteChan <- q
}

func nextTick(duration time.Duration) time.Duration {
	now := time.Now()
	roundedNow := now.Round(duration)
	if roundedNow.Before(now) {
		return time.Until(roundedNow.Add(duration))
	}
	return time.Until(roundedNow)
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// process quotes
func ProcessQuotes() {
	fmt.Println("starting quote processor")

	quotes := make(map[string]Quote)
	timer := time.NewTimer(nextTick(time.Second))
	for {
		select {
		case q := <-quoteChan:
			if value, ok := quotes[q.Symbol]; ok {
				quotes[q.Symbol] = Quote{
					High: min(q.BidPrice, value.High),
					Low:  min(q.AskPrice, value.Low),
				}
			} else {
				quotes[q.Symbol] = Quote{
					High: q.BidPrice,
					Low:  q.AskPrice,
				}
			}
		case t := <-timer.C:
			for k, v := range quotes {
				redisWriter.RedisChan <- map[string]interface{}{
					"T": "quotes",
					"S": k,
					"l": v.Low,
					"h": v.High,
					"t": t.UnixMilli(),
				}
			}
			quotes = make(map[string]Quote)
			timer.Reset(nextTick(time.Second))
		}
	}
}
