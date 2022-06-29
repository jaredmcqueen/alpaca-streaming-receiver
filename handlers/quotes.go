package handlers

import (
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	quoteChan          = make(chan stream.Quote, util.Config.ChannelQueueSize)
	alpacaQuoteCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alpaca_receiver_websockets_quotes_total",
		Help: "quotes received from alpaca",
	})
	redisQuoteCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alpaca_receiver_redis_writer_quotes_total",
		Help: "quotes written to redis streams",
	})
)

func QuoteHandler(q stream.Quote) {
	quoteChan <- q
	alpacaQuoteCounter.Inc()
}

type Quote struct {
	High float64
	Low  float64
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

// process quotes batches quotes by 1 second and emits max(high) and min(low)
func ProcessQuotes() {
	fmt.Println("starting quote processor")

	quotes := make(map[string]Quote)
	timer := time.NewTimer(nextTick(time.Second))
	for {
		select {
		case q := <-quoteChan:
			if value, ok := quotes[q.Symbol]; ok {
				quotes[q.Symbol] = Quote{
					High: max(q.BidPrice, value.High),
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
				redisQuoteCounter.Inc()
			}
			quotes = make(map[string]Quote)
			timer.Reset(nextTick(time.Second))
		}
	}
}
