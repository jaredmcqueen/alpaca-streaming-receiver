package handlers

import (
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

var (
	quoteChan = make(chan stream.Quote, util.Config.ChannelQueueSize)
)

func init() {
	go func() {
		for {
			cacheCounter.WithLabelValues("quotes").Set(float64(len(quoteChan)))
			time.Sleep(time.Second)
		}
	}()
}

func QuoteHandler(q stream.Quote) {
	quoteChan <- q
	websocketCounter.WithLabelValues("quotes").Inc()
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

	quoteAccumulator := make(map[string]Quote)
	timer := time.NewTimer(nextTick(time.Second))

	for {
		select {
		case q := <-quoteChan:
			if value, ok := quoteAccumulator[q.Symbol]; ok {
				quoteAccumulator[q.Symbol] = Quote{
					Low:  min(q.BidPrice, value.Low),
					High: max(q.AskPrice, value.High),
				}
			} else {
				quoteAccumulator[q.Symbol] = Quote{
					Low:  q.BidPrice,
					High: q.AskPrice,
				}
			}
		case t := <-timer.C:
			for k, v := range quoteAccumulator {
				redisWriter.RedisChan <- map[string]interface{}{
					"T": "quotes",
					"S": k,
					"l": v.Low,
					"h": v.High,
					"t": t.UnixMilli(),
				}
				redisCounter.WithLabelValues("quotes").Inc()
			}

			quoteAccumulator = make(map[string]Quote)
			timer.Reset(nextTick(time.Second))
		}
	}
}
