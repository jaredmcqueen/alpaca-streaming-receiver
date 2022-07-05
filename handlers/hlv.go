package handlers

import (
	"fmt"
	"time"

	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
)

type HLV struct {
	High   float64
	Low    float64
	Volume uint32
}

// ProcessHLV combines quotes (High/Low) and trades (Volume), emitting a HLV tick
func ProcessHLV() {
	fmt.Println("starting HLV processor")

	hlvMap := make(map[string]HLV)

	timer := time.NewTimer(nextTick(time.Second))
	for {
		select {
		case quote := <-quoteChan:
			if value, ok := hlvMap[quote.Symbol]; ok {
				// entry exists, update the HL
				value.High = max(quote.AskPrice, value.High)
				value.Low = min(quote.BidPrice, value.Low)
				hlvMap[quote.Symbol] = value
			} else {
				// entry does not exist
				hlvMap[quote.Symbol] = HLV{
					Low:  quote.BidPrice,
					High: quote.AskPrice,
				}
			}
		case trade := <-tradeChan:
			if value, ok := hlvMap[trade.Symbol]; ok {
				// entry exists
				value.Volume += trade.Size
				hlvMap[trade.Symbol] = value
			} else {
				// entry does not exist
				hlvMap[trade.Symbol] = HLV{
					Volume: trade.Size,
				}
			}
		case t := <-timer.C:
			for k, v := range hlvMap {
				redisWriter.RedisChan <- map[string]interface{}{
					"T": "ticks",
					"S": k,
					"h": v.High,
					"l": v.Low,
					"v": v.Volume,
					"t": t.UnixMilli(),
				}
				redisCounter.WithLabelValues("ticks").Inc()
			}
			hlvMap = make(map[string]HLV)
			timer.Reset(nextTick(time.Second))
		}
	}
}
