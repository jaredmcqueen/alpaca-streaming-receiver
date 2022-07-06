package handlers

import (
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/redisWriter"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

var (
	tradeChan         = make(chan stream.Trade, util.Config.ChannelQueueSize)
	allowedConditions = strings.Split(util.Config.AllowedTradeConditions, "")
)

func init() {
	go func() {
		for {
			cacheCounter.WithLabelValues("trades").Set(float64(len(tradeChan)))
			time.Sleep(time.Second)
		}
	}()
}

func TradeHandler(t stream.Trade) {
	tradeChan <- t
	websocketCounter.WithLabelValues("trades").Inc()
}

func filterConditions(conditions []string) bool {
Loop:
	for _, c := range conditions {
		for _, a := range allowedConditions {
			if c == a {
				continue Loop
			}
		}
		return false
	}
	return true
}

func ProcessTrades() {
	fmt.Println("starting trades processor")
	fmt.Printf("allowed trade conditions are %+v\n", allowedConditions)

	for t := range tradeChan {

		if filterConditions(t.Conditions) != true {
			continue
		}

		redisWriter.RedisChan <- map[string]interface{}{
			"T": "trades",
			"S": t.Symbol,
			"x": fmt.Sprintf("%v", t.Exchange),
			"p": fmt.Sprintf("%v", t.Price),
			"s": fmt.Sprintf("%v", t.Size),
			"t": fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
			"c": fmt.Sprintf("%s", strings.Join(t.Conditions, "")),
			"z": t.Tape,
		}
		// redisCounter.WithLabelValues("trades").Inc()

		type Trade struct {
			High   float64
			Low    float64
			Volume uint32
		}
		tradeAccumulator := make(map[string]Trade)
		timer := time.NewTimer(nextTick(time.Second))
		for {
			select {
			case t := <-tradeChan:
				if value, ok := tradeAccumulator[t.Symbol]; ok {
					tradeAccumulator[t.Symbol] = Trade{
						High:   max(t.Price, value.High),
						Low:    min(t.Price, value.Low),
						Volume: value.Volume + t.Size,
					}
				} else {
					tradeAccumulator[t.Symbol] = Trade{
						High:   t.Price,
						Low:    t.Price,
						Volume: t.Size,
					}
				}
			case t := <-timer.C:
				for k, v := range tradeAccumulator {
					redisWriter.RedisChan <- map[string]interface{}{
						"T": "trades",
						"S": k,
						"t": t.UnixMilli(),
						"h": v.High,
						"l": v.Low,
						"v": v.Volume,
					}
					redisCounter.WithLabelValues("trades").Inc()
				}

				tradeAccumulator = make(map[string]Trade)
				timer.Reset(nextTick(time.Second))
			}
		}
	}
}
