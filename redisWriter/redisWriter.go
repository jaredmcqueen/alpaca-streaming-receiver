package redisWriter

import (
	"time"

	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RedisChan = make(chan map[string]interface{}, util.Config.ChannelQueueSize)

	cacheCounter = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "alpaca_receiver_redis_writer_channel_cache",
		Help: "cache size from processors",
	})
)

func init() {
	go func() {
		for {
			cacheCounter.Set(float64(len(RedisChan)))
			time.Sleep(time.Second)
		}
	}()
}
