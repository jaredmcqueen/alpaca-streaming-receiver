package handlers

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	websocketCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alpaca_receiver_handlers_total",
		Help: "message count from alpaca websockets handlers",
	}, []string{"type"})

	redisCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alpaca_receiver_processors_total",
		Help: "messages count from processors",
	}, []string{"type"})
)
