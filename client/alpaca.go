package client

import (
	"context"
	"encoding/json"
	"log"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var promHandlerCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "alpaca_client",
}, []string{"data_feed"})

type AlpacaClient struct {
	Conn stream.StocksClient
}

func NewAlpacaClient(feed string) *AlpacaClient {
	conn := stream.NewStocksClient(feed)

	log.Println("connecting to Alpaca Data Streaming")
	if err := conn.Connect(context.Background()); err != nil {
		log.Fatal(err)
	}

	r := AlpacaClient{
		Conn: conn,
	}

	return &r
}

func encodeJSON(v any) []byte {
	thing, err := json.Marshal(v)
	if err != nil {
		log.Fatal(err)
	}
	return thing
}

func (r *AlpacaClient) AddBarHandler(ch chan []byte, symbols []string) {
	log.Println("Subscribing to Bars", symbols)
	handler := func(bar stream.Bar) {
		ch <- encodeJSON(bar)
		promHandlerCounter.WithLabelValues("bars").Inc()
	}
	if err := r.Conn.SubscribeToBars(handler, symbols...); err != nil {
		log.Fatal(err)
	}
}

func (r *AlpacaClient) AddQuoteHandler(ch chan []byte, symbols []string) {
	log.Println("Subscribing to Quotes", symbols)
	handler := func(quote stream.Quote) {
		ch <- encodeJSON(quote)
		promHandlerCounter.WithLabelValues("quotes").Inc()
	}
	if err := r.Conn.SubscribeToQuotes(handler, symbols...); err != nil {
		log.Fatal(err)
	}
}

func (r *AlpacaClient) AddTradeHandler(ch chan []byte, symbols []string) {
	log.Println("Subscribing to Trades", symbols)
	handler := func(trade stream.Trade) {
		ch <- encodeJSON(trade)
		promHandlerCounter.WithLabelValues("trades").Inc()
	}
	if err := r.Conn.SubscribeToTrades(handler, symbols...); err != nil {
		log.Fatal(err)
	}
}

func (r *AlpacaClient) AddStatusHandler(ch chan []byte, symbols []string) {
	log.Println("Subscribing to Statuses", symbols)
	handler := func(status stream.TradingStatus) {
		ch <- encodeJSON(status)
		promHandlerCounter.WithLabelValues("statues").Inc()
	}
	if err := r.Conn.SubscribeToStatuses(handler, symbols...); err != nil {
		log.Fatal(err)
	}
}
