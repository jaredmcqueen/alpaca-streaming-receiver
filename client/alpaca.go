package client

import (
	"context"
	"log"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var promHandlerCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "alpaca_client",
}, []string{"data_feed"})

type alpacaClient struct {
	con stream.StocksClient
}

func NewAlpacaClient(feed string) (*alpacaClient, error) {
	conn := stream.NewStocksClient(feed)

	log.Println("connecting to Alpaca Data Streaming")
	if err := conn.Connect(context.Background()); err != nil {
		return nil, err
	}

	r := alpacaClient{
		con: conn,
	}

	return &r, nil
}

func (r *alpacaClient) AddBarHandler(ch chan any, symbols []string) {
	log.Println("Subscribing to Bars", symbols)
	handler := func(bar stream.Bar) {
		ch <- bar
		promHandlerCounter.WithLabelValues("bars").Inc()
	}
	if err := r.con.SubscribeToBars(handler, symbols...); err != nil {
		log.Fatal(err)
	}
}

func (r *alpacaClient) AddQuoteHandler(ch chan any, symbols []string) {
	log.Println("Subscribing to Quotes", symbols)
	handler := func(quote stream.Quote) {
		ch <- quote
		promHandlerCounter.WithLabelValues("quotes").Inc()
	}
	if err := r.con.SubscribeToQuotes(handler, symbols...); err != nil {
		log.Fatal(err)
	}
}

func (r *alpacaClient) AddTradeHandler(ch chan any, symbols []string) {
	log.Println("Subscribing to Trades", symbols)
	handler := func(trade stream.Trade) {
		ch <- trade
		promHandlerCounter.WithLabelValues("trades").Inc()
	}
	if err := r.con.SubscribeToTrades(handler, symbols...); err != nil {
		log.Fatal(err)
	}
}

func (r *alpacaClient) AddStatusHandler(ch chan any, symbols []string) {
	log.Println("Subscribing to Statuses", symbols)
	handler := func(status stream.TradingStatus) {
		ch <- status
		promHandlerCounter.WithLabelValues("statues").Inc()
	}
	if err := r.con.SubscribeToStatuses(handler, symbols...); err != nil {
		log.Fatal(err)
	}
}
