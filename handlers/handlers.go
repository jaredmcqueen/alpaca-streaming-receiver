package handlers

import (
	"fmt"
	"strings"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	"github.com/jaredmcqueen/alpaca-streaming-receiver/util"
)

var TradeHandler = func(t stream.Trade) {

	si := util.StreamItem{Stream: "trades"}

	// https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/realtime/#trade-schema
	// T 	string 	message type, always “t”
	// S 	string 	symbol
	// i 	int 	trade ID
	// x 	string 	exchange code where the trade occurred
	// p 	number 	trade price
	// s 	int 	trade size
	// t 	string 	RFC-3339 formatted timestamp with nanosecond precision
	// c 	array 	trade condition
	// z 	string 	tape

	for i, c := range t.Conditions {
		if c == " " {
			t.Conditions[i] = "@"
		}
	}

	si.Values = map[string]interface{}{
		"S": t.Symbol,
		"x": fmt.Sprintf("%v", t.Exchange),
		"p": fmt.Sprintf("%v", t.Price),
		"s": fmt.Sprintf("%v", t.Size),
		"t": fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
		"c": fmt.Sprintf("%s", strings.Join(t.Conditions, "")),
		"z": t.Tape,
	}

	util.StreamChan <- si
}

var MinuteBarHandler = func(t stream.Bar) {

	si := util.StreamItem{Stream: "minuteBars"}

	// https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/realtime/#minute-bar-schema
	// T 	string 	message type, always “b”
	// S 	string 	symbol
	// o 	number 	open price
	// h 	number 	high price
	// l 	number 	low price
	// c 	number 	close price
	// v 	int 	volume
	// t 	string 	RFC-3339 formatted timestamp

	si.Values = map[string]interface{}{
		"S": t.Symbol,
		"o": fmt.Sprintf("%v", t.Open),
		"h": fmt.Sprintf("%v", t.High),
		"l": fmt.Sprintf("%v", t.Low),
		"c": fmt.Sprintf("%v", t.Close),
		"v": fmt.Sprintf("%v", t.Volume),
		"t": fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
	}

	util.StreamChan <- si
}

var DailyBarHandler = func(t stream.Bar) {

	si := util.StreamItem{Stream: "dailyBars"}

	// https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/realtime/#daily-bar-schema
	// T 	string 	message type, always “d”
	// S 	string 	symbol
	// o 	number 	open price
	// h 	number 	high price
	// l 	number 	low price
	// c 	number 	close price
	// v 	int 	volume
	// t 	string 	RFC-3339 formatted timestamp

	si.Values = map[string]interface{}{
		"S": t.Symbol,
		"o": fmt.Sprintf("%v", t.Open),
		"h": fmt.Sprintf("%v", t.High),
		"l": fmt.Sprintf("%v", t.Low),
		"c": fmt.Sprintf("%v", t.Close),
		"v": fmt.Sprintf("%v", t.Volume),
		"t": fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
	}

	util.StreamChan <- si
}

var StatusHandler = func(t stream.TradingStatus) {

	si := util.StreamItem{Stream: "tradingStatuses"}

	// https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/realtime/#status-schema
	// T 	string 	message type, always “s”
	// S 	string 	symbol
	// sc 	string 	status code
	// sm 	string 	status message
	// rc 	string 	reason code
	// rm 	string 	reason message
	// t 	string 	RFC-3339 formatted timestamp
	// z 	string 	tape

	si.Values = map[string]interface{}{
		"S":  t.Symbol,
		"sc": t.StatusCode,
		"sm": t.StatusMsg,
		"rc": t.ReasonCode,
		"rm": t.ReasonMsg,
		"t":  fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
		"z":  t.Tape,
	}

	util.StreamChan <- si
}

var QuoteHandler = func(t stream.Quote) {

	si := util.StreamItem{Stream: "quotes"}

	// https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/realtime/#quote-schema
	// T 	string 	message type, always “q”
	// S 	string 	symbol
	// ax 	string 	ask exchange code
	// ap 	number 	ask price
	// as 	int 	ask size
	// bx 	string 	bid exchange code
	// bp 	number 	bid price
	// bs 	int 	bid size
	// t 	string 	RFC-3339 formatted timestamp with nanosecond precision
	// c 	array 	quote condition
	// z 	string 	tape

	// for i, c := range t.Conditions {
	// 	if c == " " {
	// 		t.Conditions[i] = "-"
	// 	}
	// }

	si.Values = map[string]interface{}{
		"S":  t.Symbol,
		"ax": t.AskExchange,
		"ap": fmt.Sprintf("%v", t.AskPrice),
		"as": fmt.Sprintf("%v", t.AskSize),
		"bx": t.BidExchange,
		"bp": fmt.Sprintf("%v", t.BidPrice),
		"bs": fmt.Sprintf("%v", t.BidSize),
		"t":  fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
		"c":  fmt.Sprintf("%s", strings.Join(t.Conditions, "")),
		"z":  t.Tape,
	}

	util.StreamChan <- si
}
