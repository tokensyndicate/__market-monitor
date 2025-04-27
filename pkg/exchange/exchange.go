package exchange

import (
	"context"
	"fmt"
	"monitor/pkg/types"
	"strconv"
	"sync"
	"time"

	ccxt "github.com/ccxt/ccxt/go/v4"
	"github.com/rs/zerolog/log"
)

type Client struct {
	exchange   ccxt.IExchange
	orderBooks map[string]*OrderBookCache
	mu         sync.RWMutex
}

type Candle struct {
	Timestamp   time.Time
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      float64
	TradesCount int64
}

type OrderBookCache struct {
	Data      *OrderBook
	Timestamp time.Time
}

type OrderBook struct {
	Timestamp time.Time
	Bids      [][2]float64
	Asks      [][2]float64
}

func NewClient(name, apiKey, apiSecret string) (*Client, error) {
	// Create configuration
	config := map[string]interface{}{
		"enableRateLimit": true,
	}

	// Add credentials only if both are provided
	if apiKey != "" && apiSecret != "" {
		config["apiKey"] = apiKey
		config["secret"] = apiSecret
	}

	// Create exchange instance dynamically
	exchange, ok := ccxt.DynamicallyCreateInstance(name, config)
	if !ok {
		return nil, fmt.Errorf("failed to create exchange instance: %s", name)
	}

	// Load markets and validate
	ch := exchange.LoadMarkets(nil)
	markets := <-ch

	if ccxt.IsError(markets) {
		return nil, fmt.Errorf("failed to load markets: %v", ccxt.CreateReturnError(markets))
	}

	return &Client{
		exchange:   exchange,
		orderBooks: make(map[string]*OrderBookCache),
	}, nil
}

func (c *Client) HasCredentials() bool {
	if exchangeImpl, ok := c.exchange.(*ccxt.Exchange); ok {
		return exchangeImpl.ApiKey != "" && exchangeImpl.Secret != ""
	}
	return false
}

func (c *Client) FetchCandles(ctx context.Context, symbol string, interval types.Interval, since time.Time) ([]Candle, error) {
	log.Info().
		Str("symbol", symbol).
		Str("interval", string(interval)).
		Time("since", since).
		Msg("Fetching candles from exchange")

	// Fetch candles
	ch := c.exchange.FetchOHLCV(symbol, interval, nil, 1000)
	result := <-ch

	if ccxt.IsError(result) {
		log.Error().
			Str("symbol", symbol).
			Str("interval", interval.String()).
			Err(ccxt.CreateReturnError(result)).
			Msg("Failed to fetch candles from exchange")
		return nil, fmt.Errorf("failed to fetch candles: %v", ccxt.CreateReturnError(result))
	}

	ohlcvData, ok := result.([]interface{})
	if !ok {
		log.Error().
			Str("symbol", symbol).
			Str("interval", interval.String()).
			Interface("result", result).
			Msg("Invalid OHLCV response type")
		return nil, fmt.Errorf("invalid OHLCV response type")
	}

	log.Debug().
		Str("symbol", symbol).
		Str("interval", interval.String()).
		Int("candles_count", len(ohlcvData)).
		Msg("Received candles from exchange")

	candles := make([]Candle, 0, len(ohlcvData))
	for i, data := range ohlcvData {
		candleData, ok := data.([]interface{})
		if !ok || len(candleData) < 6 {
			log.Warn().
				Str("symbol", symbol).
				Str("interval", interval.String()).
				Int("index", i).
				Interface("data", data).
				Msg("Invalid candle data format")
			continue
		}

		// Проверяем timestamp и конвертируем его правильно
		var timestamp int64
		switch ts := candleData[0].(type) {
		case float64:
			timestamp = int64(ts)
		case int64:
			timestamp = ts
		case int:
			timestamp = int64(ts)
		default:
			log.Warn().
				Str("symbol", symbol).
				Str("interval", interval.String()).
				Int("index", i).
				Interface("timestamp", candleData[0]).
				Msg("Invalid timestamp type")
			continue
		}

		// Проверяем, что timestamp находится в прошлом
		candleTime := time.UnixMilli(timestamp)
		if candleTime.After(time.Now()) {
			log.Warn().
				Str("symbol", symbol).
				Str("interval", interval.String()).
				Int("index", i).
				Time("candle_time", candleTime).
				Msg("Skipping future candle")
			continue
		}

		open, _ := toFloat64(candleData[1])
		high, _ := toFloat64(candleData[2])
		low, _ := toFloat64(candleData[3])
		close, _ := toFloat64(candleData[4])
		volume, _ := toFloat64(candleData[5])

		var tradesCount int64
		if len(candleData) > 6 {
			if count, ok := toFloat64(candleData[6]); ok {
				tradesCount = int64(count)
			}
		}

		candles = append(candles, Candle{
			Timestamp:   candleTime,
			Open:        open,
			High:        high,
			Low:         low,
			Close:       close,
			Volume:      volume,
			TradesCount: tradesCount,
		})
	}

	if len(candles) == 0 {
		log.Warn().
			Str("symbol", symbol).
			Str("interval", interval.String()).
			Msg("No valid candles received")
		return []Candle{}, nil
	}

	log.Info().
		Str("symbol", symbol).
		Str("interval", interval.String()).
		Int("processed_candles", len(candles)).
		Time("first_candle", candles[0].Timestamp).
		Time("last_candle", candles[len(candles)-1].Timestamp).
		Msg("Successfully processed candles")

	return candles, nil
}

func toFloat64(v interface{}) (float64, bool) {
	switch i := v.(type) {
	case float64:
		return i, true
	case int64:
		return float64(i), true
	case int:
		return float64(i), true
	case string:
		if f, err := strconv.ParseFloat(i, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func (c *Client) FetchOrderBook(ctx context.Context, symbol string) (*OrderBook, error) {
	c.mu.RLock()
	if cached, ok := c.orderBooks[symbol]; ok {
		if time.Since(cached.Timestamp) < time.Second {
			c.mu.RUnlock()
			return cached.Data, nil
		}
	}
	c.mu.RUnlock()

	ch := c.exchange.FetchOrderBook(symbol)
	result := <-ch

	if ccxt.IsError(result) {
		return nil, fmt.Errorf("failed to fetch order book: %v", ccxt.CreateReturnError(result))
	}

	book, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid order book response type")
	}

	bids := parseOrders(book["bids"])
	asks := parseOrders(book["asks"])

	orderBook := &OrderBook{
		Timestamp: time.Now(),
		Bids:      bids,
		Asks:      asks,
	}

	c.mu.Lock()
	c.orderBooks[symbol] = &OrderBookCache{
		Data:      orderBook,
		Timestamp: time.Now(),
	}
	c.mu.Unlock()

	log.Debug().
		Str("symbol", symbol).
		Int("bids_count", len(bids)).
		Int("asks_count", len(asks)).
		Msg("Fetched order book from exchange")

	return orderBook, nil
}

func (c *Client) FetchMyTrades(ctx context.Context, symbol string, since time.Time) ([]interface{}, error) {
	params := map[string]interface{}{
		"since": since.UnixMilli(),
	}

	ch := c.exchange.FetchMyTrades(symbol, 1000, params)
	result := <-ch

	if ccxt.IsError(result) {
		return nil, fmt.Errorf("failed to fetch trades: %v", ccxt.CreateReturnError(result))
	}

	trades, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid trades response type")
	}

	return trades, nil
}

func (c *Client) FetchHistoricalTrades(ctx context.Context, symbol string, start, end time.Time) ([]interface{}, error) {
	var allTrades []interface{}
	current := start

	for current.Before(end) {
		params := map[string]interface{}{
			"since": current.UnixMilli(),
		}

		ch := c.exchange.FetchMyTrades(symbol, 1000, params)
		result := <-ch

		if ccxt.IsError(result) {
			return nil, fmt.Errorf("failed to fetch historical trades: %v", ccxt.CreateReturnError(result))
		}

		trades, ok := result.([]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid trades response type")
		}

		if len(trades) == 0 {
			break
		}

		allTrades = append(allTrades, trades...)

		// Update current time based on last trade
		if lastTrade, ok := trades[len(trades)-1].(map[string]interface{}); ok {
			if timestamp, exists := lastTrade["timestamp"].(int64); exists {
				current = time.UnixMilli(timestamp)
			}
		}

		// Get rate limit from exchange
		if exchangeImpl, ok := c.exchange.(*ccxt.Exchange); ok {
			if exchangeImpl.RateLimit > 0 {
				time.Sleep(time.Duration(exchangeImpl.RateLimit) * time.Millisecond)
			}
		}
	}

	return allTrades, nil
}

func parseOrders(data interface{}) [][2]float64 {
	if data == nil {
		return nil
	}

	orders, ok := data.([]interface{})
	if !ok {
		return nil
	}

	result := make([][2]float64, len(orders))
	for i, order := range orders {
		if orderArr, ok := order.([]interface{}); ok && len(orderArr) >= 2 {
			if price, ok := orderArr[0].(float64); ok {
				if amount, ok := orderArr[1].(float64); ok {
					result[i] = [2]float64{price, amount}
				}
			}
		}
	}

	return result
}
