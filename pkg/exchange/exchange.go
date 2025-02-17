package exchange

import (
	"context"
	"fmt"
	"sync"
	"time"

	ccxt "github.com/ccxt/ccxt/go/v4"
)

type Client struct {
	exchange   ccxt.IExchange
	orderBooks map[string]*OrderBookCache
	mu         sync.RWMutex
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
