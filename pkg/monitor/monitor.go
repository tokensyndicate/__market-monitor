package monitor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"monitor/pkg/exchange"
	"monitor/pkg/influx"
)

type Monitor struct {
	exchange     *exchange.Client
	influx       *influx.Client
	exchangeName string
	clientID     string
	pairs        []string
	lastTrades   map[string]time.Time
	mu           sync.RWMutex
}

func New(
	exchangeClient *exchange.Client,
	influxClient *influx.Client,
	exchangeName string,
	clientID string,
	pairs []string,
) *Monitor {
	return &Monitor{
		exchange:     exchangeClient,
		influx:       influxClient,
		exchangeName: exchangeName,
		clientID:     clientID,
		pairs:        pairs,
		lastTrades:   make(map[string]time.Time),
	}
}

func (m *Monitor) Start(ctx context.Context) error {
	var wg sync.WaitGroup

	// Calculate number of routines
	// Order book monitoring for all pairs
	routines := len(m.pairs)

	// Trade monitoring only if we have API credentials
	hasCredentials := m.exchange.HasCredentials()
	if hasCredentials {
		routines *= 2
	}

	errCh := make(chan error, routines)

	for _, pair := range m.pairs {
		wg.Add(1)
		// Order book monitoring always runs
		go func(pair string) {
			defer wg.Done()
			if err := m.monitorOrderBook(ctx, pair); err != nil {
				errCh <- fmt.Errorf("order book monitor failed for %s: %w", pair, err)
			}
		}(pair)

		// Trade monitoring only if we have credentials
		if hasCredentials {
			wg.Add(1)
			go func(pair string) {
				defer wg.Done()
				if err := m.monitorTrades(ctx, pair); err != nil {
					errCh <- fmt.Errorf("trades monitor failed for %s: %w", pair, err)
				}
			}(pair)
		}
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		return err
	}

	return nil
}

func (m *Monitor) monitorOrderBook(ctx context.Context, pair string) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			book, err := m.exchange.FetchOrderBook(ctx, pair)
			if err != nil {
				log.Printf("Failed to fetch order book for %s: %v", pair, err)
				continue
			}

			timestamp := time.Now()

			// Process bids
			for i, bid := range book.Bids {
				totalVolume := 0.0
				for j := 0; j <= i; j++ {
					totalVolume += book.Bids[j][1]
				}

				entry := influx.OrderBookEntry{
					Timestamp:   timestamp,
					Exchange:    m.exchangeName,
					TradingPair: pair,
					ClientID:    m.clientID,
					Level:       i,
					Side:        "bid",
					Price:       bid[0],
					Volume:      bid[1],
					TotalVolume: totalVolume,
				}

				if err := m.influx.WriteOrderBookEntry(entry); err != nil {
					log.Printf("Failed to write bid entry: %v", err)
				}
			}

			// Process asks
			for i, ask := range book.Asks {
				totalVolume := 0.0
				for j := 0; j <= i; j++ {
					totalVolume += book.Asks[j][1]
				}

				entry := influx.OrderBookEntry{
					Timestamp:   timestamp,
					Exchange:    m.exchangeName,
					TradingPair: pair,
					ClientID:    m.clientID,
					Level:       i,
					Side:        "ask",
					Price:       ask[0],
					Volume:      ask[1],
					TotalVolume: totalVolume,
				}

				if err := m.influx.WriteOrderBookEntry(entry); err != nil {
					log.Printf("Failed to write ask entry: %v", err)
				}
			}
		}
	}
}

func (m *Monitor) monitorTrades(ctx context.Context, pair string) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	// Initialize last trade time
	m.mu.Lock()
	m.lastTrades[pair] = time.Now().Add(-time.Hour * 24)
	m.mu.Unlock()

	// First run - get historical data
	if err := m.recoverHistoricalTrades(ctx, pair); err != nil {
		log.Printf("Failed to recover historical trades for %s: %v", pair, err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			m.mu.RLock()
			since := m.lastTrades[pair]
			m.mu.RUnlock()

			trades, err := m.exchange.FetchMyTrades(ctx, pair, since)
			if err != nil {
				log.Printf("Failed to fetch trades for %s: %v", pair, err)
				continue
			}

			if len(trades) > 0 {
				var lastTimestamp time.Time
				for _, t := range trades {
					trade, ok := t.(map[string]interface{})
					if !ok {
						log.Printf("Invalid trade data type for %s", pair)
						continue
					}

					// Extract trade data safely
					timestamp, _ := trade["timestamp"].(int64)
					orderId, _ := trade["id"].(string)
					tradeId, _ := trade["id"].(string)
					side, _ := trade["side"].(string)
					price, _ := trade["price"].(float64)
					amount, _ := trade["amount"].(float64)
					cost, _ := trade["cost"].(float64)
					liquidityRole, _ := trade["takerOrMaker"].(string)

					// Extract fee
					var feeCost float64
					var feeCurrency string
					if feeData, ok := trade["fee"].(map[string]interface{}); ok {
						if cost, ok := feeData["cost"].(float64); ok {
							feeCost = cost
						}
						if currency, ok := feeData["currency"].(string); ok {
							feeCurrency = currency
						}
					}

					data := influx.Trade{
						Timestamp:     time.UnixMilli(timestamp),
						Exchange:      m.exchangeName,
						TradingPair:   pair,
						ClientID:      m.clientID,
						TradeID:       tradeId,
						OrderID:       orderId,
						Side:          side,
						Price:         price,
						Volume:        amount,
						Value:         cost,
						FeeAmount:     feeCost,
						FeeCurrency:   feeCurrency,
						LiquidityRole: liquidityRole,
					}

					if err := m.influx.WriteTrade(data); err != nil {
						log.Printf("Failed to write trade data: %v", err)
					}

					tradeTime := time.UnixMilli(timestamp)
					if tradeTime.After(lastTimestamp) {
						lastTimestamp = tradeTime
					}
				}

				if !lastTimestamp.IsZero() {
					m.mu.Lock()
					m.lastTrades[pair] = lastTimestamp
					m.mu.Unlock()
				}
			}
		}
	}
}

func (m *Monitor) recoverHistoricalTrades(ctx context.Context, pair string) error {
	end := time.Now()
	start := end.Add(-time.Hour * 24)

	trades, err := m.exchange.FetchHistoricalTrades(ctx, pair, start, end)
	if err != nil {
		return fmt.Errorf("failed to recover historical trades: %w", err)
	}

	var lastTimestamp time.Time
	for _, t := range trades {
		trade, ok := t.(map[string]interface{})
		if !ok {
			log.Printf("Invalid historical trade data type for %s", pair)
			continue
		}

		timestamp, _ := trade["timestamp"].(int64)
		orderId, _ := trade["id"].(string)
		tradeId, _ := trade["id"].(string)
		side, _ := trade["side"].(string)
		price, _ := trade["price"].(float64)
		amount, _ := trade["amount"].(float64)
		cost, _ := trade["cost"].(float64)
		liquidityRole, _ := trade["takerOrMaker"].(string)

		var feeCost float64
		var feeCurrency string
		if feeData, ok := trade["fee"].(map[string]interface{}); ok {
			if cost, ok := feeData["cost"].(float64); ok {
				feeCost = cost
			}
			if currency, ok := feeData["currency"].(string); ok {
				feeCurrency = currency
			}
		}

		data := influx.Trade{
			Timestamp:     time.UnixMilli(timestamp),
			Exchange:      m.exchangeName,
			TradingPair:   pair,
			ClientID:      m.clientID,
			TradeID:       tradeId,
			OrderID:       orderId,
			Side:          side,
			Price:         price,
			Volume:        amount,
			Value:         cost,
			FeeAmount:     feeCost,
			FeeCurrency:   feeCurrency,
			LiquidityRole: liquidityRole,
		}

		if err := m.influx.WriteTrade(data); err != nil {
			log.Printf("Failed to write historical trade data: %v", err)
		}

		tradeTime := time.UnixMilli(timestamp)
		if tradeTime.After(lastTimestamp) {
			lastTimestamp = tradeTime
		}
	}

	if !lastTimestamp.IsZero() {
		m.mu.Lock()
		m.lastTrades[pair] = lastTimestamp
		m.mu.Unlock()
	}

	return nil
}
