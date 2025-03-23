package monitor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"monitor/pkg/exchange"
	"monitor/pkg/influx"

	"github.com/rs/zerolog/log"
)

type Monitor struct {
	exchange     *exchange.Client
	influx       *influx.Client
	exchangeName string
	clientID     string
	pairs        []string
	lastTrades   map[string]time.Time
	mu           sync.RWMutex
	intervals    []string
}

func New(
	exchangeClient *exchange.Client,
	influxClient *influx.Client,
	exchangeName string,
	clientID string,
	pairs []string,
	intervals []string,
) *Monitor {
	return &Monitor{
		exchange:     exchangeClient,
		influx:       influxClient,
		exchangeName: exchangeName,
		clientID:     clientID,
		pairs:        pairs,
		intervals:    intervals,
		lastTrades:   make(map[string]time.Time),
	}
}

func (m *Monitor) Start(ctx context.Context) error {
	var wg sync.WaitGroup

	// Increase the number of routines by the number of pairs and intervals
	routines := len(m.pairs) * (1 + len(m.intervals))
	if m.exchange.HasCredentials() {
		routines += len(m.pairs)
	}

	errCh := make(chan error, routines)

	for _, pair := range m.pairs {
		// Start monitoring order book
		wg.Add(1)
		go func(pair string) {
			defer wg.Done()
			if err := m.monitorOrderBook(ctx, pair); err != nil {
				errCh <- fmt.Errorf("order book monitor failed for %s: %w", pair, err)
			}
		}(pair)

		// Start monitoring candles
		for _, interval := range m.intervals {
			wg.Add(1)
			go func(pair, interval string) {
				defer wg.Done()
				if err := m.monitorCandles(ctx, pair, interval); err != nil {
					errCh <- fmt.Errorf("candles monitor failed for %s (%s): %w", pair, interval, err)
				}
			}(pair, interval)
		}

		// If exchange has credentials, start monitoring trades
		if m.exchange.HasCredentials() {
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

func parseInterval(interval string) (time.Duration, error) {
	switch interval {
	case "1m":
		return time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported interval: %s", interval)
	}
}

func (m *Monitor) monitorCandles(ctx context.Context, pair string, interval string) error {
	duration, err := parseInterval(interval)
	if err != nil {
		log.Error().
			Str("pair", pair).
			Str("interval", interval).
			Err(err).
			Msg("Failed to parse interval")
		return fmt.Errorf("invalid interval %s: %w", interval, err)
	}

	log.Info().
		Str("pair", pair).
		Str("interval", interval).
		Dur("duration", duration).
		Msg("Starting candles monitor")

	// Начальная точка - 24 часа назад
	since := time.Now().Add(-24 * time.Hour)

	log.Info().
		Str("pair", pair).
		Str("interval", interval).
		Time("since", since).
		Msg("Initial fetch point set")

	// Выполняем первый запрос немедленно
	if err := m.fetchAndSaveCandles(ctx, pair, interval, since); err != nil {
		log.Error().
			Str("pair", pair).
			Str("interval", interval).
			Err(err).
			Msg("Initial candles fetch failed")
	}

	// Устанавливаем тикер на начало следующего периода
	now := time.Now()
	nextTick := now.Truncate(duration).Add(duration)
	initialDelay := nextTick.Sub(now)

	timer := time.NewTimer(initialDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().
				Str("pair", pair).
				Str("interval", interval).
				Msg("Stopping candles monitor")
			return nil
		case <-timer.C:
			if err := m.fetchAndSaveCandles(ctx, pair, interval, since); err != nil {
				log.Error().
					Str("pair", pair).
					Str("interval", interval).
					Err(err).
					Msg("Failed to fetch and save candles")
			}
			// Устанавливаем следующий тик через duration
			timer.Reset(duration)
		}
	}
}

func (m *Monitor) fetchAndSaveCandles(ctx context.Context, pair string, interval string, since time.Time) error {
	log.Debug().
		Str("pair", pair).
		Str("interval", interval).
		Time("since", since).
		Msg("Fetching candles")

	candles, err := m.exchange.FetchCandles(ctx, pair, interval, since)
	if err != nil {
		return fmt.Errorf("failed to fetch candles: %w", err)
	}

	if len(candles) == 0 {
		log.Debug().
			Str("pair", pair).
			Str("interval", interval).
			Msg("No new candles received")
		return nil
	}

	log.Debug().
		Str("pair", pair).
		Str("interval", interval).
		Int("candles_count", len(candles)).
		Msg("Processing fetched candles")

	var lastTimestamp time.Time
	for _, candle := range candles {
		data := influx.Candle{
			Timestamp:   candle.Timestamp,
			Exchange:    m.exchangeName,
			TradingPair: pair,
			Interval:    interval,
			ClientID:    m.clientID,
			Open:        candle.Open,
			High:        candle.High,
			Low:         candle.Low,
			Close:       candle.Close,
			Volume:      candle.Volume,
			TradesCount: candle.TradesCount,
		}

		if err := m.influx.WriteCandle(data); err != nil {
			log.Error().
				Str("pair", pair).
				Str("interval", interval).
				Time("timestamp", candle.Timestamp).
				Err(err).
				Msg("Failed to write candle data")
			continue
		}

		if candle.Timestamp.After(lastTimestamp) {
			lastTimestamp = candle.Timestamp
		}
	}

	log.Info().
		Str("pair", pair).
		Str("interval", interval).
		Int("processed_candles", len(candles)).
		Time("last_timestamp", lastTimestamp).
		Msg("Successfully processed and saved candles")

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
