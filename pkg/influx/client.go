package influx

import (
	"context"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

type Client struct {
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	org      string
	bucket   string
}

// OrderBookEntry represents a single entry in the order book
type OrderBookEntry struct {
	Timestamp   time.Time
	Exchange    string
	TradingPair string
	ClientID    string
	Level       int
	Side        string
	Price       float64
	Volume      float64
	TotalVolume float64
}

// Trade represents a single trade
type Trade struct {
	Timestamp     time.Time
	Exchange      string
	TradingPair   string
	ClientID      string
	TradeID       string
	OrderID       string
	Side          string
	Price         float64
	Volume        float64
	Value         float64
	FeeAmount     float64
	FeeCurrency   string
	LiquidityRole string
}

// Candle represents OHLCV data
type Candle struct {
	Timestamp   time.Time
	Exchange    string
	TradingPair string
	Interval    string
	ClientID    string
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      float64
	TradesCount int64
}

func NewClient(url, token, org, bucket string) (*Client, error) {
	client := influxdb2.NewClient(url, token)

	// Check connection
	_, err := client.Ping(context.Background())
	if err != nil {
		return nil, err
	}

	return &Client{
		client:   client,
		writeAPI: client.WriteAPIBlocking(org, bucket),
		org:      org,
		bucket:   bucket,
	}, nil
}

func (c *Client) Close() {
	c.client.Close()
}

func (c *Client) WriteOrderBookEntry(entry OrderBookEntry) error {
	p := influxdb2.NewPointWithMeasurement("orderbook")

	// Tags (indexed fields)
	p.AddTag("exchange", entry.Exchange)
	p.AddTag("trading_pair", entry.TradingPair)
	p.AddTag("side", entry.Side)
	if entry.ClientID != "" {
		p.AddTag("client_id", entry.ClientID)
	}

	// Fields (non-indexed)
	p.AddField("level", entry.Level)
	p.AddField("price", entry.Price)
	p.AddField("volume", entry.Volume)
	p.AddField("total_volume", entry.TotalVolume)

	p.SetTime(entry.Timestamp)

	return c.writeAPI.WritePoint(context.Background(), p)
}

func (c *Client) WriteTrade(trade Trade) error {
	p := influxdb2.NewPointWithMeasurement("trades")

	// Tags (indexed fields)
	p.AddTag("exchange", trade.Exchange)
	p.AddTag("trading_pair", trade.TradingPair)
	p.AddTag("client_id", trade.ClientID)
	p.AddTag("side", trade.Side)
	p.AddTag("liquidity_role", trade.LiquidityRole)

	// Fields (non-indexed)
	p.AddField("trade_id", trade.TradeID)
	p.AddField("order_id", trade.OrderID)
	p.AddField("price", trade.Price)
	p.AddField("volume", trade.Volume)
	p.AddField("value", trade.Value)
	p.AddField("fee_amount", trade.FeeAmount)
	p.AddField("fee_currency", trade.FeeCurrency)

	p.SetTime(trade.Timestamp)

	return c.writeAPI.WritePoint(context.Background(), p)
}

func (c *Client) WriteCandle(candle Candle) error {
	p := influxdb2.NewPointWithMeasurement("candles")

	// Tags
	p.AddTag("exchange", candle.Exchange)
	p.AddTag("trading_pair", candle.TradingPair)
	p.AddTag("interval", candle.Interval)
	if candle.ClientID != "" {
		p.AddTag("client_id", candle.ClientID)
	}

	// Fields
	p.AddField("open", candle.Open)
	p.AddField("high", candle.High)
	p.AddField("low", candle.Low)
	p.AddField("close", candle.Close)
	p.AddField("volume", candle.Volume)
	p.AddField("trades_count", candle.TradesCount)

	p.SetTime(candle.Timestamp)

	return c.writeAPI.WritePoint(context.Background(), p)
}
