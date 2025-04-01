package influx

import (
	"context"
	"monitor/pkg/aggregator"
	"monitor/pkg/types"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

type Client struct {
	client    influxdb2.Client
	writeAPIs struct {
		candles      api.WriteAPIBlocking
		orderBook    api.WriteAPIBlocking
		orderBookAgg api.WriteAPIBlocking
	}
	org string
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
	Interval    types.Interval
	ClientID    string
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      float64
	TradesCount int64
}

func NewClient(url, token, org string, buckets struct {
	Candles      string
	OrderBook    string
	OrderBookAgg string
}) (*Client, error) {
	client := influxdb2.NewClient(url, token)

	// Check connection
	_, err := client.Ping(context.Background())
	if err != nil {
		return nil, err
	}

	c := &Client{
		client: client,
		org:    org,
	}

	// Initialize separate write APIs for each bucket
	c.writeAPIs.candles = client.WriteAPIBlocking(org, buckets.Candles)
	c.writeAPIs.orderBook = client.WriteAPIBlocking(org, buckets.OrderBook)
	c.writeAPIs.orderBookAgg = client.WriteAPIBlocking(org, buckets.OrderBookAgg)

	return c, nil
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
	p.AddField("level", float64(entry.Level))
	p.AddField("price", float64(entry.Price))
	p.AddField("volume", float64(entry.Volume))
	p.AddField("total_volume", entry.TotalVolume)

	p.SetTime(entry.Timestamp)

	return c.writeAPIs.orderBook.WritePoint(context.Background(), p)
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
	p.AddField("price", float64(trade.Price))
	p.AddField("volume", float64(trade.Volume))
	p.AddField("value", float64(trade.Value))
	p.AddField("fee_amount", float64(trade.FeeAmount))
	p.AddField("fee_currency", trade.FeeCurrency)

	p.SetTime(trade.Timestamp)

	return c.writeAPIs.orderBook.WritePoint(context.Background(), p)
}

func (c *Client) WriteCandle(candle Candle) error {
	p := influxdb2.NewPointWithMeasurement("candles")

	// Tags
	p.AddTag("exchange", candle.Exchange)
	p.AddTag("trading_pair", candle.TradingPair)
	p.AddTag("interval", candle.Interval.String())
	if candle.ClientID != "" {
		p.AddTag("client_id", candle.ClientID)
	}

	// Fields
	p.AddField("open", float64(candle.Open))
	p.AddField("high", float64(candle.High))
	p.AddField("low", float64(candle.Low))
	p.AddField("close", float64(candle.Close))
	p.AddField("volume", float64(candle.Volume))
	p.AddField("trades_count", candle.TradesCount)

	p.SetTime(candle.Timestamp)

	return c.writeAPIs.candles.WritePoint(context.Background(), p)
}

func (c *Client) WriteOrderBookAggregation(agg aggregator.OrderBookAggregation) error {
	p := influxdb2.NewPointWithMeasurement("orderbook_aggregations")

	// Tags (indexed fields)
	p.AddTag("exchange", agg.Exchange)
	p.AddTag("trading_pair", agg.TradingPair)
	p.AddTag("interval", string(agg.Interval))

	// Price metrics
	p.AddField("mid_price", agg.MidPrice)
	p.AddField("average_spread", agg.AverageSpread)
	p.AddField("min_spread", agg.MinSpread)
	p.AddField("max_spread", agg.MaxSpread)
	p.AddField("spread_volatility", agg.SpreadVolatility)

	// Depth metrics
	p.AddField("bid_depth", agg.BidDepth)
	p.AddField("ask_depth", agg.AskDepth)
	p.AddField("depth_imbalance", agg.DepthImbalance)

	// Pressure metrics
	p.AddField("buy_pressure", agg.BuyPressure)
	p.AddField("sell_pressure", agg.SellPressure)

	// Count metrics
	p.AddField("snapshot_count", agg.SnapshotCount)

	p.SetTime(agg.Timestamp)

	return c.writeAPIs.orderBookAgg.WritePoint(context.Background(), p)
}
