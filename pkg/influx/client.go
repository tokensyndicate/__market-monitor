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
    Timestamp    time.Time
    Exchange     string
    TradingPair  string
    ClientID     string  // Optional, for client-specific views
    Level        int     // Order book level (0 is top of book)
    Side         string  // "bid" or "ask"
    Price        float64
    Volume       float64
    TotalVolume  float64 // Cumulative volume up to this level
}

// Trade represents a single trade
type Trade struct {
    Timestamp      time.Time
    Exchange       string
    TradingPair    string
    ClientID       string    // Required for client trades
    TradeID        string
    OrderID        string
    Side           string    // "buy" or "sell"
    Price          float64
    Volume         float64
    Value          float64   // Price * Volume
    FeeAmount      float64
    FeeCurrency    string
    LiquidityRole  string    // "maker" or "taker"
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
