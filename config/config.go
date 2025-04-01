package config

import (
	"fmt"
	"monitor/pkg/types"
	"os"
	"strings"
)

type Config struct {
	Exchange      string
	TradingPairs  []string
	APIKey        string
	APISecret     string
	ClientID      string
	InfluxURL     string
	InfluxToken   string
	InfluxOrg     string
	InfluxBuckets struct {
		Candles      string
		OrderBook    string
		OrderBookAgg string
	}
	Intervals []types.Interval
}

func Load() (*Config, error) {
	// Required for all modes
	requiredEnvs := map[string]*string{
		"EXCHANGE":      nil,
		"TRADING_PAIRS": nil,
		"INFLUX_URL":    nil,
		"INFLUX_TOKEN":  nil,
		"INFLUX_ORG":    nil,
	}

	var missingEnvs []string
	for env := range requiredEnvs {
		if value := os.Getenv(env); value != "" {
			requiredEnvs[env] = &value
		} else {
			missingEnvs = append(missingEnvs, env)
		}
	}

	if len(missingEnvs) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missingEnvs, ", "))
	}

	// Optional API credentials
	apiKey := os.Getenv("API_KEY")
	apiSecret := os.Getenv("API_SECRET")
	clientID := os.Getenv("CLIENT_ID")

	// Parse trading pairs
	pairs := strings.Split(*requiredEnvs["TRADING_PAIRS"], ",")
	for i := range pairs {
		pairs[i] = strings.TrimSpace(pairs[i])
	}

	// Load intervals with defaults if not specified
	intervals := types.DefaultIntervals()
	if envIntervals := os.Getenv("INTERVALS"); envIntervals != "" {
		customIntervals := strings.Split(envIntervals, ",")
		intervals = make([]types.Interval, 0, len(customIntervals))
		for _, interval := range customIntervals {
			intervals = append(intervals, types.Interval(strings.TrimSpace(interval)))
		}
	}

	// Set default bucket names or use environment variables if provided
	candlesBucket := "trading_candles"
	if bucket := os.Getenv("INFLUX_BUCKET_CANDLES"); bucket != "" {
		candlesBucket = bucket
	}

	orderBookBucket := "trading_orderbook"
	if bucket := os.Getenv("INFLUX_BUCKET_ORDERBOOK"); bucket != "" {
		orderBookBucket = bucket
	}

	orderBookAggBucket := "trading_orderbook_agg"
	if bucket := os.Getenv("INFLUX_BUCKET_ORDERBOOK_AGG"); bucket != "" {
		orderBookAggBucket = bucket
	}

	return &Config{
		Exchange:     *requiredEnvs["EXCHANGE"],
		TradingPairs: pairs,
		APIKey:       apiKey,
		APISecret:    apiSecret,
		ClientID:     clientID,
		InfluxURL:    *requiredEnvs["INFLUX_URL"],
		InfluxToken:  *requiredEnvs["INFLUX_TOKEN"],
		InfluxOrg:    *requiredEnvs["INFLUX_ORG"],
		InfluxBuckets: struct {
			Candles      string
			OrderBook    string
			OrderBookAgg string
		}{
			Candles:      candlesBucket,
			OrderBook:    orderBookBucket,
			OrderBookAgg: orderBookAggBucket,
		},
		Intervals: intervals,
	}, nil
}
