// config/config.go
package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	Exchange     string
	TradingPairs []string
	APIKey       string
	APISecret    string
	ClientID     string
	InfluxURL    string
	InfluxToken  string
	InfluxOrg    string
	InfluxBucket string
}

func Load() (*Config, error) {
	var missing []string

	// Get environment variables
	exchange := os.Getenv("EXCHANGE")
	tradingPairs := os.Getenv("TRADING_PAIRS")
	apiKey := os.Getenv("API_KEY")
	apiSecret := os.Getenv("API_SECRET")
	clientID := os.Getenv("CLIENT_ID")
	influxURL := os.Getenv("INFLUX_URL")
	influxToken := os.Getenv("INFLUX_TOKEN")
	influxOrg := os.Getenv("INFLUX_ORG")
	influxBucket := os.Getenv("INFLUX_BUCKET")

	// Check required variables
	if exchange == "" {
		missing = append(missing, "EXCHANGE")
	}
	if tradingPairs == "" {
		missing = append(missing, "TRADING_PAIRS")
	}
	if apiKey == "" {
		missing = append(missing, "API_KEY")
	}
	if apiSecret == "" {
		missing = append(missing, "API_SECRET")
	}
	if apiKey != "" && clientID == "" {
		missing = append(missing, "CLIENT_ID")
	}
	if influxURL == "" {
		missing = append(missing, "INFLUX_URL")
	}
	if influxToken == "" {
		missing = append(missing, "INFLUX_TOKEN")
	}
	if influxOrg == "" {
		missing = append(missing, "INFLUX_ORG")
	}
	if influxBucket == "" {
		missing = append(missing, "INFLUX_BUCKET")
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	// Parse trading pairs
	pairs := strings.Split(tradingPairs, ",")
	for i := range pairs {
		pairs[i] = strings.TrimSpace(pairs[i])
	}

	return &Config{
		Exchange:     strings.ToLower(exchange),
		TradingPairs: pairs,
		APIKey:       apiKey,
		APISecret:    apiSecret,
		ClientID:     clientID,
		InfluxURL:    influxURL,
		InfluxToken:  influxToken,
		InfluxOrg:    influxOrg,
		InfluxBucket: influxBucket,
	}, nil
}
