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
    Intervals    []string
}

func Load() (*Config, error) {
    // Required for all modes
    requiredEnvs := map[string]*string{
        "EXCHANGE":      nil,
        "TRADING_PAIRS": nil,
        "INFLUX_URL":    nil,
        "INFLUX_TOKEN":  nil,
        "INFLUX_ORG":    nil,
        "INFLUX_BUCKET": nil,
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
    intervals := []string{"1m", "5m", "1h"}
    if envIntervals := os.Getenv("INTERVALS"); envIntervals != "" {
        intervals = strings.Split(envIntervals, ",")
        for i := range intervals {
            intervals[i] = strings.TrimSpace(intervals[i])
        }
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
        InfluxBucket: *requiredEnvs["INFLUX_BUCKET"],
        Intervals:    intervals,
    }, nil
}
