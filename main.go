package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"monitor/config"
	"monitor/pkg/exchange"
	"monitor/pkg/influx"
	"monitor/pkg/monitor"
)

func main() {
	// Setup logging
	setupLogger()
	log.Info().Msg("Starting market monitor...")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Convert []types.Interval to []string for logging
	intervals := make([]string, len(cfg.Intervals))
	for i, interval := range cfg.Intervals {
		intervals[i] = string(interval)
	}

	log.Info().
		Str("exchange", cfg.Exchange).
		Strs("pairs", cfg.TradingPairs).
		Strs("intervals", intervals).
		Str("influxDB", cfg.InfluxURL).
		Msg("Loaded configuration")

	// Create exchange client
	exchangeClient, err := exchange.NewClient(cfg.Exchange, cfg.APIKey, cfg.APISecret)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create exchange client")
	}

	// Create InfluxDB client
	influxClient, err := influx.NewClient(
		cfg.InfluxURL,
		cfg.InfluxToken,
		cfg.InfluxOrg,
		cfg.InfluxBuckets,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create InfluxDB client")
	}
	defer influxClient.Close()

	log.Info().
		Str("url", cfg.InfluxURL).
		Str("org", cfg.InfluxOrg).
		Str("bucket_candles", cfg.InfluxBuckets.Candles).
		Str("bucket_orderbook", cfg.InfluxBuckets.OrderBook).
		Str("bucket_orderbook_agg", cfg.InfluxBuckets.OrderBookAgg).
		Msg("InfluxDB client initialized")

	// Create monitor
	mon := monitor.New(
		exchangeClient,
		influxClient,
		cfg.Exchange,
		cfg.ClientID,
		cfg.TradingPairs,
		cfg.Intervals,
	)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info().
			Str("signal", sig.String()).
			Msg("Received shutdown signal")
		cancel()
	}()

	// Start monitoring
	log.Info().
		Str("exchange", cfg.Exchange).
		Strs("pairs", cfg.TradingPairs).
		Strs("intervals", intervals).
		Bool("has_credentials", exchangeClient.HasCredentials()).
		Msg("Starting market monitoring")

	if err := mon.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("Monitor failed")
	}
}

func setupLogger() {
	// Set up global logger
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Enable console writer with colors for better readability
	if os.Getenv("LOG_FORMAT") != "json" {
		log.Logger = log.Output(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
			NoColor:    os.Getenv("LOG_NO_COLOR") != "",
		})
	}

	// Set log level from environment
	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		level, err := zerolog.ParseLevel(logLevel)
		if err != nil {
			log.Warn().
				Str("level", logLevel).
				Msg("Invalid log level, using default")
		} else {
			zerolog.SetGlobalLevel(level)
			log.Info().
				Str("level", level.String()).
				Msg("Log level set")
		}
	}
}
