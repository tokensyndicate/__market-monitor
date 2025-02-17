package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"monitor/pkg/exchange"
	"monitor/pkg/influx"
	"monitor/pkg/monitor"
)

func main() {
	// Setup logging
	setupLogger()
	log.Info().Msg("Starting market monitor...")

	// Load configuration
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	log.Info().
		Str("exchange", cfg.exchange).
		Strs("pairs", cfg.pairs).
		Str("influxDB", cfg.influxURL).
		Msg("Loaded configuration")

	// Create exchange client
	exchangeClient, err := exchange.NewClient(cfg.exchange, cfg.apiKey, cfg.apiSecret)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create exchange client")
	}

	log.Info().
		Str("exchange", cfg.exchange).
		Strs("pairs", cfg.pairs).
		Str("influxDB", cfg.influxURL).
		Str("clientID", cfg.clientID).
		Msg("Loaded configuration")

	// Create InfluxDB client
	influxClient, err := influx.NewClient(
		cfg.influxURL,
		cfg.influxToken,
		cfg.influxOrg,
		cfg.influxBucket,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create InfluxDB client")
	}
	defer influxClient.Close()
	log.Info().Str("url", cfg.influxURL).Msg("InfluxDB client initialized")

	// Create monitor
	mon := monitor.New(
		exchangeClient,
		influxClient,
		cfg.exchange,
		cfg.clientID,
		cfg.pairs,
	)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
		cancel()
	}()

	// Start monitoring
	log.Info().
		Str("exchange", cfg.exchange).
		Strs("pairs", cfg.pairs).
		Msg("Starting market monitoring")

	if err := mon.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("Monitor failed")
	}
}

type config struct {
	exchange     string
	pairs        []string
	apiKey       string
	apiSecret    string
	clientID     string
	influxURL    string
	influxToken  string
	influxOrg    string
	influxBucket string
}

func loadConfig() (*config, error) {
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
		log.Error().Strs("missing_envs", missingEnvs).Msg("Missing required environment variables")
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missingEnvs, ", "))
	}

	// Optional API credentials
	apiKey := os.Getenv("API_KEY")
	apiSecret := os.Getenv("API_SECRET")
	clientID := os.Getenv("CLIENT_ID")

	pairs := strings.Split(*requiredEnvs["TRADING_PAIRS"], ",")
	for i := range pairs {
		pairs[i] = strings.TrimSpace(pairs[i])
	}

	return &config{
		exchange:     *requiredEnvs["EXCHANGE"],
		pairs:        pairs,
		apiKey:       apiKey,
		apiSecret:    apiSecret,
		clientID:     clientID,
		influxURL:    *requiredEnvs["INFLUX_URL"],
		influxToken:  *requiredEnvs["INFLUX_TOKEN"],
		influxOrg:    *requiredEnvs["INFLUX_ORG"],
		influxBucket: *requiredEnvs["INFLUX_BUCKET"],
	}, nil
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
			log.Warn().Str("level", logLevel).Msg("Invalid log level, using default")
		} else {
			zerolog.SetGlobalLevel(level)
			log.Info().Str("level", level.String()).Msg("Log level set")
		}
	}
}
