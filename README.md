# Market Monitor

Market Monitor is a Go application designed to collect and store cryptocurrency market data from various exchanges into InfluxDB. It supports both public order book monitoring and private trade tracking with client-specific data segregation.

## Project Structure

```
📁 market-monitor/
├── Dockerfile              # Production Docker build
├── Dockerfile.dev         # Development Docker build
├── docker-compose.yml    # Local development setup
├── .air.toml            # Hot reload configuration
├── go.mod
├── go.sum
├── main.go
├── config/
│   └── config.go         # Configuration management
├── pkg/
│   ├── exchange/
│   │   └── exchange.go   # Exchange client implementation
│   ├── influx/
│   │   └── client.go     # InfluxDB client implementation
│   └── monitor/
│       └── monitor.go    # Market monitoring logic
└── README.md
```

## Features

- Real-time order book monitoring
- Private trade tracking (with API credentials)
- Multi-client support with data segregation
- InfluxDB time-series storage
- Docker support with multi-architecture builds
- Hot reload for development

## Prerequisites

- Go 1.20 or higher
- Docker and Docker Compose
- Make

## Configuration

The application is configured via environment variables:

### Required Variables

- `EXCHANGE` - Exchange name (e.g., "binance")
- `TRADING_PAIRS` - Comma-separated trading pairs (e.g., "BTC/USDT,ETH/USDT")
- `INFLUX_URL` - InfluxDB URL
- `INFLUX_TOKEN` - InfluxDB access token
- `INFLUX_ORG` - InfluxDB organization
- `INFLUX_BUCKET` - InfluxDB bucket name

### Optional Variables

- `API_KEY` - Exchange API key
- `API_SECRET` - Exchange API secret
- `CLIENT_ID` - Unique client identifier (required when using API credentials)
- `LOG_LEVEL` - Logging level (debug, info, warn, error)
- `LOG_FORMAT` - Log format (text or json)

## Quick Start

1. Clone the repository:

```bash
git clone https://github.com/tokensyndicate/market-monitor.git
cd market-monitor
```

2. Initialize the development environment:

```bash
make init
```

3. Start the local development environment:

```bash
make dev
```

## Development

- **Hot Reload**: The development environment uses Air for hot reloading
- **Local Testing**: InfluxDB is included in the development setup
- **Logs**: View logs with `make logs`

## Building & Deployment

### Local Build

```bash
make build
```

### Docker Build

```bash
make docker-build VERSION=x.x.x
```

### Multi-architecture Build and Push

```bash
make release VERSION=x.x.x
```

## Docker Images

Images are available on Docker Hub:

```bash
docker pull tokensyndicate/market-monitor:latest
```

Supported architectures:

- linux/amd64
- linux/arm64

## Available Make Commands

- `make init` - Initialize development environment
- `make dev` - Start local development environment
- `make dev-down` - Stop local development environment
- `make logs` - Show container logs
- `make test` - Run tests
- `make lint` - Run linter
- `make docker-build` - Build Docker image
- `make docker-buildx` - Build multi-platform Docker images
- `make docker-push` - Push Docker image to Docker Hub
- `make release` - Full release process

## License

This software is proprietary and confidential. No license is granted for any use, modification, or distribution of this software. All rights reserved by Nikolai Evseev.

Unauthorized copying, modification, distribution, or use of this software, via any medium, is strictly prohibited.
