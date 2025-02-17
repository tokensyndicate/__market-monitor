#!/bin/bash

export EXCHANGE=binance
export TRADING_PAIRS=BTC/USDT,ETH/USDT
export CLIENT_ID=test-client
export INFLUX_URL=http://localhost:8086
export INFLUX_TOKEN=my-super-secret-token
export INFLUX_ORG=myorg
export INFLUX_BUCKET=trading
export LOG_LEVEL=debug

go run main.go
