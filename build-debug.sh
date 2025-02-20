#!/bin/sh

# Включаем вывод команд
set -x

echo "Starting build debug..."
go version
go env

# Устанавливаем переменные для ограничения параллелизма и управления памятью
export GOMAXPROCS=2
export GOGC=50
export GOPROXY=direct
export GO111MODULE=on

echo "Building ccxt in chunks..."

# Создаем временную директорию для сборки
mkdir -p /tmp/build-cache

# Собираем базовые компоненты ccxt
echo "Building base components..."
go build -v -work -x -gcflags=all="-N -l" \
    github.com/ccxt/ccxt/go/v4/base \
    github.com/ccxt/ccxt/go/v4/config \
    github.com/ccxt/ccxt/go/v4/errors

# Собираем только нужные нам биржи (например, только Binance)
echo "Building specific exchanges..."
go build -v -work -x -gcflags=all="-N -l" \
    github.com/ccxt/ccxt/go/v4/binance.go \
    github.com/ccxt/ccxt/go/v4/binance_api.go \
    github.com/ccxt/ccxt/go/v4/binance_wrapper.go

echo "Building main application..."
go build -v -work -x -gcflags=all="-N -l" ./...
