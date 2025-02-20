#!/bin/bash

# Clean vendor
rm -rf vendor/

# Update dependencies
go mod tidy

# Create vendor
go mod vendor

# Check build locally
CGO_ENABLED=1 GOOS=linux GOARCH=arm64 go build -mod=vendor -v ./...
