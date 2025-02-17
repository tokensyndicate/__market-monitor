# Makefile

# Variables
DOCKER_COMPOSE = docker-compose
GO = go
DOCKER = docker
IMAGE_NAME = evseevnn/market-monitor
GIT_TAG = $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.1")
VERSION ?= $(GIT_TAG)
DOCKER_PLATFORMS = linux/amd64

# Default target
.PHONY: all
all: build

# Build the application
.PHONY: build
build:
	CGO_ENABLED=0 GOOS=linux $(GO) build -o bin/market-monitor

# Run tests
.PHONY: test
test:
	$(GO) test -v ./...

# Clean build artifacts
.PHONY: clean
clean:
	rm -rf bin/
	rm -rf tmp/

# Start local development environment
.PHONY: dev
dev:
	$(DOCKER_COMPOSE) up --build

# Stop local development environment
.PHONY: dev-down
dev-down:
	$(DOCKER_COMPOSE) down -v

# Show logs
.PHONY: logs
logs:
	$(DOCKER_COMPOSE) logs -f

# Initialize local development environment
.PHONY: init
init:
	$(GO) mod tidy
	$(GO) mod verify

# Run linter
.PHONY: lint
lint:
	golangci-lint run

.PHONY: docker-build
docker-build:
	$(DOCKER) build -t $(IMAGE_NAME):$(VERSION) .
	$(DOCKER) tag $(IMAGE_NAME):$(VERSION) $(IMAGE_NAME):latest

# Build multi-platform images
.PHONY: docker-buildx
docker-buildx:
	$(DOCKER) buildx create --use --name market-monitor-builder || true
	$(DOCKER) buildx build --platform $(DOCKER_PLATFORMS) \
		-t $(IMAGE_NAME):$(VERSION) \
		-t $(IMAGE_NAME):latest \
		--push .

# Push Docker image
.PHONY: docker-push
docker-push: docker-build
	$(DOCKER) push $(IMAGE_NAME):$(VERSION)
	$(DOCKER) push $(IMAGE_NAME):latest

# Login to Docker Hub
.PHONY: docker-login
docker-login:
	@echo "Logging in to Docker Hub..."
	@$(DOCKER) login

# Full release process
.PHONY: release
release: docker-login docker-buildx

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  make          : Build the application"
	@echo "  make test     : Run tests"
	@echo "  make clean    : Clean build artifacts"
	@echo "  make dev      : Start local development environment"
	@echo "  make dev-down : Stop local development environment"
	@echo "  make logs     : Show container logs"
	@echo "  make init     : Initialize development environment"
	@echo "  make lint     : Run linter"
	@echo ""
	@echo "Docker commands:"
	@echo "  make docker-build   : Build Docker image"
	@echo "  make docker-buildx  : Build multi-platform Docker images"
	@echo "  make docker-push    : Push Docker image to Docker Hub"
	@echo "  make docker-login   : Login to Docker Hub"
	@echo "  make release        : Full release process"
	@echo ""
	@echo "Use VERSION=x.x.x to specify version"
