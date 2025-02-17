FROM golang:1.23.4-alpine AS builder

WORKDIR /app
COPY . .

# Install necessary build tools
RUN apk add --no-cache git make gcc musl-dev

RUN go mod download
RUN CGO_ENABLED=1 GOOS=linux go build -o market-monitor

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/market-monitor .

ENV CONFIG_PASSWORD=""
ENV EXCHANGE=""
ENV TRADING_PAIRS=""
ENV API_KEY=""
ENV API_SECRET=""
ENV INFLUX_URL=""
ENV INFLUX_TOKEN=""
ENV INFLUX_ORG=""
ENV INFLUX_BUCKET=""

CMD ["./market-monitor"]
