package aggregator

import (
	"math"
	"monitor/pkg/types"
	"sync"
	"time"
)

// OrderBookSnapshot represents a single orderbook state
type OrderBookSnapshot struct {
	Timestamp   time.Time
	Exchange    string
	TradingPair string
	Bids        [][2]float64
	Asks        [][2]float64
}

// OrderBookAggregation represents aggregated orderbook metrics
type OrderBookAggregation struct {
	Timestamp   time.Time
	Exchange    string
	TradingPair string
	Interval    types.Interval

	// Price metrics
	MidPrice         float64
	AverageSpread    float64
	MinSpread        float64
	MaxSpread        float64
	SpreadVolatility float64

	// Depth metrics
	BidDepth       float64 // Total volume within 2% of mid price
	AskDepth       float64 // Total volume within 2% of mid price
	DepthImbalance float64 // (BidDepth - AskDepth) / (BidDepth + AskDepth)

	// Pressure metrics
	BuyPressure  float64 // Relative buying pressure
	SellPressure float64 // Relative selling pressure

	// Count metrics
	SnapshotCount int // Number of snapshots used in aggregation
}

// OrderBookAggregator handles orderbook data aggregation
type OrderBookAggregator struct {
	interval    types.Interval
	snapshots   []*OrderBookSnapshot
	mu          sync.RWMutex
	lastFlush   time.Time
	depthRange  float64 // Percentage range for depth calculation
	onAggregate func(OrderBookAggregation) error
}

// NewOrderBookAggregator creates a new aggregator instance
func NewOrderBookAggregator(
	interval types.Interval,
	depthRange float64,
	onAggregate func(OrderBookAggregation) error,
) *OrderBookAggregator {
	return &OrderBookAggregator{
		interval:    interval,
		snapshots:   make([]*OrderBookSnapshot, 0),
		depthRange:  depthRange,
		onAggregate: onAggregate,
		lastFlush:   time.Now(),
	}
}

// AddSnapshot adds a new orderbook snapshot for aggregation
func (a *OrderBookAggregator) AddSnapshot(snapshot *OrderBookSnapshot) error {
	a.mu.Lock()
	a.snapshots = append(a.snapshots, snapshot)
	duration, err := a.interval.Duration()
	if err != nil {
		a.mu.Unlock()
		return err
	}
	shouldFlush := time.Since(a.lastFlush) >= duration
	a.mu.Unlock()

	if shouldFlush {
		return a.Flush()
	}
	return nil
}

// Flush forces aggregation of current snapshots
func (a *OrderBookAggregator) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.snapshots) == 0 {
		return nil
	}

	agg := a.aggregate()
	a.snapshots = make([]*OrderBookSnapshot, 0)
	a.lastFlush = time.Now()

	if a.onAggregate != nil {
		return a.onAggregate(agg)
	}
	return nil
}

// getDuration converts interval to time.Duration
func (a *OrderBookAggregator) getDuration() time.Duration {
	duration, err := a.interval.Duration()
	if err != nil {
		// In case of error, return default duration
		return time.Minute
	}
	return duration
}

// aggregate performs the actual aggregation of snapshots
func (a *OrderBookAggregator) aggregate() OrderBookAggregation {
	if len(a.snapshots) == 0 {
		return OrderBookAggregation{}
	}

	first := a.snapshots[0]
	agg := OrderBookAggregation{
		Timestamp:   time.Now(),
		Exchange:    first.Exchange,
		TradingPair: first.TradingPair,
		Interval:    a.interval,
	}

	var spreads []float64
	var midPriceSum float64 // Добавляем переменную для суммирования цен

	for _, snap := range a.snapshots {
		midPrice := (snap.Bids[0][0] + snap.Asks[0][0]) / 2
		midPriceSum += midPrice // Накапливаем сумму mid price
		spread := snap.Asks[0][0] - snap.Bids[0][0]

		// Update spread metrics
		spreads = append(spreads, spread)
		agg.AverageSpread += spread

		if agg.MinSpread == 0 || spread < agg.MinSpread {
			agg.MinSpread = spread
		}
		if spread > agg.MaxSpread {
			agg.MaxSpread = spread
		}

		// Calculate depth metrics
		bidDepth, askDepth := a.calculateDepth(snap, midPrice)
		agg.BidDepth += bidDepth
		agg.AskDepth += askDepth

		// Calculate pressure metrics
		buyPressure, sellPressure := a.calculatePressure(snap)
		agg.BuyPressure += buyPressure
		agg.SellPressure += sellPressure
	}

	count := float64(len(a.snapshots))
	agg.SnapshotCount = len(a.snapshots)

	// Average out the metrics
	agg.MidPrice = midPriceSum / count // Вычисляем средний mid price
	agg.AverageSpread /= count
	agg.BidDepth /= count
	agg.AskDepth /= count
	agg.BuyPressure /= count
	agg.SellPressure /= count

	// Calculate depth imbalance
	totalDepth := agg.BidDepth + agg.AskDepth
	if totalDepth > 0 {
		agg.DepthImbalance = (agg.BidDepth - agg.AskDepth) / totalDepth
	}

	// Calculate spread volatility
	agg.SpreadVolatility = calculateVolatility(spreads)

	return agg
}

// calculateDepth calculates bid and ask depth within range
func (a *OrderBookAggregator) calculateDepth(snap *OrderBookSnapshot, midPrice float64) (float64, float64) {
	threshold := midPrice * a.depthRange

	var bidDepth, askDepth float64

	// Calculate bid depth
	for _, bid := range snap.Bids {
		if midPrice-bid[0] > threshold {
			break
		}
		bidDepth += bid[1]
	}

	// Calculate ask depth
	for _, ask := range snap.Asks {
		if ask[0]-midPrice > threshold {
			break
		}
		askDepth += ask[1]
	}

	return bidDepth, askDepth
}

// calculatePressure estimates buying and selling pressure
func (a *OrderBookAggregator) calculatePressure(snap *OrderBookSnapshot) (float64, float64) {
	var buyVolume, sellVolume float64

	for i := 0; i < min(len(snap.Bids), 10); i++ {
		buyVolume += snap.Bids[i][1]
	}

	for i := 0; i < min(len(snap.Asks), 10); i++ {
		sellVolume += snap.Asks[i][1]
	}

	totalVolume := buyVolume + sellVolume
	if totalVolume == 0 {
		return 0, 0
	}

	return buyVolume / totalVolume, sellVolume / totalVolume
}

// Helper function to calculate volatility
func calculateVolatility(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}

	// Calculate mean
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))

	// Calculate variance
	var varianceSum float64
	for _, v := range values {
		diff := v - mean
		varianceSum += diff * diff
	}
	variance := varianceSum / float64(len(values)-1)

	// Return standard deviation
	return math.Sqrt(variance)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
