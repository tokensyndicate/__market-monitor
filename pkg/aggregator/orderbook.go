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
	// Проверка на пустые массивы
	if len(snapshot.Bids) == 0 || len(snapshot.Asks) == 0 {
		return nil // Игнорируем снимки с пустыми данными
	}

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
		// В случае ошибки, возвращаем дефолтную продолжительность
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
		MinSpread:   math.MaxFloat64, // Инициализируем минимальный спред максимальным значением
	}

	var spreads []float64
	var midPriceSum float64

	for _, snap := range a.snapshots {
		// Проверка на пустые массивы
		if len(snap.Bids) == 0 || len(snap.Asks) == 0 {
			continue
		}

		// Вычисляем более стабильную среднюю цену на основе нескольких уровней
		var weightedBidPrice, weightedAskPrice, bidWeight, askWeight float64

		// Берем до 5 уровней для более стабильного расчета
		bidLevelsToUse := min(len(snap.Bids), 5)
		askLevelsToUse := min(len(snap.Asks), 5)

		for i := 0; i < bidLevelsToUse; i++ {
			// Используем объем как вес
			weight := snap.Bids[i][1]
			weightedBidPrice += snap.Bids[i][0] * weight
			bidWeight += weight
		}

		for i := 0; i < askLevelsToUse; i++ {
			// Используем объем как вес
			weight := snap.Asks[i][1]
			weightedAskPrice += snap.Asks[i][0] * weight
			askWeight += weight
		}

		// Вычисляем средневзвешенные цены
		var avgBidPrice, avgAskPrice float64
		if bidWeight > 0 {
			avgBidPrice = weightedBidPrice / bidWeight
		} else {
			avgBidPrice = snap.Bids[0][0]
		}

		if askWeight > 0 {
			avgAskPrice = weightedAskPrice / askWeight
		} else {
			avgAskPrice = snap.Asks[0][0]
		}

		// Итоговая средняя цена
		midPrice := (avgBidPrice + avgAskPrice) / 2
		midPriceSum += midPrice
		spread := snap.Asks[0][0] - snap.Bids[0][0]

		// Обновляем метрики спреда
		spreads = append(spreads, spread)
		agg.AverageSpread += spread

		if spread < agg.MinSpread {
			agg.MinSpread = spread
		}
		if spread > agg.MaxSpread {
			agg.MaxSpread = spread
		}

		// Вычисляем метрики глубины
		bidDepth, askDepth := a.calculateDepth(snap, midPrice)
		agg.BidDepth += bidDepth
		agg.AskDepth += askDepth

		// Вычисляем метрики давления
		buyPressure, sellPressure := a.calculatePressure(snap)
		agg.BuyPressure += buyPressure
		agg.SellPressure += sellPressure
	}

	// Если после фильтрации нет снимков с данными
	validCount := len(spreads)
	if validCount == 0 {
		return OrderBookAggregation{}
	}

	count := float64(validCount)
	agg.SnapshotCount = validCount

	// Усредняем метрики
	agg.MidPrice = midPriceSum / count
	agg.AverageSpread /= count
	agg.BidDepth /= count
	agg.AskDepth /= count
	agg.BuyPressure /= count
	agg.SellPressure /= count

	// Вычисляем дисбаланс глубины
	totalDepth := agg.BidDepth + agg.AskDepth
	if totalDepth > 0 {
		agg.DepthImbalance = (agg.BidDepth - agg.AskDepth) / totalDepth
	}

	// Вычисляем волатильность спреда
	agg.SpreadVolatility = calculateVolatility(spreads)

	return agg
}

// calculateDepth calculates bid and ask depth within range
func (a *OrderBookAggregator) calculateDepth(snap *OrderBookSnapshot, midPrice float64) (float64, float64) {
	// Правильный расчет порога как процента от средней цены
	lowerThreshold := midPrice * (1 - a.depthRange)
	upperThreshold := midPrice * (1 + a.depthRange)

	var bidDepth, askDepth float64

	// Вычисляем глубину bids (проверяем, что цена выше нижнего порога)
	for _, bid := range snap.Bids {
		if bid[0] < lowerThreshold {
			break
		}
		bidDepth += bid[1]
	}

	// Вычисляем глубину asks (проверяем, что цена ниже верхнего порога)
	for _, ask := range snap.Asks {
		if ask[0] > upperThreshold {
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
