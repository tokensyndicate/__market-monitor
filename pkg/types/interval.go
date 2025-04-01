package types

import (
	"fmt"
	"time"
)

type Interval string

const (
	Interval1m  Interval = "1m"
	Interval5m  Interval = "5m"
	Interval15m Interval = "15m"
	Interval1h  Interval = "1h"
	Interval4h  Interval = "4h"
	Interval1d  Interval = "1d"
)

// ParseDuration converts interval to time.Duration
func (i Interval) Duration() (time.Duration, error) {
	switch i {
	case Interval1m:
		return time.Minute, nil
	case Interval5m:
		return 5 * time.Minute, nil
	case Interval15m:
		return 15 * time.Minute, nil
	case Interval1h:
		return time.Hour, nil
	case Interval4h:
		return 4 * time.Hour, nil
	case Interval1d:
		return 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported interval: %s", i)
	}
}

// DefaultIntervals returns default intervals for monitoring
func DefaultIntervals() []Interval {
	return []Interval{
		Interval1m,
		Interval5m,
		Interval1h,
	}
}

func (i Interval) String() string {
	return string(i)
}
