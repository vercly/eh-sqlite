package backoff

import (
	"math"
	"strconv"
	"strings"
	"time"
)

var DefaultRetryDelays = []time.Duration{
	5 * time.Minute,
	10 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
	2 * time.Hour,
	3 * time.Hour,
	6 * time.Hour,
	12 * time.Hour,
}

type Config struct {
	MaxRetries int
	DelayFunc  func(retryCount int64) time.Duration
}

func DefaultConfig() Config {
	return ExplicitConfig(DefaultRetryDelays...)
}

func FixedConfig(maxRetries int, delay time.Duration) Config {
	return Config{
		MaxRetries: maxRetries,
		DelayFunc: func(int64) time.Duration {
			return delay
		},
	}
}

func ProgressiveConfig(maxRetries int, baseDelay time.Duration) Config {
	return Config{
		MaxRetries: maxRetries,
		DelayFunc: func(retryCount int64) time.Duration {
			return time.Duration(retryCount) * baseDelay
		},
	}
}

func ExponentialConfig(maxRetries int, baseDelay time.Duration, multiplier float64) Config {
	return Config{
		MaxRetries: maxRetries,
		DelayFunc: func(retryCount int64) time.Duration {
			return time.Duration(float64(baseDelay) * math.Pow(multiplier, float64(retryCount-1)))
		},
	}
}

func ExplicitConfig(delays ...time.Duration) Config {
	return Config{
		MaxRetries: len(delays),
		DelayFunc: func(retryCount int64) time.Duration {
			if len(delays) == 0 {
				return 0
			}
			idx := int(retryCount) - 1
			if idx < 0 {
				idx = 0
			}
			if idx >= len(delays) {
				idx = len(delays) - 1
			}
			return delays[idx]
		},
	}
}

// ParseConfig accepts the same retry curve syntax used by vercly's
// message.ParseRetryConfig: STD, EXP, PROG, FIXED, or comma-separated delays.
func ParseConfig(pattern string) Config {
	original := strings.TrimSpace(pattern)
	upper := strings.TrimSpace(strings.ToUpper(pattern))

	if upper == "" || upper == "STD" {
		return DefaultConfig()
	}

	parts := strings.Split(upper, ":")
	originalParts := strings.Split(original, ":")
	if len(parts) > 1 {
		switch parts[0] {
		case "EXP":
			multiplier := 1.5
			maxRetries := 5
			baseDelay := time.Hour
			if len(parts) >= 2 {
				if parsed, err := strconv.ParseFloat(parts[1], 64); err == nil {
					multiplier = parsed
				}
			}
			if len(parts) >= 3 {
				if parsed, err := strconv.Atoi(parts[2]); err == nil {
					maxRetries = parsed
				}
			}
			if len(parts) >= 4 {
				if parsed, err := time.ParseDuration(originalParts[3]); err == nil {
					baseDelay = parsed
				}
			}
			return ExponentialConfig(maxRetries, baseDelay, multiplier)
		case "PROG":
			maxRetries := 5
			baseDelay := time.Hour
			if len(parts) >= 2 {
				if parsed, err := strconv.Atoi(parts[1]); err == nil {
					maxRetries = parsed
				}
			}
			if len(parts) >= 3 {
				if parsed, err := time.ParseDuration(originalParts[2]); err == nil {
					baseDelay = parsed
				}
			}
			return ProgressiveConfig(maxRetries, baseDelay)
		case "FIXED":
			maxRetries := 5
			baseDelay := time.Hour
			if len(parts) >= 2 {
				if parsed, err := strconv.Atoi(parts[1]); err == nil {
					maxRetries = parsed
				}
			}
			if len(parts) >= 3 {
				if parsed, err := time.ParseDuration(originalParts[2]); err == nil {
					baseDelay = parsed
				}
			}
			return FixedConfig(maxRetries, baseDelay)
		}
	}

	delayParts := strings.Split(original, ",")
	delays := make([]time.Duration, 0, len(delayParts))
	for _, part := range delayParts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if delay, err := time.ParseDuration(part); err == nil {
			delays = append(delays, delay)
		}
	}
	if len(delays) > 0 {
		return ExplicitConfig(delays...)
	}

	return DefaultConfig()
}
