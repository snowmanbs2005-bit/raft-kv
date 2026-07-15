package raft

import (
	"math/rand/v2"
	"time"
)

// randomTimeout returns a random duration in [min, max). Each node picks its
// own value on every election round, which is what makes split votes rare:
// with enough spread between min and max, the probability that two nodes
// pick timeouts within a heartbeat interval of each other is low.
func randomTimeout(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	span := int64(max - min)
	return min + time.Duration(rand.Int64N(span))
}
