package durable

import "sync/atomic"

// Gate is a one-way latch used to freeze the durable queued-write channel.
// Once tripped it stays tripped for the lifetime of the process; recovery
// requires operator intervention and a restart.
type Gate struct {
	fatal  atomic.Bool
	reason atomic.Value // string
}

// Trip freezes the gate with the given reason. It reports whether this call
// was the first trip. The first non-empty reason wins; later calls are
// ignored so the original cause is preserved.
func (g *Gate) Trip(reason string) bool {
	if g.fatal.CompareAndSwap(false, true) {
		g.reason.Store(reason)
		return true
	}
	return false
}

// Fatal reports whether the gate is tripped and, if so, why.
func (g *Gate) Fatal() (bool, string) {
	if !g.fatal.Load() {
		return false, ""
	}
	if v, ok := g.reason.Load().(string); ok {
		return true, v
	}
	return true, "unknown"
}
