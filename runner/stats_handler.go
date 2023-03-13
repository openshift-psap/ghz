package runner

import (
	"context"
	"encoding/base64"
	"sync"

	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// StatsHandler is for gRPC stats
type statsHandler struct {
	results chan *callResult

	id     int
	hasLog bool
	log    Logger

	lock   sync.RWMutex
	ignore bool
}

type recorderContextKey struct{}

type recorder struct {
	payload []byte
}

// HandleConn handle the connection
func (c *statsHandler) HandleConn(ctx context.Context, cs stats.ConnStats) {
	// no-op
}

// TagConn exists to satisfy gRPC stats.Handler.
func (c *statsHandler) TagConn(ctx context.Context, cti *stats.ConnTagInfo) context.Context {
	// no-op
	return ctx
}

// HandleRPC implements per-RPC tracing and stats instrumentation.
func (c *statsHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	switch rs := rs.(type) {
	case *stats.InPayload:
		r, _ := ctx.Value(recorderContextKey{}).(*recorder)
		r.payload = rs.Data

		if c.hasLog {
			data_bytes_b64 := base64.StdEncoding.EncodeToString(rs.Data)
			c.log.Debugw("Handling RPC Stat", "payload_b64", data_bytes_b64)
		}
	case *stats.End:
		ign := false
		c.lock.RLock()
		ign = c.ignore
		c.lock.RUnlock()

		if !ign {
			duration := rs.EndTime.Sub(rs.BeginTime)

			var st string
			s, ok := status.FromError(rs.Error)
			if ok {
				st = s.Code().String()
			}

			r, _ := ctx.Value(recorderContextKey{}).(*recorder)
			workerID := ctx.Value("ghzWorker").(string)

			c.results <- &callResult{rs.Error, st, duration, rs.EndTime, workerID, r.payload}

			if c.hasLog {
				c.log.Debugw("Received RPC Stats",
					"statsID", c.id, "code", st, "error", rs.Error,
					"duration", duration, "stats", rs, "worker", workerID, "payload", r.payload)
			}
		}
	}
}

func (c *statsHandler) Ignore(val bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.ignore = val
}

// TagRPC implements per-RPC context management.
func (c *statsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, recorderContextKey{}, &recorder{})
}
