// bench is a load generator that measures throughput and latency
// percentiles against a running cluster.
//
// Example:
//
//	go run ./cmd/bench -addrs 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053 \
//	    -concurrency 8 -duration 30s -read-ratio 0.5 -linearizable
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type result struct {
	latency time.Duration
	op      string
	ok      bool
}

func main() {
	addrsRaw := flag.String("addrs", "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053", "comma-separated kv node addresses")
	concurrency := flag.Int("concurrency", 8, "concurrent workers")
	duration := flag.Duration("duration", 30*time.Second, "benchmark duration")
	readRatio := flag.Float64("read-ratio", 0.5, "fraction of operations that are reads")
	linearizable := flag.Bool("linearizable", false, "use linearizable reads instead of local reads")
	keySpace := flag.Int("key-space", 64, "number of distinct keys")
	valueSize := flag.Int("value-size", 64, "value payload size in bytes")
	timeout := flag.Duration("timeout", 2*time.Second, "per-operation timeout")
	flag.Parse()

	addrs := splitAddrs(*addrsRaw)
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "no addresses provided")
		os.Exit(1)
	}

	clients := make([]proto.KVServiceClient, 0, len(addrs))
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "dial %s failed: %v\n", addr, err)
			continue
		}
		defer conn.Close()
		clients = append(clients, proto.NewKVServiceClient(conn))
	}
	if len(clients) == 0 {
		fmt.Fprintln(os.Stderr, "no reachable nodes")
		os.Exit(1)
	}

	value := strings.Repeat("x", *valueSize)
	var leaderIdx atomic.Int64 // shared leader hint

	resultCh := make(chan result, 4096)
	var collectorWG sync.WaitGroup
	collectorWG.Add(1)
	results := make([]result, 0, 1<<16)
	go func() {
		defer collectorWG.Done()
		for r := range resultCh {
			results = append(results, r)
		}
	}()

	stop := make(chan struct{})
	var workerWG sync.WaitGroup
	start := time.Now()
	for w := 0; w < *concurrency; w++ {
		w := w
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(w)*1e9))
			clientID := rng.Uint64() | 1
			seq := uint64(0)
			for {
				select {
				case <-stop:
					return
				default:
				}

				key := fmt.Sprintf("bench-%d", rng.Intn(*keySpace))
				isRead := rng.Float64() < *readRatio

				opStart := time.Now()
				var ok bool
				var op string
				if isRead {
					target := clients[rng.Intn(len(clients))]
					ctx, cancel := context.WithTimeout(context.Background(), *timeout)
					if *linearizable {
						op = "lin-get"
						// Linearizable reads only succeed on the leader.
						h := leaderIdx.Load()
						target = clients[h%int64(len(clients))]
						resp, err := target.GetLinearizable(ctx, &proto.GetRequest{Key: key})
						ok = err == nil && resp != nil && (resp.Success || resp.ErrorMessage == "Key not found")
						if !ok {
							// Only the first worker to see this hint fail
							// advances it; blind increments from many workers
							// scatter everyone across followers.
							leaderIdx.CompareAndSwap(h, h+1)
						}
					} else {
						op = "get"
						resp, err := target.Get(ctx, &proto.GetRequest{Key: key})
						ok = err == nil && resp != nil && (resp.Success || resp.ErrorMessage == "Key not found")
					}
					cancel()
				} else {
					op = "set"
					seq++
					h := leaderIdx.Load()
					target := clients[h%int64(len(clients))]
					ctx, cancel := context.WithTimeout(context.Background(), *timeout)
					resp, err := target.Set(ctx, &proto.SetRequest{
						Key: key, Value: value, ClientId: clientID, Sequence: seq,
					})
					cancel()
					ok = err == nil && resp != nil && resp.Success
					if !ok {
						// Rotate the leader hint (once per failed hint) and
						// let the retry happen as a new measured operation.
						leaderIdx.CompareAndSwap(h, h+1)
					}
				}

				resultCh <- result{latency: time.Since(opStart), op: op, ok: ok}
			}
		}()
	}

	time.Sleep(*duration)
	close(stop)
	workerWG.Wait()
	close(resultCh)
	collectorWG.Wait()
	elapsed := time.Since(start)

	byOp := map[string][]time.Duration{}
	failures := map[string]int{}
	for _, r := range results {
		if r.ok {
			byOp[r.op] = append(byOp[r.op], r.latency)
		} else {
			failures[r.op]++
		}
	}

	fmt.Printf("\n=== bench results (%s elapsed, %d workers) ===\n", elapsed.Round(time.Millisecond), *concurrency)
	ops := make([]string, 0, len(byOp))
	for op := range byOp {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	for _, op := range ops {
		lats := byOp[op]
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		n := len(lats)
		pct := func(p float64) time.Duration {
			idx := int(p * float64(n-1))
			return lats[idx]
		}
		fmt.Printf("%-8s ops=%-7d throughput=%8.1f/s  p50=%-9s p90=%-9s p95=%-9s p99=%-9s max=%-9s fail=%d\n",
			op, n, float64(n)/elapsed.Seconds(),
			pct(0.50).Round(time.Microsecond), pct(0.90).Round(time.Microsecond),
			pct(0.95).Round(time.Microsecond), pct(0.99).Round(time.Microsecond),
			lats[n-1].Round(time.Microsecond), failures[op])
	}
	for op, n := range failures {
		if len(byOp[op]) == 0 {
			fmt.Printf("%-8s all %d operations failed\n", op, n)
		}
	}
}

func splitAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
