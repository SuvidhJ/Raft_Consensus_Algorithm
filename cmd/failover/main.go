// failover measures how the cluster rides out leader crashes. It launches a
// real multi-process cluster (one OS process per node, gRPC over loopback,
// fsynced WALs), drives a continuous sessioned write workload, hard-kills the
// leader process, and records per trial:
//
//   - election time: kill -> a survivor reports itself leader of a newer term
//   - write unavailability: kill -> first write acknowledged by a survivor
//   - catch-up time: restart of the killed node -> its applied index reaches
//     the leader's commit index (process boot + WAL replay + log catch-up)
//
// After every failover the workload is paused and every key is checked
// against its last acknowledged value on every replica (local reads) and
// through linearizable reads on the leader, so a lost or phantom
// acknowledged write fails the run.
//
// Example:
//
//	go build -o raftkv.exe . && go run ./cmd/failover -bin ./raftkv.exe -trials 50
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

type node struct {
	idx    int
	cmd    *exec.Cmd
	exited chan struct{}
	conn   *grpc.ClientConn
	kv     proto.KVServiceClient
	raft   proto.RaftServiceClient
}

type cluster struct {
	bin     string
	dataDir string
	addrs   []string
	nodes   []atomic.Pointer[node]
}

// Environment variables the harness sets itself; inherited copies are
// dropped so a stray shell setting cannot change the configuration.
var ownedEnv = []string{"PORT=", "NODE_ID=", "PEERS=", "RAFT_", "METRICS_PORT="}

func (c *cluster) start(i int) error {
	peers := make([]string, 0, len(c.addrs)-1)
	for j, a := range c.addrs {
		if j != i {
			peers = append(peers, a)
		}
	}
	env := make([]string, 0, len(os.Environ())+6)
	for _, kv := range os.Environ() {
		owned := false
		for _, p := range ownedEnv {
			if strings.HasPrefix(strings.ToUpper(kv), p) {
				owned = true
				break
			}
		}
		if !owned {
			env = append(env, kv)
		}
	}
	env = append(env,
		"PORT="+strings.Split(c.addrs[i], ":")[1],
		"NODE_ID="+strconv.Itoa(i+1),
		"PEERS="+strings.Join(peers, ","),
		"RAFT_ADVERTISE_ADDR="+c.addrs[i],
		"RAFT_DATA_DIR="+c.dataDir,
		"METRICS_PORT=0",
	)

	logf, err := os.OpenFile(filepath.Join(c.dataDir, fmt.Sprintf("node-%d.log", i+1)),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(c.bin)
	cmd.Env = env
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return err
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		logf.Close()
		close(exited)
	}()

	// Reconnect quickly so client-side backoff does not inflate the
	// measured recovery of a restarted node.
	conn, err := grpc.Dial(c.addrs[i],
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 10 * time.Millisecond, Multiplier: 1.5, Jitter: 0.2, MaxDelay: 50 * time.Millisecond},
			MinConnectTimeout: time.Second,
		}))
	if err != nil {
		_ = cmd.Process.Kill()
		<-exited
		return err
	}
	c.nodes[i].Store(&node{
		idx: i, cmd: cmd, exited: exited, conn: conn,
		kv: proto.NewKVServiceClient(conn), raft: proto.NewRaftServiceClient(conn),
	})
	return nil
}

// kill terminates the node process without any graceful shutdown
// (TerminateProcess on Windows, SIGKILL elsewhere) and waits for it to exit.
func (c *cluster) kill(i int) {
	n := c.nodes[i].Load()
	_ = n.cmd.Process.Kill()
	<-n.exited
	_ = n.conn.Close()
}

func (c *cluster) status(i int, timeout time.Duration) (*proto.ClusterStatusResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.nodes[i].Load().raft.ClusterStatus(ctx, &proto.ClusterStatusRequest{})
}

// stableLeader returns the leader index once every listed node agrees on the
// same leader and term.
func (c *cluster) stableLeader(live []int, timeout time.Duration) (int, *proto.ClusterStatusResponse, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		sts := make([]*proto.ClusterStatusResponse, len(live))
		ok := true
		for k, i := range live {
			st, err := c.status(i, 200*time.Millisecond)
			if err != nil {
				ok = false
				break
			}
			sts[k] = st
		}
		if ok {
			leader := -1
			for k, st := range sts {
				if st.State == "leader" {
					leader = k
				}
			}
			if leader >= 0 {
				agree := true
				for _, st := range sts {
					if st.LeaderId != sts[leader].NodeId || st.Term != sts[leader].Term {
						agree = false
					}
				}
				if agree {
					return live[leader], sts[leader], nil
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return -1, nil, fmt.Errorf("no stable leader among %v within %s", live, timeout)
}

func (c *cluster) waitApplied(i int, index uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if st, err := c.status(i, 200*time.Millisecond); err == nil && st.AppliedIndex >= index {
			return nil
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fmt.Errorf("node %d did not apply index %d within %s", i+1, index, timeout)
}

type writer struct {
	id       int
	clientID uint64
	seq      uint64
	last     []uint64 // per slot: last acknowledged sequence (0 = never written)
}

// workload runs sessioned writers. Each writer has one operation in flight
// and retries it (same session sequence, so exactly-once) until it is
// acknowledged. Holding gate exclusively pauses the workload with no
// operation in flight, which makes every key's expected value exact.
type workload struct {
	c       *cluster
	slots   int
	gate    sync.RWMutex
	hint    atomic.Int64
	stop    atomic.Bool
	writers []*writer

	mu        sync.Mutex
	acked     uint64
	killedIdx int
	killAt    time.Time
	firstAck  time.Time
	sinceKill []int
}

func (w *workload) key(wr *writer, slot int) string { return fmt.Sprintf("w%d-%d", wr.id, slot) }

func (w *workload) run(wr *writer, wg *sync.WaitGroup) {
	defer wg.Done()
	n := int64(len(w.c.addrs))
	for {
		w.gate.RLock()
		if w.stop.Load() {
			w.gate.RUnlock()
			return
		}
		wr.seq++
		slot := int(wr.seq % uint64(w.slots))
		req := &proto.SetRequest{Key: w.key(wr, slot), Value: strconv.FormatUint(wr.seq, 10),
			ClientId: wr.clientID, Sequence: wr.seq}
		opStart := time.Now()
		for {
			h := w.hint.Load()
			idx := int(h % n)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			resp, err := w.c.nodes[idx].Load().kv.Set(ctx, req)
			cancel()
			if err == nil && resp.Success {
				w.onAck(wr, idx, time.Now())
				wr.last[slot] = wr.seq
				break
			}
			if time.Since(opStart) > 30*time.Second {
				must(fmt.Errorf("write %s unacknowledged for 30s (cluster unavailable)", req.Key))
			}
			w.hint.CompareAndSwap(h, h+1)
			time.Sleep(time.Millisecond)
		}
		w.gate.RUnlock()
	}
}

func (w *workload) onAck(wr *writer, idx int, at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.acked++
	if w.killedIdx < 0 || !at.After(w.killAt) {
		return
	}
	w.sinceKill[wr.id]++
	// A reply the killed leader wrote just before dying can still arrive
	// after killAt; only a survivor's ack proves the new leader serves.
	if idx != w.killedIdx && w.firstAck.IsZero() {
		w.firstAck = at
	}
}

// verify checks every key on node i. Local reads require the node to have
// applied everything committed (the workload is paused); linearizable reads
// go through the ReadIndex barrier and are only valid on the leader.
func (w *workload) verify(i int, linearizable bool) (checked, bad int) {
	type job struct {
		wr   *writer
		slot int
	}
	jobs := make(chan job)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kvc := w.c.nodes[i].Load().kv
			for j := range jobs {
				key := w.key(j.wr, j.slot)
				want := ""
				if s := j.wr.last[j.slot]; s != 0 {
					want = strconv.FormatUint(s, 10)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				var resp *proto.GetResponse
				var err error
				if linearizable {
					resp, err = kvc.GetLinearizable(ctx, &proto.GetRequest{Key: key})
				} else {
					resp, err = kvc.Get(ctx, &proto.GetRequest{Key: key})
				}
				cancel()
				got, ok := "", false
				if err == nil && resp.Success {
					got, ok = resp.Value, true
				} else if err == nil && resp.ErrorMessage == "Key not found" {
					got, ok = "", true
				}
				mu.Lock()
				checked++
				if !ok || got != want {
					bad++
					if bad <= 5 {
						fmt.Fprintf(os.Stderr, "MISMATCH node %d key %s: want %q got %q (err=%v)\n", i+1, key, want, got, err)
					}
				}
				mu.Unlock()
			}
		}()
	}
	for _, wr := range w.writers {
		for s := 0; s < w.slots; s++ {
			jobs <- job{wr, s}
		}
	}
	close(jobs)
	wg.Wait()
	return checked, bad
}

type trial struct {
	n                int
	killed           int
	oldTerm, newTerm uint64
	electionMs       float64
	unavailMs        float64
	bootMs           float64 // restart -> node answers RPCs (includes WAL recovery)
	catchupMs        float64 // restart -> applied index reaches leader commit
	entriesBehind    uint64
	maxSinceKill     int
	keysVerified     int
	mismatches       int
	churn            uint64 // extra terms after the first post-crash leader, before the restart
	rejoinDisrupted  bool   // leader or term changed while the crashed node rejoined
}

func main() {
	bin := flag.String("bin", "", "path to the built raft-kv server binary (required)")
	nodes := flag.Int("nodes", 3, "cluster size")
	basePort := flag.Int("base-port", 52051, "first gRPC port")
	trials := flag.Int("trials", 30, "number of leader crashes")
	writers := flag.Int("writers", 4, "concurrent sessioned writers")
	slots := flag.Int("slots", 512, "keys per writer (overwritten round-robin)")
	warmup := flag.Duration("warmup", 1500*time.Millisecond, "steady-state load before each crash")
	settle := flag.Duration("settle", 300*time.Millisecond, "load after the first post-crash ack before pausing to verify")
	csvPath := flag.String("csv", "", "write per-trial rows to this CSV file")
	keep := flag.Bool("keep", false, "keep the data directory (WALs and node logs)")
	flag.Parse()

	if *bin == "" {
		fmt.Fprintln(os.Stderr, "-bin is required (go build -o raftkv.exe .)")
		os.Exit(1)
	}
	binAbs, err := filepath.Abs(*bin)
	must(err)
	dataDir, err := os.MkdirTemp("", "raft-failover-")
	must(err)

	c := &cluster{bin: binAbs, dataDir: dataDir, addrs: make([]string, *nodes), nodes: make([]atomic.Pointer[node], *nodes)}
	all := make([]int, *nodes)
	for i := range c.addrs {
		c.addrs[i] = fmt.Sprintf("127.0.0.1:%d", *basePort+i)
		all[i] = i
	}
	cleanup = sync.OnceFunc(func() {
		for i := range c.nodes {
			if c.nodes[i].Load() != nil {
				c.kill(i)
			}
		}
		if *keep {
			fmt.Printf("data kept in %s\n", dataDir)
		} else {
			_ = os.RemoveAll(dataDir)
		}
	})
	defer cleanup()
	for i := range c.addrs {
		must(c.start(i))
	}

	_, _, err = c.stableLeader(all, 15*time.Second)
	must(err)

	w := &workload{c: c, slots: *slots, killedIdx: -1, sinceKill: make([]int, *writers)}
	var wwg sync.WaitGroup
	for i := 0; i < *writers; i++ {
		wr := &writer{id: i, clientID: uint64(time.Now().UnixNano()) + uint64(i)*7919, last: make([]uint64, *slots)}
		w.writers = append(w.writers, wr)
		wwg.Add(1)
		go w.run(wr, &wwg)
	}

	fmt.Printf("failover: %d nodes, %d writers x %d keys, %d trials, GOOS=%s CPUs=%d clock-resolution=%s\n",
		*nodes, *writers, *slots, *trials, runtime.GOOS, runtime.NumCPU(), clockResolution())
	fmt.Printf("%-5s %-6s %-9s %-11s %-10s %-9s %-11s %-8s %-9s %s\n",
		"trial", "killed", "terms", "election", "unavail", "boot", "catch-up", "behind", "verified", "bad")

	start := time.Now()
	results := make([]trial, 0, *trials)
	for t := 1; t <= *trials; t++ {
		time.Sleep(*warmup)
		leader, lst, err := c.stableLeader(all, 10*time.Second)
		must(err)
		tr := trial{n: t, killed: leader, oldTerm: lst.Term}
		survivors := make([]int, 0, *nodes-1)
		for _, i := range all {
			if i != leader {
				survivors = append(survivors, i)
			}
		}

		killAt := time.Now()
		w.mu.Lock()
		w.killedIdx, w.killAt, w.firstAck = leader, killAt, time.Time{}
		for i := range w.sinceKill {
			w.sinceKill[i] = 0
		}
		w.mu.Unlock()
		c.kill(leader)

		// Election: a survivor reports leadership of a newer term.
		newLeader := -1
		for newLeader < 0 {
			if time.Since(killAt) > 15*time.Second {
				must(fmt.Errorf("trial %d: no new leader 15s after crash", t))
			}
			for _, s := range survivors {
				if st, err := c.status(s, 100*time.Millisecond); err == nil && st.State == "leader" && st.Term > tr.oldTerm {
					tr.electionMs = ms(time.Since(killAt))
					tr.newTerm = st.Term
					newLeader = s
					break
				}
			}
			if newLeader < 0 {
				time.Sleep(time.Millisecond)
			}
		}

		// Unavailability: first acknowledgement from a survivor.
		for {
			w.mu.Lock()
			fa := w.firstAck
			w.mu.Unlock()
			if !fa.IsZero() {
				tr.unavailMs = ms(fa.Sub(killAt))
				break
			}
			if time.Since(killAt) > 30*time.Second {
				must(fmt.Errorf("trial %d: no write acknowledged 30s after crash", t))
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(*settle)

		// Pause the workload (no op in flight) and verify survivors.
		w.gate.Lock()
		w.mu.Lock()
		for _, v := range w.sinceKill {
			tr.maxSinceKill = max(tr.maxSinceKill, v)
		}
		w.killedIdx = -1
		w.mu.Unlock()
		if tr.maxSinceKill >= *slots {
			fmt.Fprintf(os.Stderr, "WARNING trial %d: %d writes since crash >= %d slots; pre-crash acks may be overwritten before verification\n", t, tr.maxSinceKill, *slots)
		}

		// Leadership may have moved again since the first post-crash
		// leader (churn), so verify through whoever leads now.
		curLeader, lst2, err := c.stableLeader(survivors, 10*time.Second)
		must(err)
		commit := lst2.CommitIndex
		for _, s := range survivors {
			must(c.waitApplied(s, commit, 10*time.Second))
			n, bad := w.verify(s, false)
			tr.keysVerified += n
			tr.mismatches += bad
		}
		n, bad := w.verify(curLeader, true)
		tr.keysVerified += n
		tr.mismatches += bad

		// Restart the crashed node from its WAL and time its catch-up.
		curLeader, lst3, err := c.stableLeader(survivors, 10*time.Second)
		must(err)
		tr.churn = lst3.Term - tr.newTerm
		commit = lst3.CommitIndex
		tr.entriesBehind = commit - lst.CommitIndex
		restartAt := time.Now()
		must(c.start(leader))
		for {
			if _, err := c.status(leader, 100*time.Millisecond); err == nil {
				tr.bootMs = ms(time.Since(restartAt))
				break
			}
			if time.Since(restartAt) > 30*time.Second {
				must(fmt.Errorf("trial %d: restarted node never answered", t))
			}
			time.Sleep(time.Millisecond)
		}
		must(c.waitApplied(leader, commit, 60*time.Second))
		tr.catchupMs = ms(time.Since(restartAt))
		n, bad = w.verify(leader, false)
		tr.keysVerified += n
		tr.mismatches += bad

		// Pre-vote should keep the rejoining node from forcing an election.
		if l, st, err := c.stableLeader(all, 10*time.Second); err != nil || l != curLeader || st.Term != lst3.Term {
			tr.rejoinDisrupted = true
		}

		results = append(results, tr)
		note := ""
		if tr.churn > 0 {
			note += fmt.Sprintf("  (%d extra election(s) after failover)", tr.churn)
		}
		if tr.rejoinDisrupted {
			note += "  (leader changed on rejoin)"
		}
		fmt.Printf("%-5d n%-5d %3d->%-4d %8.1fms %8.1fms %7.1fms %8.1fms %-8d %-9d %d%s\n",
			t, tr.killed+1, tr.oldTerm, tr.newTerm, tr.electionMs, tr.unavailMs, tr.bootMs, tr.catchupMs,
			tr.entriesBehind, tr.keysVerified, tr.mismatches, note)
		w.gate.Unlock()
	}

	w.stop.Store(true)
	wwg.Wait()
	elapsed := time.Since(start)

	var verified, bad, disrupted, churned, maxSince int
	var churn uint64
	for _, r := range results {
		verified += r.keysVerified
		bad += r.mismatches
		maxSince = max(maxSince, r.maxSinceKill)
		churn += r.churn
		if r.churn > 0 {
			churned++
		}
		if r.rejoinDisrupted {
			disrupted++
		}
	}
	w.mu.Lock()
	acked := w.acked
	w.mu.Unlock()

	fmt.Printf("\n=== failover summary (%d leader crashes, %s) ===\n", len(results), elapsed.Round(time.Second))
	summarize("election (kill -> new leader)", results, func(r trial) float64 { return r.electionMs })
	summarize("write unavailability", results, func(r trial) float64 { return r.unavailMs })
	summarize("restart -> serving RPCs", results, func(r trial) float64 { return r.bootMs })
	summarize("restart -> fully caught up", results, func(r trial) float64 { return r.catchupMs })
	fmt.Printf("acknowledged writes:        %d (%.0f/s average incl. pauses)\n", acked, float64(acked)/elapsed.Seconds())
	fmt.Printf("key checks after crashes:   %d, mismatches (lost/phantom writes): %d\n", verified, bad)
	fmt.Printf("max writes/writer between crash and verification: %d of %d slots\n", maxSince, *slots)
	fmt.Printf("leader churn after failover: %d extra elections in %d of %d trials\n", churn, churned, len(results))
	fmt.Printf("leader changes on rejoin:   %d of %d\n", disrupted, len(results))

	if *csvPath != "" {
		must(writeCSV(*csvPath, results))
	}
	if bad > 0 {
		cleanup()
		os.Exit(3)
	}
}

// summarize prints nearest-rank percentiles.
func summarize(name string, rs []trial, f func(trial) float64) {
	v := make([]float64, len(rs))
	sum := 0.0
	for i, r := range rs {
		v[i] = f(r)
		sum += v[i]
	}
	sort.Float64s(v)
	pct := func(p float64) float64 {
		k := int(math.Ceil(p*float64(len(v)))) - 1
		return v[max(k, 0)]
	}
	fmt.Printf("%-28s min=%7.1f p50=%7.1f p90=%7.1f p99=%7.1f max=%7.1f mean=%7.1f ms\n",
		name, v[0], pct(0.50), pct(0.90), pct(0.99), v[len(v)-1], sum/float64(len(v)))
}

func writeCSV(path string, rs []trial) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	cw := csv.NewWriter(f)
	_ = cw.Write([]string{"trial", "killed_node", "old_term", "new_term", "election_ms", "unavail_ms",
		"boot_ms", "catchup_ms", "entries_behind", "max_writes_since_kill", "keys_verified", "mismatches",
		"churn_terms", "leader_changed_on_rejoin"})
	for _, r := range rs {
		_ = cw.Write([]string{
			strconv.Itoa(r.n), strconv.Itoa(r.killed + 1),
			strconv.FormatUint(r.oldTerm, 10), strconv.FormatUint(r.newTerm, 10),
			fmt.Sprintf("%.2f", r.electionMs), fmt.Sprintf("%.2f", r.unavailMs),
			fmt.Sprintf("%.2f", r.bootMs), fmt.Sprintf("%.2f", r.catchupMs),
			strconv.FormatUint(r.entriesBehind, 10), strconv.Itoa(r.maxSinceKill),
			strconv.Itoa(r.keysVerified), strconv.Itoa(r.mismatches),
			strconv.FormatUint(r.churn, 10), strconv.FormatBool(r.rejoinDisrupted),
		})
	}
	cw.Flush()
	return cw.Error()
}

// clockResolution reports the smallest observable step of time.Now, which
// bounds the precision of every duration this tool prints.
func clockResolution() time.Duration {
	best := time.Hour
	for i := 0; i < 200; i++ {
		t0 := time.Now()
		t1 := time.Now()
		for t1 == t0 {
			t1 = time.Now()
		}
		if d := t1.Sub(t0); d > 0 && d < best {
			best = d
		}
	}
	return best
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// cleanup kills every node process and removes the data directory; fatal
// paths call it explicitly because os.Exit skips deferred calls.
var cleanup = func() {}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "failover:", err)
		cleanup()
		os.Exit(1)
	}
}
