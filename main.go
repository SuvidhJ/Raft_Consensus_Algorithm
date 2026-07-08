package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/kv"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
)

func main() {
	portStr := os.Getenv("PORT")
	if portStr == "" {
		portStr = "50051"
	}
	portNum, err := strconv.Atoi(portStr)
	if err != nil || portNum < 1 || portNum > 65535 {
		log.Fatalf("invalid PORT value %q; expected 1-65535", portStr)
	}

	idStr := os.Getenv("NODE_ID")
	id := 1
	if strings.TrimSpace(idStr) != "" {
		parsedID, err := strconv.Atoi(idStr)
		if err != nil || parsedID <= 0 {
			log.Fatalf("invalid NODE_ID value %q; expected positive integer", idStr)
		}
		id = parsedID
	}

	peers := parsePeers(os.Getenv("PEERS"))

	log.Printf("Starting Raft KV Store Node %d on port %s (peers=%d)", id, portStr, len(peers))

	// Channel to receive committed logs from Raft
	applyCh := make(chan *proto.LogEntry, 100)

	// Initialize Raft node
	raftNode := raft.NewRaftNode(uint32(id), peers, applyCh)

	// Initialize KV Store state machine
	store := kv.NewKVStore(raftNode)
	raftNode.SetSnapshotter(store)

	// Background worker to apply logs
	stopApply := make(chan struct{})
	var applyWG sync.WaitGroup
	applyWG.Add(1)
	go func() {
		defer applyWG.Done()
		for {
			select {
			case <-stopApply:
				return
			case entry := <-applyCh:
				if entry != nil {
					store.ApplyCommand(entry)
				}
			}
		}
	}()

	// Observability endpoint (Prometheus /metrics, JSON /status, /healthz).
	// Defaults to gRPC port + 1000; set METRICS_PORT=0 to disable.
	stopMetrics := func() {}
	metricsPort := portNum + 1000
	if raw := strings.TrimSpace(os.Getenv("METRICS_PORT")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 || v > 65535 {
			log.Fatalf("invalid METRICS_PORT value %q", raw)
		}
		metricsPort = v
	}
	if metricsPort != 0 {
		stop, err := startObservabilityServer(fmt.Sprintf(":%d", metricsPort), raftNode, store)
		if err != nil {
			log.Fatalf("failed to start metrics server on :%d: %v", metricsPort, err)
		}
		stopMetrics = stop
		log.Printf("metrics/status available on :%d", metricsPort)
	}

	// Start gRPC Server
	lis, err := net.Listen("tcp", ":"+portStr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	proto.RegisterKVServiceServer(grpcServer, store)
	proto.RegisterRaftServiceServer(grpcServer, raft.NewService(raftNode))

	var shutdownOnce sync.Once
	shutdown := func(reason string) {
		shutdownOnce.Do(func() {
			log.Printf("shutting down (%s)", reason)

			done := make(chan struct{})
			go func() {
				grpcServer.GracefulStop()
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				grpcServer.Stop()
			}

			raftNode.Close()
			close(stopApply)
			applyWG.Wait()
			stopMetrics()
			_ = lis.Close()
		})
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		sig := <-sigCh
		shutdown("signal: " + sig.String())
	}()

	if err := grpcServer.Serve(lis); err != nil {
		if !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("server exited with error: %v", err)
		}
	}

	shutdown("server-exit")
}

func parsePeers(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return []string{}
	}
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
