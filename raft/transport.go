package raft

import (
	"context"
	"sync"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Transport abstracts peer-to-peer RPC delivery so the consensus core is
// independent of the wire protocol. Production uses gRPC; tests plug in an
// in-memory network with fault injection (partitions, drops, delays).
type Transport interface {
	PreVote(ctx context.Context, target string, req *proto.PreVoteRequest) (*proto.PreVoteResponse, error)
	RequestVote(ctx context.Context, target string, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, target string, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, target string, req *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error)
	TimeoutNow(ctx context.Context, target string, req *proto.TimeoutNowRequest) (*proto.TimeoutNowResponse, error)
	Close()
}

// grpcTransport maintains a lazily-built pool of gRPC client connections.
type grpcTransport struct {
	mu      sync.Mutex
	clients map[string]proto.RaftServiceClient
	conns   map[string]*grpc.ClientConn
	closed  bool
}

// NewGRPCTransport returns the production Transport backed by gRPC.
func NewGRPCTransport() Transport {
	return &grpcTransport{
		clients: make(map[string]proto.RaftServiceClient),
		conns:   make(map[string]*grpc.ClientConn),
	}
}

func (t *grpcTransport) client(ctx context.Context, addr string) (proto.RaftServiceClient, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, context.Canceled
	}
	if c, ok := t.clients[addr]; ok {
		t.mu.Unlock()
		return c, nil
	}
	t.mu.Unlock()

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	client := proto.NewRaftServiceClient(conn)
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = conn.Close()
		return nil, context.Canceled
	}
	if existing, ok := t.clients[addr]; ok {
		t.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	t.clients[addr] = client
	t.conns[addr] = conn
	t.mu.Unlock()
	return client, nil
}

func (t *grpcTransport) PreVote(ctx context.Context, target string, req *proto.PreVoteRequest) (*proto.PreVoteResponse, error) {
	c, err := t.client(ctx, target)
	if err != nil {
		return nil, err
	}
	return c.PreVote(ctx, req)
}

func (t *grpcTransport) RequestVote(ctx context.Context, target string, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	c, err := t.client(ctx, target)
	if err != nil {
		return nil, err
	}
	return c.RequestVote(ctx, req)
}

func (t *grpcTransport) AppendEntries(ctx context.Context, target string, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	c, err := t.client(ctx, target)
	if err != nil {
		return nil, err
	}
	return c.AppendEntries(ctx, req)
}

func (t *grpcTransport) InstallSnapshot(ctx context.Context, target string, req *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error) {
	c, err := t.client(ctx, target)
	if err != nil {
		return nil, err
	}
	return c.InstallSnapshot(ctx, req)
}

func (t *grpcTransport) TimeoutNow(ctx context.Context, target string, req *proto.TimeoutNowRequest) (*proto.TimeoutNowResponse, error) {
	c, err := t.client(ctx, target)
	if err != nil {
		return nil, err
	}
	return c.TimeoutNow(ctx, req)
}

func (t *grpcTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	for addr, conn := range t.conns {
		_ = conn.Close()
		delete(t.conns, addr)
		delete(t.clients, addr)
	}
}
