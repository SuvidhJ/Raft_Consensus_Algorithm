package raft

import (
	"context"
	"errors"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
)

type Service struct {
	proto.UnimplementedRaftServiceServer
	node *RaftNode
}

func NewService(node *RaftNode) *Service {
	return &Service{node: node}
}

func (s *Service) PreVote(_ context.Context, req *proto.PreVoteRequest) (*proto.PreVoteResponse, error) {
	return s.node.HandlePreVote(req), nil
}

func (s *Service) RequestVote(_ context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	return s.node.HandleRequestVote(req), nil
}

func (s *Service) AppendEntries(_ context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	return s.node.HandleAppendEntries(req), nil
}

func (s *Service) InstallSnapshot(_ context.Context, req *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error) {
	return s.node.HandleInstallSnapshot(req), nil
}

func (s *Service) TimeoutNow(_ context.Context, req *proto.TimeoutNowRequest) (*proto.TimeoutNowResponse, error) {
	return s.node.HandleTimeoutNow(req), nil
}

func (s *Service) AddServer(ctx context.Context, req *proto.AddServerRequest) (*proto.AddServerResponse, error) {
	if req == nil {
		return &proto.AddServerResponse{Success: false, ErrorMessage: "invalid request"}, nil
	}
	err := s.node.AddServer(ctx, req.Address)
	if err != nil {
		resp := &proto.AddServerResponse{Success: false, ErrorMessage: err.Error()}
		if errors.Is(err, ErrNotLeader) {
			resp.LeaderId = s.node.LeaderID()
		}
		return resp, nil
	}
	return &proto.AddServerResponse{Success: true}, nil
}

func (s *Service) RemoveServer(ctx context.Context, req *proto.RemoveServerRequest) (*proto.RemoveServerResponse, error) {
	if req == nil {
		return &proto.RemoveServerResponse{Success: false, ErrorMessage: "invalid request"}, nil
	}
	err := s.node.RemoveServer(ctx, req.Address)
	if err != nil {
		resp := &proto.RemoveServerResponse{Success: false, ErrorMessage: err.Error()}
		if errors.Is(err, ErrNotLeader) {
			resp.LeaderId = s.node.LeaderID()
		}
		return resp, nil
	}
	return &proto.RemoveServerResponse{Success: true}, nil
}

func (s *Service) ClusterStatus(_ context.Context, _ *proto.ClusterStatusRequest) (*proto.ClusterStatusResponse, error) {
	st := s.node.Status()
	return &proto.ClusterStatusResponse{
		NodeId:        st.ID,
		State:         st.State.String(),
		Term:          st.Term,
		LeaderId:      st.LeaderID,
		CommitIndex:   st.CommitIndex,
		AppliedIndex:  st.AppliedIndex,
		LastLogIndex:  st.LastLogIndex,
		LastLogTerm:   st.LastLogTerm,
		SnapshotIndex: st.SnapshotIndex,
		Members:       st.Members,
	}, nil
}
