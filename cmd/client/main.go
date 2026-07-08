// client is a small CLI for exercising a cluster node.
//
// Examples:
//
//	go run ./cmd/client -op set -key name -value raft-kv
//	go run ./cmd/client -op get -key name
//	go run ./cmd/client -op lin-get -key name
//	go run ./cmd/client -op del -key name
//	go run ./cmd/client -op cas -key name -expect old -value new
//	go run ./cmd/client -op status
//	go run ./cmd/client -op add-server -peer 127.0.0.1:50054
//	go run ./cmd/client -op remove-server -peer 127.0.0.1:50054
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "server address")
	op := flag.String("op", "smoke", "operation: smoke|set|get|lin-get|del|cas|status|add-server|remove-server")
	key := flag.String("key", "name", "key")
	value := flag.String("value", "raft-kv", "value (set/cas new value)")
	expect := flag.String("expect", "", "cas expected value")
	expectAbsent := flag.Bool("expect-absent", false, "cas: succeed only if key absent")
	peer := flag.String("peer", "", "peer address for add-server/remove-server")
	flag.Parse()

	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	kv := proto.NewKVServiceClient(conn)
	admin := proto.NewRaftServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch *op {
	case "smoke":
		setResp, err := kv.Set(ctx, &proto.SetRequest{Key: *key, Value: *value})
		if err != nil {
			log.Fatalf("Set RPC failed: %v", err)
		}
		fmt.Printf("SET success=%v err=%q\n", setResp.Success, setResp.ErrorMessage)
		getResp, err := kv.Get(ctx, &proto.GetRequest{Key: *key})
		if err != nil {
			log.Fatalf("Get RPC failed: %v", err)
		}
		fmt.Printf("GET success=%v value=%q err=%q\n", getResp.Success, getResp.Value, getResp.ErrorMessage)

	case "set":
		resp, err := kv.Set(ctx, &proto.SetRequest{Key: *key, Value: *value})
		if err != nil {
			log.Fatalf("Set RPC failed: %v", err)
		}
		fmt.Printf("SET success=%v err=%q\n", resp.Success, resp.ErrorMessage)

	case "get":
		resp, err := kv.Get(ctx, &proto.GetRequest{Key: *key})
		if err != nil {
			log.Fatalf("Get RPC failed: %v", err)
		}
		fmt.Printf("GET success=%v value=%q err=%q\n", resp.Success, resp.Value, resp.ErrorMessage)

	case "lin-get":
		resp, err := kv.GetLinearizable(ctx, &proto.GetRequest{Key: *key})
		if err != nil {
			log.Fatalf("GetLinearizable RPC failed: %v", err)
		}
		fmt.Printf("LIN-GET success=%v value=%q err=%q\n", resp.Success, resp.Value, resp.ErrorMessage)

	case "del":
		resp, err := kv.Delete(ctx, &proto.DeleteRequest{Key: *key})
		if err != nil {
			log.Fatalf("Delete RPC failed: %v", err)
		}
		fmt.Printf("DEL success=%v existed=%v err=%q\n", resp.Success, resp.Existed, resp.ErrorMessage)

	case "cas":
		resp, err := kv.CompareAndSwap(ctx, &proto.CasRequest{
			Key:           *key,
			ExpectedValue: *expect,
			NewValue:      *value,
			ExpectAbsent:  *expectAbsent,
		})
		if err != nil {
			log.Fatalf("CompareAndSwap RPC failed: %v", err)
		}
		fmt.Printf("CAS success=%v swapped=%v current=%q err=%q\n", resp.Success, resp.Swapped, resp.CurrentValue, resp.ErrorMessage)

	case "status":
		resp, err := admin.ClusterStatus(ctx, &proto.ClusterStatusRequest{})
		if err != nil {
			log.Fatalf("ClusterStatus RPC failed: %v", err)
		}
		fmt.Printf("node=%d state=%s term=%d leader=%d commit=%d applied=%d lastLog=%d snap=%d\nmembers=%v\n",
			resp.NodeId, resp.State, resp.Term, resp.LeaderId, resp.CommitIndex,
			resp.AppliedIndex, resp.LastLogIndex, resp.SnapshotIndex, resp.Members)

	case "add-server":
		if *peer == "" {
			log.Fatal("add-server requires -peer")
		}
		resp, err := admin.AddServer(ctx, &proto.AddServerRequest{Address: *peer})
		if err != nil {
			log.Fatalf("AddServer RPC failed: %v", err)
		}
		fmt.Printf("ADD-SERVER success=%v err=%q leaderHint=%d\n", resp.Success, resp.ErrorMessage, resp.LeaderId)

	case "remove-server":
		if *peer == "" {
			log.Fatal("remove-server requires -peer")
		}
		resp, err := admin.RemoveServer(ctx, &proto.RemoveServerRequest{Address: *peer})
		if err != nil {
			log.Fatalf("RemoveServer RPC failed: %v", err)
		}
		fmt.Printf("REMOVE-SERVER success=%v err=%q leaderHint=%d\n", resp.Success, resp.ErrorMessage, resp.LeaderId)

	default:
		log.Fatalf("unknown -op %q", *op)
	}
}
