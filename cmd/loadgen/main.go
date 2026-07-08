package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addrsRaw := flag.String("addrs", "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053", "comma-separated kv node addresses")
	keyPrefix := flag.String("key-prefix", "demo", "key prefix")
	interval := flag.Duration("interval", 300*time.Millisecond, "delay between operations")
	duration := flag.Duration("duration", 45*time.Second, "total run duration (0 for infinite)")
	flag.Parse()

	addrs := splitAddrs(*addrsRaw)
	if len(addrs) == 0 {
		fmt.Println("no addresses provided")
		return
	}

	type clientWrap struct {
		addr   string
		conn   *grpc.ClientConn
		client proto.KVServiceClient
	}
	clients := make([]clientWrap, 0, len(addrs))
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("dial failed addr=%s err=%v\n", addr, err)
			continue
		}
		clients = append(clients, clientWrap{addr: addr, conn: conn, client: proto.NewKVServiceClient(conn)})
	}
	if len(clients) == 0 {
		fmt.Println("no reachable nodes")
		return
	}
	defer func() {
		for _, c := range clients {
			_ = c.conn.Close()
		}
	}()

	fmt.Printf("loadgen started targets=%v interval=%s duration=%s\n", addrs, interval.String(), duration.String())
	start := time.Now()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	step := 0
	for {
		if *duration > 0 && time.Since(start) > *duration {
			fmt.Println("loadgen finished")
			return
		}

		c := clients[step%len(clients)]
		key := fmt.Sprintf("%s-%d", *keyPrefix, step%6)
		val := fmt.Sprintf("v-%d", step)

		ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
		setResp, setErr := c.client.Set(ctx, &proto.SetRequest{Key: key, Value: val})
		cancel()

		if setErr != nil {
			fmt.Printf("[%s] SET key=%s err=%v\n", c.addr, key, setErr)
		} else {
			fmt.Printf("[%s] SET key=%s success=%v msg=%q\n", c.addr, key, setResp.Success, setResp.ErrorMessage)
		}

		readFrom := clients[rng.Intn(len(clients))]
		ctx, cancel = context.WithTimeout(context.Background(), 1200*time.Millisecond)
		getResp, getErr := readFrom.client.Get(ctx, &proto.GetRequest{Key: key})
		cancel()

		if getErr != nil {
			fmt.Printf("[%s] GET key=%s err=%v\n", readFrom.addr, key, getErr)
		} else {
			fmt.Printf("[%s] GET key=%s success=%v value=%q msg=%q\n", readFrom.addr, key, getResp.Success, getResp.Value, getResp.ErrorMessage)
		}

		step++
		time.Sleep(*interval)
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
