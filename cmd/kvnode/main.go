package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mc714/src/kvraft"
	"mc714/src/labrpc"
	"mc714/src/raft"
)

type RaftRPC struct{ rf *raft.Raft }

func (r *RaftRPC) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	r.rf.RequestVote(args, reply)
	return nil
}

func (r *RaftRPC) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	r.rf.AppendEntries(args, reply)
	return nil
}

func (r *RaftRPC) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	r.rf.InstallSnapshot(args, reply)
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	me, err := strconv.Atoi(envOr("NODE_ID", "0"))
	if err != nil {
		log.Fatalf("invalid NODE_ID: %v", err)
	}
	peerAddrs := strings.Split(envOr("PEERS", "localhost:8000"), ",")
	raftAddr := envOr("RAFT_ADDR", ":8000")
	httpAddr := envOr("HTTP_ADDR", ":9000")
	dataDir := envOr("DATA_DIR", "/tmp/opencode/kvraft-node-"+strconv.Itoa(me))
	maxRaftState, _ := strconv.Atoi(envOr("MAX_RAFT_STATE", "4096"))

	ends := make([]labrpc.RPCClient, len(peerAddrs))
	for i, addr := range peerAddrs {
		ends[i] = labrpc.MakeTCPEnd(strings.TrimSpace(addr), 800*time.Millisecond)
	}

	persister, err := raft.MakeFilePersister(dataDir)
	if err != nil {
		log.Fatalf("persister init: %v", err)
	}

	kv := kvraft.StartKVServer(ends, me, persister, maxRaftState)

	// Internal peer-to-peer Raft RPC server (not exposed to external clients)
	raftSrv := rpc.NewServer()
	if err := raftSrv.RegisterName("Raft", &RaftRPC{rf: kv.Raft()}); err != nil {
		log.Fatalf("register Raft: %v", err)
	}
	raftLn, err := net.Listen("tcp", raftAddr)
	if err != nil {
		log.Fatalf("listen raft %s: %v", raftAddr, err)
	}
	go raftSrv.Accept(raftLn)

	// External HTTP API for KV operations
	httpSrv := &http.Server{
		Addr:    httpAddr,
		Handler: kv.Handler(),
	}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen http %s: %v", httpAddr, err)
		}
	}()

	log.Printf("node %d started: raft=%s http=%s peers=%v data=%s", me, raftAddr, httpAddr, peerAddrs, dataDir)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	kv.Kill()
	_ = httpSrv.Close()
	_ = raftLn.Close()
}
