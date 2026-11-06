package kvraft

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/rpc"
	"strings"
	"testing"
	"time"

	"mc714/src/labrpc"
	"mc714/src/raft"
)

type raftTCPAdapter struct{ rf *raft.Raft }

func (r *raftTCPAdapter) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	r.rf.RequestVote(args, reply)
	return nil
}

func (r *raftTCPAdapter) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	r.rf.AppendEntries(args, reply)
	return nil
}

func (r *raftTCPAdapter) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	r.rf.InstallSnapshot(args, reply)
	return nil
}

func TestHTTPAndTCPCluster(t *testing.T) {
	const n = 3
	raftListeners := make([]net.Listener, n)
	raftAddrs := make([]string, n)

	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		raftListeners[i] = ln
		raftAddrs[i] = ln.Addr().String()
	}

	servers := make([]*KVServer, n)
	httpServers := make([]*httptest.Server, n)
	defer func() {
		for i := 0; i < n; i++ {
			if httpServers[i] != nil {
				httpServers[i].Close()
			}
			if servers[i] != nil {
				servers[i].Kill()
			}
			if raftListeners[i] != nil {
				_ = raftListeners[i].Close()
			}
		}
	}()

	for i := 0; i < n; i++ {
		ends := make([]labrpc.RPCClient, n)
		for j := 0; j < n; j++ {
			ends[j] = labrpc.MakeTCPEnd(raftAddrs[j], 500*time.Millisecond)
		}
		ps, err := raft.MakeFilePersister(fmt.Sprintf("%s/node-%d", t.TempDir(), i))
		if err != nil {
			t.Fatalf("persister: %v", err)
		}
		kv := StartKVServer(ends, i, ps, 1024)
		servers[i] = kv

		// Internal Raft RPC server
		srv := rpc.NewServer()
		_ = srv.RegisterName("Raft", &raftTCPAdapter{rf: kv.Raft()})
		go srv.Accept(raftListeners[i])

		// External KV HTTP server
		httpServers[i] = httptest.NewServer(kv.Handler())
	}

	ckEnds := make([]labrpc.RPCClient, n)
	for j := 0; j < n; j++ {
		ckEnds[j] = MakeHTTPEnd(httpServers[j].URL, 1*time.Second)
	}
	ck := MakeClerk(ckEnds)

	ck.Put("course", "MC714")
	ck.Append("course", "-2s2026")
	if got := ck.Get("course"); got != "MC714-2s2026" {
		t.Fatalf("got %q, want %q", got, "MC714-2s2026")
	}

	// The retry loop lives in the Clerk: a follower answers 421, it does not
	// forward or redirect. Check that the server side stays dumb.
	for i := 0; i < n; i++ {
		if _, isLeader := servers[i].Raft().GetState(); isLeader {
			continue
		}
		resp, err := http.Post(httpServers[i].URL+"/kv/course", "text/plain", strings.NewReader("x"))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusMisdirectedRequest {
			t.Fatalf("follower %d served a write: status=%d body=%s", i, resp.StatusCode, body)
		}
	}
}
