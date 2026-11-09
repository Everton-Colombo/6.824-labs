package kvraft

import (
	"encoding/json"
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

	// /status must report the same leader on every node.
	leaderID := -1
	for i := 0; i < n; i++ {
		st := fetchStatus(t, httpServers[i].URL)
		if st.IsLeader {
			leaderID = st.ID
		}
	}
	if leaderID == -1 {
		t.Fatal("no leader in /status")
	}
	for i := 0; i < n; i++ {
		st := fetchStatus(t, httpServers[i].URL)
		if st.LeaderID != leaderID {
			t.Fatalf("node %d reports leader %d, want %d", i, st.LeaderID, leaderID)
		}
		if st.LastLogIndex < st.CommitIndex || st.CommitIndex < st.LastApplied {
			t.Fatalf("node %d inconsistent indexes: %+v", i, st)
		}
	}
}

type statusJSON struct {
	ID             int  `json:"id"`
	Term           int  `json:"term"`
	IsLeader       bool `json:"is_leader"`
	LeaderID       int  `json:"leader_id"`
	CommitIndex    int  `json:"commit_index"`
	LastLogIndex   int  `json:"last_log_index"`
	LastApplied    int  `json:"last_applied"`
	RaftStateBytes int  `json:"raft_state_bytes"`
	SnapshotBytes  int  `json:"snapshot_bytes"`
}

func fetchStatus(t *testing.T, baseURL string) statusJSON {
	t.Helper()
	resp, err := http.Get(baseURL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st statusJSON
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	return st
}
