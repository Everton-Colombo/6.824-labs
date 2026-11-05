package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"os"
	"path/filepath"
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
	dir       string
}

func MakePersister() *Persister {
	return &Persister{}
}

func MakeFilePersister(dir string) (*Persister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ps := &Persister{dir: dir}
	ps.raftstate, _ = os.ReadFile(filepath.Join(dir, "raft.state"))
	ps.snapshot, _ = os.ReadFile(filepath.Join(dir, "kv.snap"))
	return ps, nil
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	if ps.dir != "" {
		_ = os.WriteFile(filepath.Join(ps.dir, "raft.state"), state, 0o644)
	}
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.snapshot = snapshot
	if ps.dir != "" {
		_ = os.WriteFile(filepath.Join(ps.dir, "raft.state"), state, 0o644)
		_ = os.WriteFile(filepath.Join(ps.dir, "kv.snap"), snapshot, 0o644)
	}
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
