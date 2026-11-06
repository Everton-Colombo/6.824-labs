package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"mc714/src/labgob"
	"mc714/src/labrpc"
	"mc714/src/raft"
)

const Timeout = 5 * time.Second

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type RequestId struct {
	ClientId int
	Seq      int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId RequestId
	Name      string // "Get", "Put" or "Append"
	Key       string
	Value     string
}

type ClientReponseRecord struct {
	Seq   int
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore                map[string]string
	waitingRequests        map[RequestId]chan RequestResponse // command index -> channel
	waitingIndex           map[int]RequestId                  // raft idx -> RequestId expeting to be there
	lastResponseValueCache map[int]ClientReponseRecord
}

func (kv *KVServer) takeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.lastResponseValueCache)

	return w.Bytes()
}

func (kv *KVServer) decodeSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvStore map[string]string
	var lrvCache map[int]ClientReponseRecord
	if d.Decode(&kvStore) == nil &&
		d.Decode(&lrvCache) == nil {
		kv.kvStore = kvStore
		kv.lastResponseValueCache = lrvCache
	}
}

func (kv *KVServer) applyMsgHandler() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.SnapshotValid {
			if kv.rf.CondInstallSnapshot(msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot) {
				kv.mu.Lock()
				kv.decodeSnapshot(msg.Snapshot)
				kv.mu.Unlock()
			}
			continue
		}

		if !msg.CommandValid {
			continue
		}

		response := RequestResponse{Err: OK}
		kv.mu.Lock()
		cmd := msg.Command.(Op)
		cachedRercord := kv.lastResponseValueCache[cmd.RequestId.ClientId]

		if rid, ok := kv.waitingIndex[msg.CommandIndex]; ok {
			delete(kv.waitingIndex, msg.CommandIndex)
			if rid != cmd.RequestId {
				if ch, ok2 := kv.waitingRequests[rid]; ok2 {
					delete(kv.waitingRequests, rid)
					ch <- RequestResponse{Err: ErrWrongLeader}
				}
			}
		}
		if cachedRercord.Seq >= cmd.RequestId.Seq {
			response.Value = cachedRercord.Value
		} else {
			switch cmd.Name {
			case "Put":
				kv.kvStore[cmd.Key] = cmd.Value
			case "Append":
				kv.kvStore[cmd.Key] += cmd.Value
			case "Get":
			}
			response.Value = kv.kvStore[cmd.Key]

			kv.lastResponseValueCache[cmd.RequestId.ClientId] = ClientReponseRecord{Seq: cmd.RequestId.Seq, Value: response.Value}

			// Snapshot if needed
			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
				snapshot := kv.takeSnapshot()
				kv.rf.Snapshot(msg.CommandIndex, snapshot)
			}
		}
		if ch, ok := kv.waitingRequests[cmd.RequestId]; ok {
			delete(kv.waitingRequests, cmd.RequestId)
			ch <- response
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) startAndWaitForApply(op Op) RequestResponse {
	waitCh := make(chan RequestResponse, 1)
	kv.mu.Lock()
	kv.waitingRequests[op.RequestId] = waitCh
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Lock()
		delete(kv.waitingRequests, op.RequestId)
		kv.mu.Unlock()
		return RequestResponse{Err: ErrWrongLeader}
	}

	kv.mu.Lock()
	kv.waitingIndex[index] = op.RequestId
	kv.mu.Unlock()

	select {
	case response := <-waitCh:
		return response
	case <-time.After(Timeout):
		kv.mu.Lock()
		delete(kv.waitingRequests, op.RequestId)
		delete(kv.waitingIndex, index)
		kv.mu.Unlock()
		return RequestResponse{Err: ErrTimeout}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *RequestResponse) {
	command := Op{
		Name: "Get",
		Key:  args.Key,
		RequestId: RequestId{
			ClientId: args.ClientID,
			Seq:      args.Seq,
		},
	}

	*reply = kv.startAndWaitForApply(command)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *RequestResponse) {
	command := Op{
		Name:  args.Op,
		Key:   args.Key,
		Value: args.Value,
		RequestId: RequestId{
			ClientId: args.ClientID,
			Seq:      args.Seq,
		},
	}

	*reply = kv.startAndWaitForApply(command)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func (kv *KVServer) Raft() *raft.Raft {
	return kv.rf
}

func StartKVServer(servers []labrpc.RPCClient, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvStore = make(map[string]string)
	kv.waitingRequests = make(map[RequestId]chan RequestResponse)
	kv.lastResponseValueCache = make(map[int]ClientReponseRecord)
	kv.waitingIndex = make(map[int]RequestId)

	if data := persister.ReadSnapshot(); len(data) > 0 {
		kv.decodeSnapshot(data)
	}

	go kv.applyMsgHandler()

	return kv
}
