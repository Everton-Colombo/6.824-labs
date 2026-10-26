package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	currentLeader int
	clientId      int
	seq           int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.currentLeader = 0
	ck.clientId = int(nrand())
	ck.seq = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.seq++
	getArgs := GetArgs{Key: key, ClientID: ck.clientId, Seq: ck.seq}
	var response RequestResponse
	for {
		ok := ck.servers[ck.currentLeader].Call("KVServer.Get", &getArgs, &response)
		if response.Err == ErrWrongLeader || !ok {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		} else {
			return response.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seq++
	getArgs := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientId, Seq: ck.seq}
	var response RequestResponse
	for {
		ok := ck.servers[ck.currentLeader].Call("KVServer.Get", &getArgs, &response)
		if response.Err == ErrWrongLeader || !ok {
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		} else {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
