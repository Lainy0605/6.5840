package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me          int64
	leaderId    int
	operationId int64
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
	// You'll have to add code here.
	ck.me = nrand()
	ck.leaderId = 0
	ck.operationId = 0
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
	args := GetArgs{
		key,
		ck.me,
		ck.getOperationId(),
	}

	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)

		if !ok {
			continue
		}

		switch reply.Err {
		case OK:
			return reply.Value
		case ErrWrongLeader:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		case ErrNoKey:
			return ""
		}
	}

	// You will have to modify this function.
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, operationType OperationType) {
	// You will have to modify this function.
	args := PutAppendArgs{
		key,
		value,
		operationType,
		ck.me,
		ck.getOperationId(),
	}

	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)

		if !ok {
			continue
		}

		switch reply.Err {
		case ErrWrongLeader:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		case OK:
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

func (ck *Clerk) getOperationId() int64 {
	return atomic.AddInt64(&ck.operationId, 1)
}
