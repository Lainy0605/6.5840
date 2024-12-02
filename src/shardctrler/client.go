package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId    int64
	operationId int64
	leaderId    int
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
	// Your code here.
	ck.clientId = nrand()
	ck.operationId = 0
	return ck
}

func (ck *Clerk) getOperationId() int64 {
	ck.operationId += 1
	return ck.operationId
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.OperationId = ck.getOperationId()

	for {
		// try each known server.
		reply := &QueryReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, reply)
		if ok && reply.Err == OK {
			return reply.Config
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.OperationId = ck.getOperationId()

	for {
		// try each known server.
		reply := &JoinReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.OperationId = ck.getOperationId()

	for {
		// try each known server.
		reply := &LeaveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.OperationId = ck.getOperationId()

	for {
		// try each known server.
		reply := &MoveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
