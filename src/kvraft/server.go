package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const OperationTimeOut = 1 * time.Second

type OperationArgs struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type        OperationType
	Key         string
	Value       string
	OperationId int64
	ClientId    int64
}

type OperationReply struct {
	Err         Err
	Value       string
	OperationId int64
	ClientId    int64
	Term        int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db             map[string]string
	waitCmdApplyCh map[int]*chan OperationReply
	latestOp       map[int64]*OperationReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := OperationArgs{
		GET,
		args.Key,
		"",
		args.OperationId,
		args.ClientId,
	}

	res := kv.handleOperation(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := OperationArgs{
		args.Type,
		args.Key,
		args.Value,
		args.OperationId,
		args.ClientId,
	}

	res := kv.handleOperation(op)
	reply.Err = res.Err
}

func (kv *KVServer) handleOperation(operationArgs OperationArgs) (reply OperationReply) {
	index, term, isLeader := kv.rf.Start(operationArgs)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	newChan := make(chan OperationReply)
	kv.waitCmdApplyCh[index] = &newChan
	DPrintf("HandleOperation(LEADER:%d Client:%d OperationId:%d): create new waitCmdApplyCh:%p\n", kv.me, operationArgs.ClientId, operationArgs.OperationId, &newChan)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		close(newChan)
		delete(kv.waitCmdApplyCh, index)
		kv.mu.Unlock()
	}()

	select {
	case msg, success := <-newChan:
		if success {
			if msg.OperationId == operationArgs.OperationId && msg.ClientId == operationArgs.ClientId && term == msg.Term {
				reply = msg
			} else {
				reply.Err = ErrLeaderOutOfDate
			}
		} else if !success {
			reply.Err = ErrChanClosed
		}
	case <-time.After(OperationTimeOut):
		reply.Err = ErrOperationTimeOut
	}

	return
}

func (kv *KVServer) ApplyDaemon() {
	for !kv.killed() {
		appliedLog := <-kv.applyCh
		if appliedLog.CommandValid {
			operation := appliedLog.Command.(OperationArgs)
			var res OperationReply
			needApply := true
			kv.mu.Lock()
			if latestOp, exist := kv.latestOp[operation.ClientId]; exist {
				if latestOp.OperationId == operation.OperationId {
					res = *latestOp
					needApply = false
				}
			}

			if needApply {
				res = kv.DBExecute(&operation)
				kv.latestOp[operation.ClientId] = &res
			}

			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.mu.Unlock()
				continue
			}

			responseCh, exist := kv.waitCmdApplyCh[appliedLog.CommandIndex]
			if !exist {
				DPrintf("ApplyDaemon(LEADER:%d commandIndex:%d): waitCmdApplyCh has been closed", kv.me, appliedLog.CommandIndex)
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()
			func() {
				defer func() {
					if recover() != nil {
						// 如果这里有 panic，是因为通道关闭
						DPrintf("leader %v ApplyDaemon 发现 ClientId:%v OperationId:%v 的管道不存在, 应该是超时被关闭了", kv.me, operation.ClientId, operation.OperationId)
					}
				}()
				*responseCh <- res
			}()
		}
	}
}

func (kv *KVServer) DBExecute(op *OperationArgs) (res OperationReply) {
	res.Err = OK
	res.OperationId = op.OperationId
	res.ClientId = op.ClientId
	res.Term, _ = kv.rf.GetState()
	switch op.Type {
	case GET:
		if value, exist := kv.db[op.Key]; exist {
			res.Value = value
			return
		} else {
			res.Err = ErrNoKey
			return
		}
	case PUT:
		kv.db[op.Key] = op.Value
		return
	case APPEND:
		if value, exist := kv.db[op.Key]; exist {
			kv.db[op.Key] = value + op.Value
			return
		} else {
			kv.db[op.Key] = op.Value
			return
		}
	}

	return
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OperationArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.waitCmdApplyCh = make(map[int]*chan OperationReply)
	kv.latestOp = make(map[int64]*OperationReply)
	go kv.ApplyDaemon()

	return kv
}
