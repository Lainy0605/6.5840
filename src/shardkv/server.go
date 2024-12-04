package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

const GetLatestConfig = 80 * time.Millisecond
const handleOpTimeOut = 1000 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64
	OperationId int64
	OpType      OpType

	Key   string //for GET/PUT/APPEND
	Value string //for PUT/APPEND

	NewConfig shardctrler.Config //for UPDATECONFIG

	ConfigNum int               //for GetShard/GiveShard
	ShardNum  int               //for GetShard/GiveShard
	ShardData map[string]string //for GetShard
}

const (
	Get          OpType     = "GET"
	Put          OpType     = "PUT"
	Append       OpType     = "APPEND"
	UpdateConfig OpType     = "UpdateConfig"
	GetShard     OpType     = "GetShard"
	GiveShard    OpType     = "GiveShard"
	Exist        ShardState = "EXIST"
	NoExist      ShardState = "NOEXIST"
	WaitGet      ShardState = "WAITGET"
	WaitGive     ShardState = "WaitGive"
)

type OpType string
type ShardState string

type Result struct {
	OperationId int64

	Err   Err
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DB            map[int]map[string]string
	historyResult map[int64]*Result
	waitCh        map[int]*chan Result
	lastApplied   int

	mck       *shardctrler.Clerk
	curConfig shardctrler.Config
	preConfig shardctrler.Config

	ownedShards [shardctrler.NShards]ShardState //the state of group's owned shard
	dead        int32
}

func (kv *ShardKV) PersistSnapshot() []byte {
	// 调用时必须持有锁mu
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.DB) != nil ||
		e.Encode(kv.historyResult) != nil ||
		e.Encode(kv.preConfig) != nil ||
		e.Encode(kv.curConfig) != nil ||
		e.Encode(kv.ownedShards) != nil {
		log.Fatal("ShardKV encode error")
	}

	ShardKVState := w.Bytes()
	return ShardKVState
}

func (kv *ShardKV) ReadSnapshot(data []byte) {
	// 调用时必须持有锁mu
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var DB map[int]map[string]string
	var historyResult map[int64]*Result
	var preConfig shardctrler.Config
	var curConfig shardctrler.Config
	var ownedShards [shardctrler.NShards]ShardState

	if d.Decode(&DB) != nil ||
		d.Decode(&historyResult) != nil ||
		d.Decode(&preConfig) != nil ||
		d.Decode(&curConfig) != nil ||
		d.Decode(&ownedShards) != nil {
		log.Fatal("ShardKV decode error")
	} else {
		kv.DB = DB
		kv.historyResult = historyResult
		kv.preConfig = preConfig
		kv.curConfig = curConfig
		kv.ownedShards = ownedShards
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opArgs := &Op{
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		Key:         args.Key,
		OpType:      Get,
	}

	res := kv.handleOp(opArgs)

	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opArgs := &Op{
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		Key:         args.Key,
		Value:       args.Value,
	}
	if args.Op == "Put" {
		opArgs.OpType = Put
	} else {
		opArgs.OpType = Append
	}

	res := kv.handleOp(opArgs)
	reply.Err = res.Err
}

func (kv *ShardKV) handleOp(op *Op) (res Result) {
	kv.mu.Lock()

	if !kv.keyInGroup(op.Key) {
		res.Err = ErrWrongGroup
		kv.mu.Unlock()
		DPrintf("ShardKVServer(HandleOp<Group[%d] ShardKV[%d]>): Group isn't responsible for key[%s], shard[%d]\n", kv.gid, kv.me, op.Key, key2shard(op.Key))
		return
	}

	if history, exist := kv.historyResult[op.ClientId]; exist {
		if history.OperationId == op.OperationId {
			kv.mu.Unlock()
			DPrintf("ShardKVServer(HandleOp<Group[%d] ShardKV[%d]>): Operation[%d] has been executed\n", kv.gid, kv.me, op.OperationId)
			return *history
		} else if history.OperationId > op.OperationId {
			res.Err = ErrOpOutOfDate
			kv.mu.Unlock()
			DPrintf("ShardKVServer(HandleOp<Group[%d] ShardKV[%d]>): Operation[%d] has been out of date\n", kv.gid, kv.me, op.OperationId)
			return
		}
	}

	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		res.Err = ErrWrongLeader
		DPrintf("ShardKVServer(HandleOp<Group[%d] ShardKV[%d]>): ErrWrongLeader\n", kv.gid, kv.me)
		return
	}

	waitCh := kv.createWaitCh(index)
	select {
	case msg := <-waitCh:
		kv.mu.Lock()

		if kv.keyInGroup(op.Key) {
			res = msg
			res.Err = OK
			DPrintf("ShardKVServer(HandleOp<Group[%d] ShardKV[%d]>): Operation[%v] executed successfully, Reply[%v]\n", kv.gid, kv.me, op, res)
		} else {
			res.Err = ErrWrongGroup
			DPrintf("ShardKVServer(HandleOp<Group[%d] ShardKV[%d]>): Group isn't resonpisible for shard[%d]\n", kv.gid, kv.me, key2shard(op.Key))
		}

		kv.mu.Unlock()
	case <-time.After(handleOpTimeOut):
		res.Err = ErrHandleOpTimeOut
		DPrintf("ShardKVServer(HandleOp<Group[%d] ShardKV[%d]>): Operation[%d] timeout\n", kv.gid, kv.me, op.OperationId)
	}

	go kv.closeWaitCh(index)
	return
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) keyInGroup(key string) bool {
	shard := key2shard(key)
	return kv.ownedShards[shard] == Exist
}

func (kv *ShardKV) createWaitCh(index int) chan Result {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	waitCh := make(chan Result, 1)
	kv.waitCh[index] = &waitCh
	return waitCh
}

func (kv *ShardKV) closeWaitCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exist := kv.waitCh[index]; exist {
		close(*kv.waitCh[index])
		delete(kv.waitCh, index)
	}
}

func (kv *ShardKV) getLatestConfig() {
	for kv.killed() == false {
		//only leader can launch a config update
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(GetLatestConfig)
			continue
		}

		//only last config update finished can the next update begin
		if !kv.updateConfigReady() {
			time.Sleep(GetLatestConfig)
			DPrintf("ShardKVServer(GetLatestConfig<Group[%d] ShardKV[%d]>): last config update hasn't finished\n", kv.gid, kv.me)
			continue
		}

		kv.mu.Lock()
		curConfig := kv.curConfig
		kv.mu.Unlock()

		nextConfig := kv.mck.Query(curConfig.Num + 1)

		if nextConfig.Num == curConfig.Num+1 {
			configUpdateOp := Op{
				OpType:    UpdateConfig,
				NewConfig: nextConfig,
			}

			DPrintf("ShardKVServer(GetLatestConfig<Group[%d] ShardKV[%d]>): push ConfigUpdateOp to Raft with newConfigNum:%d\n", kv.gid, kv.me, nextConfig.Num)
			kv.rf.Start(configUpdateOp)
		}

		time.Sleep(GetLatestConfig)
	}
}

func (kv *ShardKV) checkAndGetShard() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(GetLatestConfig)
			continue
		}

		var waitGetShards []int
		kv.mu.Lock()
		for shard, state := range kv.ownedShards {
			if state == WaitGet {
				waitGetShards = append(waitGetShards, shard)
			}
		}

		preConfig := kv.preConfig
		curConfigNum := kv.curConfig.Num
		kv.mu.Unlock()

		var wg sync.WaitGroup
		for _, shard := range waitGetShards {
			wg.Add(1)
			preGid := preConfig.Shards[shard]
			preServers := preConfig.Groups[preGid]

			go func(servers []string, configNum int, shardNum int) {
				defer wg.Done()
				for _, server := range servers {
					sv := kv.make_end(server)
					args := MigrateShardArgs{
						ConfigNum: configNum,
						ShardNum:  shardNum,
					}
					reply := MigrateShardReply{}

					ok := sv.Call("ShardKV.MigrateShard", &args, &reply)

					if !ok || reply.Err == ErrWrongLeader {
						DPrintf("ShardKVServer(CheckAndGetShard<Group[%d] ShardKV[%d]>): request server%s for shard%d [Failed]|[ErrWrongLeader]\n", kv.gid, kv.me, server, shardNum)
						continue
					} else if reply.Err == ErrNotReady {
						DPrintf("ShardKVServer(CheckAndGetShard<Group[%d] ShardKV[%d]>): request server%s for shard%d [ErrNotReady]\n", kv.gid, kv.me, server, shardNum)
						break
					} else if reply.Err == OK {
						getShardOp := Op{
							OpType:    GetShard,
							ConfigNum: configNum,
							ShardNum:  shardNum,
							ShardData: reply.ShardData,
						}
						kv.rf.Start(getShardOp)
						DPrintf("ShardKVServer(CheckAndGetShard<Group[%d] ShardKV[%d]>): request server%s for shard%d [Successfully]\n", kv.gid, kv.me, server, shardNum)
						break
					}
				}
			}(preServers, curConfigNum, shard)
		}

		wg.Wait()
		time.Sleep(GetLatestConfig)
	}
}

func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum > kv.curConfig.Num {
		reply.Err = ErrNotReady
	} else if args.ConfigNum < kv.curConfig.Num {
		reply.Err = OK
	} else {
		shardData := kv.deepCopyMap(kv.DB[args.ShardNum])
		reply.ShardData = shardData
		reply.Err = OK
	}

	return
}

func (kv *ShardKV) AckReceivedShard(args *AckArgs, reply *AckReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum > kv.curConfig.Num {
		reply.Err = ErrNotReady
	} else if args.ConfigNum < kv.curConfig.Num {
		reply.Err = OK
		reply.Receive = true
	} else {
		reply.Err = OK
		if kv.ownedShards[args.ShardNum] == Exist {
			reply.Receive = true
		} else {
			reply.Receive = false
		}
	}

	return
}

func (kv *ShardKV) deepCopyMap(oldMap map[string]string) map[string]string {
	newMap := make(map[string]string, len(oldMap))
	for key, value := range oldMap {
		newMap[key] = value
	}

	return newMap
}

func (kv *ShardKV) checkAndGiveShard() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(GetLatestConfig)
			continue
		}

		var waitGiveShards []int
		kv.mu.Lock()

		for shard, state := range kv.ownedShards {
			if state == WaitGive {
				waitGiveShards = append(waitGiveShards, shard)
			}
		}

		curConfig := kv.curConfig
		kv.mu.Unlock()
		var wg sync.WaitGroup

		for _, shard := range waitGiveShards {
			wg.Add(1)
			curGid := curConfig.Shards[shard]
			curServers := curConfig.Groups[curGid]

			go func(servers []string, configNum int, shardNum int) {
				defer wg.Done()
				for _, server := range servers {
					sv := kv.make_end(server)
					args := AckArgs{
						configNum,
						shardNum,
					}
					reply := AckReply{}
					ok := sv.Call("ShardKV.AckReceivedShard", &args, &reply)

					if !ok || reply.Err == ErrWrongLeader {
						DPrintf("ShardKVServer(checkAndGiveShard<Group[%d] ShardKV[%d]>): request server%s for ack shard%d [Failed]|[ErrWrongLeader]\n", kv.gid, kv.me, server, shardNum)
						continue
					} else if reply.Err == ErrNotReady {
						DPrintf("ShardKVServer(checkAndGiveShard<Group[%d] ShardKV[%d]>): request server%s for ack shard%d [ErrNotReady]\n", kv.gid, kv.me, server, shardNum)
						break
					} else if reply.Err == OK {
						if !reply.Receive {
							DPrintf("ShardKVServer(checkAndGiveShard<Group[%d] ShardKV[%d]>): request server%s for ack shard%d [Haven't received]\n", kv.gid, kv.me, server, shardNum)
							break
						} else {
							giveShardOp := Op{
								OpType:    GiveShard,
								ConfigNum: configNum,
								ShardNum:  shardNum,
							}
							kv.rf.Start(giveShardOp)
							DPrintf("ShardKVServer(checkAndGiveShard<Group[%d] ShardKV[%d]>): request server%s for ack shard%d [Successfully]\n", kv.gid, kv.me, server, shardNum)
							break
						}
					}
				}
			}(curServers, curConfig.Num, shard)
		}
		wg.Wait()
		time.Sleep(GetLatestConfig)
	}
}

func (kv *ShardKV) updateConfigReady() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, state := range kv.ownedShards {
		if state != NoExist && state != Exist {
			return false
		}
	}

	return true
}

func (kv *ShardKV) ApplyHandler() {
	for kv.killed() == false {
		log := <-kv.applyCh
		if log.CommandValid {
			op := log.Command.(Op)
			if op.OpType == Get || op.OpType == Append || op.OpType == Put {
				DPrintf("ShardKVServer(ApplyHandler<Group[%d] ShardKV[%d]>): Start to execute client command:%v\n", kv.gid, kv.me, op)
				kv.executeClientCmd(op, log)
			} else if op.OpType == UpdateConfig || op.OpType == GiveShard || op.OpType == GetShard {
				DPrintf("ShardKVServer(ApplyHandler<Group[%d] ShardKV[%d]>): Start to execute config command:%v\n", kv.gid, kv.me, op)
				kv.executeConfigCmd(op, log)
			}
		} else if log.SnapshotValid {

		} else {
			//TODO
		}
	}
}

func (kv *ShardKV) executeClientCmd(op Op, log raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	res := Result{
		op.OperationId,
		OK,
		"",
	}

	if !kv.keyInGroup(op.Key) {
		res.Err = ErrWrongGroup
		return
	} else if log.CommandIndex <= kv.lastApplied {
		return
	} else {
		needApply := false
		if history, exist := kv.historyResult[op.ClientId]; exist {
			if history.OperationId == op.OperationId {
				res = *history
			} else if history.OperationId > op.OperationId {
				res.Err = ErrOpOutOfDate
			} else {
				needApply = true
			}
		} else {
			needApply = true
		}

		if needApply {
			shardNo := key2shard(op.Key)
			switch op.OpType {
			case Get:
				if value, ok := kv.DB[shardNo][op.Key]; ok {
					res.Value = value
				} else {
					res.Err = ErrNoKey
					res.Value = ""
				}
			case Put:
				kv.DB[shardNo][op.Key] = op.Value
			case Append:
				if value, ok := kv.DB[shardNo][op.Key]; ok {
					kv.DB[shardNo][op.Key] = value + op.Value
				} else {
					kv.DB[shardNo][op.Key] = op.Value
				}
			}

			kv.historyResult[op.ClientId] = &res
		}

		if waitCh, exist := kv.waitCh[log.CommandIndex]; exist {
			currentTerm, isLeader := kv.rf.GetState()
			if currentTerm == log.SnapshotTerm && isLeader {
				*waitCh <- res
			}
		}
	}
}

func (kv *ShardKV) executeConfigCmd(op Op, log raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if log.CommandIndex <= kv.lastApplied {
		return
	}

	switch op.OpType {
	case UpdateConfig:
		if op.NewConfig.Num == kv.curConfig.Num+1 {
			kv.updateShardState(op.NewConfig)
			DPrintf("ShardKVServer(executeConfigCmd<Group[%d] ShardKV[%d]>): executed UpdateConfig(NewConfig:%v)\n", kv.gid, kv.me, op.NewConfig)
		}
	case GetShard:
		if kv.curConfig.Num == op.ConfigNum && kv.ownedShards[op.ShardNum] == WaitGet {
			kvMap := kv.deepCopyMap(op.ShardData)
			kv.DB[op.ShardNum] = kvMap
			kv.ownedShards[op.ShardNum] = Exist

			//TODO
			DPrintf("ShardKVServer(executeConfigCmd<Group[%d] ShardKV[%d]>): executed GetShard(shardNum:%d, shardData:%v)\n", kv.gid, kv.me, op.ShardNum, op.ShardData)
		}
	case GiveShard:
		if kv.curConfig.Num == op.ConfigNum && kv.ownedShards[op.ShardNum] == WaitGive {
			kv.ownedShards[op.ShardNum] = NoExist
			kv.DB[op.ShardNum] = map[string]string{}

			DPrintf("ShardKVServer(executeConfigCmd<Group[%d] ShardKV[%d]>): executed GiveShard(shardNum:%d)\n", kv.gid, kv.me, op.ShardNum)
		}
	default:
		DPrintf("ShardKVServer(executeConfigCmd<Group[%d] ShardKV[%d]>): Unkonwn Cmd\n", kv.gid, kv.me)
	}
}

func (kv *ShardKV) updateShardState(newConfig shardctrler.Config) {
	for shard, gid := range newConfig.Shards {
		if kv.ownedShards[shard] == Exist && gid != kv.gid {
			kv.ownedShards[shard] = WaitGive
		}

		if kv.ownedShards[shard] == NoExist && gid == kv.gid {
			if newConfig.Num == 1 { //initial
				kv.ownedShards[shard] = Exist
			} else {
				kv.ownedShards[shard] = WaitGet
			}
		}
	}

	DPrintf("ShardKVServer(updateShardState<Group[%d] ShardKV[%d]>): update shard state:%v\n", kv.gid, kv.me, kv.ownedShards)
	kv.preConfig = kv.curConfig
	kv.curConfig = newConfig
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.historyResult = make(map[int64]*Result)
	kv.waitCh = make(map[int]*chan Result)
	kv.DB = make(map[int]map[string]string)
	kv.ownedShards = [shardctrler.NShards]ShardState{}
	for i := 0; i < shardctrler.NShards; i++ {
		kv.DB[i] = make(map[string]string)
		kv.ownedShards[i] = NoExist
	}
	kv.lastApplied = 0
	kv.preConfig = shardctrler.Config{}
	kv.curConfig = shardctrler.Config{}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ApplyHandler()
	go kv.getLatestConfig()
	go kv.checkAndGetShard()
	go kv.checkAndGiveShard()
	return kv
}
