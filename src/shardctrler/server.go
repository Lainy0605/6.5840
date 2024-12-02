package shardctrler

import (
	"6.5840/raft"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32
	// Your data here.
	historyResults map[int64]*Result
	waitCh         map[int]*chan Result
	configs        []Config // indexed by config num
}

const HandleOpTimeOut = 1000 * time.Millisecond
const (
	Join  = "Join"
	Leave = "Leave"
	Query = "Query"
	Move  = "Move"
)

type OpType string

type Op struct {
	// Your data here.
	ClientId    int64
	OperationId int64
	OpType      OpType

	Servers map[int][]string //Join
	GIDs    []int            //Leave

	Shard int //Move
	GID   int

	Num int //Query
}

type Result struct {
	OperationId int64
	Err         Err
	Config      Config
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := &Op{
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		OpType:      Join,
		Servers:     args.Servers,
	}
	res := sc.handleOp(op)

	reply.Err = res.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := &Op{
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		OpType:      Leave,
		GIDs:        args.GIDs,
	}
	res := sc.handleOp(op)

	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := &Op{
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		OpType:      Move,
		Shard:       args.Shard,
		GID:         args.GID,
	}
	res := sc.handleOp(op)

	reply.Err = res.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := &Op{
		ClientId:    args.ClientId,
		OperationId: args.OperationId,
		OpType:      Query,
		Num:         args.Num,
	}
	res := sc.handleOp(op)

	reply.Err = res.Err
	reply.Config = res.Config
}

func (sc *ShardCtrler) handleOp(args *Op) (res Result) {
	sc.mu.Lock()
	DPrintf("handleOp(peer:%d): get operation %v\n", sc.me, *args)
	if historyResult, exist := sc.historyResults[args.ClientId]; exist {
		if historyResult.OperationId > args.OperationId {
			res.Err = ErrOpOutOfDate
			sc.mu.Unlock()
			return
		} else if historyResult.OperationId == args.OperationId {
			sc.mu.Unlock()
			return *historyResult
		}
	}

	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(*args)

	if !isLeader {
		res.Err = WrongLeader
		DPrintf("handleOp(peer:%d): ErrWrongLeader\n", sc.me)
		return
	}

	waitCh := sc.createWaitCh(index)

	select {
	case msg := <-waitCh:
		res = msg
		res.Err = OK
		DPrintf("handleOp(Leader:%d): get operation result from channel:%v\n", sc.me, res)
	case <-time.After(HandleOpTimeOut):
		DPrintf("handleOp(Leader:%d): ErrHandleOpTimeOut\n", sc.me)
		res.Err = ErrHandleOpTimeOut
	}

	go sc.closeWaitCh(index)
	return
}

func (sc *ShardCtrler) createWaitCh(index int) chan Result {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	waitCh := make(chan Result, 1)
	sc.waitCh[index] = &waitCh
	return waitCh
}

func (sc *ShardCtrler) closeWaitCh(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if _, ok := sc.waitCh[index]; ok {
		close(*sc.waitCh[index])
		delete(sc.waitCh, index)
	}
}

func (sc *ShardCtrler) ApplyHandler() {
	for !sc.killed() {
		log := <-sc.applyCh
		if log.CommandValid {
			sc.mu.Lock()
			op, ok := log.Command.(Op)
			if !ok {
				DPrintf("Command Invalid\n")
			} else {
				res := Result{}
				needApply := false
				if historyResult, exist := sc.historyResults[op.ClientId]; exist {
					if historyResult.OperationId > op.OperationId {
						res.Err = ErrOpOutOfDate
					} else if historyResult.OperationId == op.OperationId {
						res = *historyResult
					} else if historyResult.OperationId < op.OperationId {
						needApply = true
					}
				} else {
					needApply = true
				}

				if needApply {
					res = sc.executeOp(&op)
					sc.historyResults[op.ClientId] = &res
				}

				if waitCh, exist := sc.waitCh[log.CommandIndex]; exist {
					currentTerm, isLeader := sc.rf.GetState()
					if currentTerm == log.SnapshotTerm && isLeader {
						DPrintf("ApplyHandler(Leader:%d): send result to channel\n", sc.me)
						*waitCh <- res
					}
				}
			}

			sc.mu.Unlock()
		} else {
			DPrintf("Invalid ApplyMsg\n")
		}
	}
}

func (sc *ShardCtrler) executeOp(op *Op) (res Result) {
	switch op.OpType {
	case Join:
		oldConfig := sc.getLastConfig()

		newGroups := sc.deepCopyGroups(oldConfig.Groups)
		for gid, newGroup := range op.Servers {
			newGroups[gid] = newGroup
		}

		newConfig := Config{
			oldConfig.Num + 1,
			sc.shardLoadBalance(newGroups, oldConfig.Shards),
			newGroups,
		}

		sc.configs = append(sc.configs, newConfig)
		res.Err = OK
	case Leave:
		oldConfig := sc.getLastConfig()

		newGroups := sc.deepCopyGroups(oldConfig.Groups)
		for _, gid := range op.GIDs {
			delete(newGroups, gid)
		}

		newConfig := Config{
			oldConfig.Num + 1,
			sc.shardLoadBalance(newGroups, oldConfig.Shards),
			newGroups,
		}

		sc.configs = append(sc.configs, newConfig)
		res.Err = OK
	case Move:
		oldConfig := sc.getLastConfig()

		var newShards [NShards]int
		for shard, gid := range oldConfig.Shards {
			newShards[shard] = gid
		}
		newShards[op.Shard] = op.GID

		newConfig := Config{
			oldConfig.Num + 1,
			newShards,
			oldConfig.Groups,
		}

		sc.configs = append(sc.configs, newConfig)
		res.Err = OK
	case Query:
		lastConfig := sc.getLastConfig()

		if op.Num == -1 || op.Num > lastConfig.Num {
			res.Config = lastConfig
		} else {
			res.Config = sc.configs[op.Num]
		}
		res.Err = OK
	}
	return
}

func (sc *ShardCtrler) shardLoadBalance(newGroups map[int][]string, shards [NShards]int) [NShards]int {
	groupLen := len(newGroups)
	if groupLen == 0 {
		for shard, _ := range shards {
			shards[shard] = 0
		}

		return shards
	}

	shardNumPerGroup := make(map[int]int, len(newGroups))
	//count how many shards does each group have, and set the shard's gid to 0 if that group doesn't in newGroups now
	for shard, gid := range shards {
		if _, exist := newGroups[gid]; exist {
			shardNumPerGroup[gid]++
		} else {
			shards[shard] = 0
		}
	}

	gids := make([]int, 0, groupLen)
	for gid, _ := range newGroups {
		gids = append(gids, gid)

		if _, exist := shardNumPerGroup[gid]; !exist {
			shardNumPerGroup[gid] = 0
		}
	}

	sortedGid := binarySort(gids, shardNumPerGroup)
	avgShardNumPerGroup := NShards / groupLen
	remainder := NShards % groupLen

	for i := 0; i < groupLen; i++ {
		targetShardNumPerGroup := avgShardNumPerGroup
		if i < remainder {
			targetShardNumPerGroup++
		}

		delta := shardNumPerGroup[sortedGid[i]] - targetShardNumPerGroup
		if delta > 0 {
			for j := 0; j < NShards; j++ {
				if delta == 0 {
					break
				}
				if shards[j] == sortedGid[i] {
					shards[j] = 0
					delta--
				}
			}
		}

		if delta < 0 {
			for j := 0; j < NShards; j++ {
				if delta == 0 {
					break
				}
				if shards[j] == 0 {
					shards[j] = sortedGid[i]
					delta++
				}
			}
		}
	}

	return shards
}

func binarySort(gid []int, shardCntPerGroup map[int]int) []int {
	newGid := make([]int, len(gid))

	for i := 0; i < len(gid); i++ {
		left := 0
		right := i
		target := gid[i]

		for left < right {
			mid := (right-left)/2 + left
			if shardCntPerGroup[target] > shardCntPerGroup[newGid[mid]] {
				right = mid
			} else if shardCntPerGroup[target] == shardCntPerGroup[newGid[mid]] {
				if target > newGid[mid] {
					left = mid + 1
				} else {
					right = mid
				}
			} else if shardCntPerGroup[target] < shardCntPerGroup[newGid[mid]] {
				left = mid + 1
			}
		}

		for j := i; j > left; j-- {
			newGid[j] = newGid[j-1]
		}
		newGid[left] = target
	}

	return newGid
}

func (sc *ShardCtrler) getLastConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) deepCopyGroups(oldGroup map[int][]string) map[int][]string {
	newGroup := make(map[int][]string, len(oldGroup))
	for gid, servers := range oldGroup {
		newGroup[gid] = append(newGroup[gid], servers[:]...)
	}

	return newGroup
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.historyResults = make(map[int64]*Result)
	sc.waitCh = make(map[int]*chan Result)

	go sc.ApplyHandler()
	return sc
}
