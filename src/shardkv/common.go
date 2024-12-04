package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrOpOutOfDate     = "ErrOpOutOfDate"
	ErrHandleOpTimeOut = "ErrHandleOpTimeOut"

	ErrNotReady = "ErrNotReady" //for MigrateShardRPC
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64
	OperationId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId    int64
	OperationId int64
}

type GetReply struct {
	Err   Err
	Value string
}

// MigrateShardArgs RPC that ShardKV leader requests shard's ex-owner for shard's data
type MigrateShardArgs struct {
	ConfigNum int
	ShardNum  int
}

type MigrateShardReply struct {
	Err           Err
	ShardData     map[string]string
	HistoryResult map[int64]*Result
}

type AckArgs struct {
	ConfigNum int
	ShardNum  int
}

type AckReply struct {
	Err     Err
	Receive bool
}
