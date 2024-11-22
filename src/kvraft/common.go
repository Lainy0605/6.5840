package kvraft

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrOperationTimeOut = "ErrOperationTimeOut"
	ErrChanClosed       = "ErrChanClosed"
	ErrLeaderOutOfDate  = "ErrLeaderOutOfDate"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Type  OperationType // "Put" or "Append"
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

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type OperationType string
