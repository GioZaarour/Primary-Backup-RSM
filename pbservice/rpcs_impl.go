package pbservice

// In all data types that represent arguments to RPCs, field names
// must start with capital letters, otherwise RPC will break.

//
// additional state to include in arguments to PutAppend RPC.
//

// GRACE PART
type PutAppendArgsImpl struct {
	ClientID  int64
	RequestID int64
	Operation string
}

//
// additional state to include in arguments to Get RPC.
//
type GetArgsImpl struct {
	ClientID  int64
	RequestID int64
}

//
// for new RPCs that you add, declare types for arguments and reply.
//
type ForwardDatabaseArgs struct {
	Data map[string]string
}

type ForwardDatabaseReply struct {
	Err Err
}

type ForwardPutArgs struct {
	ClientID  int64
	RequestID int64
	Operation string
	Key       string
	Value     string
}

type ForwardPutReply struct {
	Err Err
}
