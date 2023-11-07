package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Impl  PutAppendArgsImpl
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key  string
	Impl GetArgsImpl
}

type GetReply struct {
	Err   Err
	Value string
}
