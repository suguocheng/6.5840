package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string
	ClientId int
	TaskId   int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key      string
	ClientId int
	TaskId   int
}

type GetReply struct {
	Value string
}
