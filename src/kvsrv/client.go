package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	server *labrpc.ClientEnd
	Mu     sync.Mutex
	TaskId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)must match the declared types of the RPC handler function'sarguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.Mu.Lock()
	defer ck.Mu.Unlock()

	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}

	for !ck.server.Call("KVServer.Get", &args, &reply) {
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.Mu.Lock()
	defer ck.Mu.Unlock()

	ck.TaskId = nrand()

	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		TaskId: ck.TaskId,
	}
	reply := PutAppendReply{}

	for !ck.server.Call("KVServer."+op, &args, &reply) {
	}

	// 通知服务器删除数据
	if op == "Append" {
		args = PutAppendArgs{
			TaskType: "notify",
			TaskId:   ck.TaskId,
		}

		for !ck.server.Call("KVServer.Append", &args, &reply) {
		}
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
