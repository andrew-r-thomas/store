package client

import (
	"net"
	"sync/atomic"
)

type Client struct {
	conns []*Conn
	next  atomic.Uint64
}

func (client *Client) BeginTxn() Txn {
	// simple round robin
	i := int(client.next.Add(1) - 1)
	return client.conns[i&(len(client.conns)-1)].beginTxn()
}

type Conn struct{}

func newConn(addr string) (*Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
}
func (conn *Conn) beginTxn() Txn {
	return Txn{}
}

type Txn struct{}

type Request struct {
	TxnId uint64
	Op    RequestOp
}
type RequestOp struct {
	flags byte
	data  []byte
}

type Format interface {
	size() int
	parse()
}
