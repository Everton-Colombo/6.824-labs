package labrpc

import (
	"net"
	"net/rpc"
	"sync"
	"time"
)

// TCPEnd implements RPCClient over stdlib net/rpc with an explicit timeout.
type TCPEnd struct {
	addr    string
	timeout time.Duration
	mu      sync.Mutex
	client  *rpc.Client
}

func MakeTCPEnd(addr string, timeout time.Duration) *TCPEnd {
	if timeout <= 0 {
		timeout = 1 * time.Second
	}
	return &TCPEnd{addr: addr, timeout: timeout}
}

func (e *TCPEnd) getClient() (*rpc.Client, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client != nil {
		return e.client, nil
	}
	conn, err := net.DialTimeout("tcp", e.addr, e.timeout)
	if err != nil {
		return nil, err
	}
	e.client = rpc.NewClient(conn)
	return e.client, nil
}

func (e *TCPEnd) resetClient(bad *rpc.Client) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client == bad && e.client != nil {
		_ = e.client.Close()
		e.client = nil
	}
}

func (e *TCPEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	cl, err := e.getClient()
	if err != nil {
		return false
	}
	call := cl.Go(svcMeth, args, reply, make(chan *rpc.Call, 1))
	select {
	case res := <-call.Done:
		if res.Error != nil {
			e.resetClient(cl)
			return false
		}
		return true
	case <-time.After(e.timeout):
		e.resetClient(cl)
		return false
	}
}
