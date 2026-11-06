package kvraft

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var autoClientID int64 = time.Now().UnixNano()

// Handler returns an http.Handler exposing the KVServer operations:
//   - GET  /kv/{key}   -> JSON RequestResponse with the current value
//   - PUT  /kv/{key}   -> sets the key to the request body
//   - POST /kv/{key}   -> appends the request body to the key
//   - GET  /status     -> JSON {"id": me, "term": term, "is_leader": bool}
//
// Followers reply 421 + {"Err":"ErrWrongLeader"}; the retry loop that moves to
// the next server lives in the Clerk (client side).
func (kv *KVServer) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		term, isLeader := kv.rf.GetState()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"id":        kv.me,
			"term":      term,
			"is_leader": isLeader,
		})
	})

	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		clientID, _ := strconv.Atoi(r.Header.Get("X-Client-Id"))
		seq, _ := strconv.Atoi(r.Header.Get("X-Seq"))
		if clientID == 0 {
			clientID = int(atomic.AddInt64(&autoClientID, 1))
			seq = 1
		}

		var resp RequestResponse
		switch r.Method {
		case http.MethodGet:
			kv.Get(&GetArgs{Key: key, ClientID: clientID, Seq: seq}, &resp)
		case http.MethodPut, http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			op := "Put"
			if r.Method == http.MethodPost {
				op = "Append"
			}
			kv.PutAppend(&PutAppendArgs{
				Key:      key,
				Value:    string(body),
				Op:       op,
				ClientID: clientID,
				Seq:      seq,
			}, &resp)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		switch resp.Err {
		case OK:
			w.WriteHeader(http.StatusOK)
		case ErrWrongLeader:
			w.WriteHeader(http.StatusMisdirectedRequest)
		case ErrTimeout:
			w.WriteHeader(http.StatusGatewayTimeout)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(resp)
	})

	return mux
}

// HTTPEnd implements labrpc.RPCClient for KVServer over the HTTP API,
// so kvraft.Clerk can talk to nodes over HTTP without changing Clerk's retry/dedup logic.
type HTTPEnd struct {
	baseURL string
	client  *http.Client
}

func MakeHTTPEnd(addr string, timeout time.Duration) *HTTPEnd {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &HTTPEnd{
		baseURL: strings.TrimRight(addr, "/"),
		client:  &http.Client{Timeout: timeout},
	}
}

func (e *HTTPEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	out, ok := reply.(*RequestResponse)
	if !ok {
		return false
	}

	var req *http.Request
	var err error

	switch svcMeth {
	case "KVServer.Get":
		a := args.(*GetArgs)
		req, err = http.NewRequest(http.MethodGet, e.baseURL+"/kv/"+a.Key, nil)
		if err != nil {
			return false
		}
		req.Header.Set("X-Client-Id", strconv.Itoa(a.ClientID))
		req.Header.Set("X-Seq", strconv.Itoa(a.Seq))

	case "KVServer.PutAppend":
		a := args.(*PutAppendArgs)
		method := http.MethodPut
		if a.Op == "Append" {
			method = http.MethodPost
		}
		req, err = http.NewRequest(method, e.baseURL+"/kv/"+a.Key, bytes.NewBufferString(a.Value))
		if err != nil {
			return false
		}
		req.Header.Set("X-Client-Id", strconv.Itoa(a.ClientID))
		req.Header.Set("X-Seq", strconv.Itoa(a.Seq))

	default:
		return false
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return false
	}
	return true
}
