// Package sim is a simulated network for testing distributed protocols.
//
// Every RPC passes through a Network, which can drop, delay, and reorder
// messages, disconnect nodes, and split the cluster into partitions. Arguments
// and replies are gob-encoded on the way through, so handlers never share
// memory with callers, just like over a real wire.
package sim

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// NodeID identifies a node on the network.
type NodeID = int

// Network routes RPCs between registered nodes.
type Network struct {
	mu       sync.Mutex
	rng      *rand.Rand
	services map[NodeID]*service
	up       map[NodeID]bool // false means the node is cut off from everyone
	group    map[NodeID]int  // nodes talk only if they share a group
	dropRate float64         // probability of dropping a request or a reply
	maxDelay time.Duration   // upper bound on random per-message delay
	rpcs     atomic.Int64
	bytes    atomic.Int64
}

// New returns an empty, fully reliable network. The seed drives every random
// choice the network makes (drops, delays).
func New(seed int64) *Network {
	return &Network{
		rng:      rand.New(rand.NewSource(seed)),
		services: map[NodeID]*service{},
		up:       map[NodeID]bool{},
		group:    map[NodeID]int{},
	}
}

// Register makes svc reachable as node id. Exported methods of the form
//
//	func (s *T) Name(args *A, reply *R)
//
// become callable by name. Registering again replaces the previous service;
// replies still in flight from the old one are discarded, which models a
// crash followed by a restart.
func (n *Network) Register(id NodeID, svc any) {
	s := newService(svc)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.services[id] = s
	if _, ok := n.up[id]; !ok {
		n.up[id] = true
	}
}

// Unregister removes node id. Calls to it fail until it is registered again.
func (n *Network) Unregister(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.services, id)
}

// Connect lets node id send and receive messages again.
func (n *Network) Connect(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.up[id] = true
}

// Disconnect cuts node id off from every other node.
func (n *Network) Disconnect(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.up[id] = false
}

// Partition splits the network: nodes can only talk to nodes in the same
// group. Nodes not listed in any group end up together in a group of their own.
func (n *Network) Partition(groups ...[]NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[NodeID]int{}
	for g, ids := range groups {
		for _, id := range ids {
			n.group[id] = g + 1
		}
	}
}

// Heal removes all partitions. Individually disconnected nodes stay disconnected.
func (n *Network) Heal() {
	n.Partition()
}

// SetDropRate sets the probability (0 to 1) of losing each request and each reply.
func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = p
}

// SetMaxDelay sets the upper bound on the random delay added to each message.
// Random delays make messages arrive out of order.
func (n *Network) SetMaxDelay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.maxDelay = d
}

// RPCCount is the number of RPCs attempted so far.
func (n *Network) RPCCount() int64 { return n.rpcs.Load() }

// ByteCount is the number of argument bytes sent so far.
func (n *Network) ByteCount() int64 { return n.bytes.Load() }

// End returns a handle node from uses to call node to.
func (n *Network) End(from, to NodeID) *End {
	return &End{net: n, from: from, to: to}
}

// End is one node's connection to another. It is safe for concurrent use.
type End struct {
	net      *Network
	from, to NodeID
}

// Call invokes method on the remote node and fills in reply. It returns false
// if the request or the reply was lost; the handler may or may not have run.
// A lost message takes a while to report, the way a real timeout would.
func (e *End) Call(method string, args, reply any) bool {
	return e.net.call(e.from, e.to, method, args, reply)
}

func (n *Network) call(from, to NodeID, method string, args, reply any) bool {
	n.rpcs.Add(1)

	n.mu.Lock()
	svc := n.services[to]
	ok := svc != nil && n.reachableLocked(from, to)
	drop := n.dropRate > 0 && n.rng.Float64() < n.dropRate
	delay := n.delayLocked()
	n.mu.Unlock()

	if !ok {
		// Unreachable peers look like slow timeouts, not instant errors.
		time.Sleep(n.timeout())
		return false
	}
	time.Sleep(delay)
	if drop {
		time.Sleep(n.timeout())
		return false
	}

	reqBytes := encode(args)
	n.bytes.Add(int64(len(reqBytes)))
	replyBytes, err := svc.dispatch(method, reqBytes)
	if err != nil {
		panic(fmt.Sprintf("sim: node %d calling %d: %v", from, to, err))
	}

	n.mu.Lock()
	// The reply is lost if the link broke, or the callee crashed and
	// restarted, while the handler ran.
	ok = n.services[to] == svc && n.reachableLocked(from, to)
	drop = n.dropRate > 0 && n.rng.Float64() < n.dropRate
	delay = n.delayLocked()
	n.mu.Unlock()

	if !ok || drop {
		time.Sleep(n.timeout())
		return false
	}
	time.Sleep(delay)
	decode(replyBytes, reply)
	return true
}

func (n *Network) reachableLocked(from, to NodeID) bool {
	return n.up[from] && n.up[to] && n.group[from] == n.group[to]
}

func (n *Network) delayLocked() time.Duration {
	if n.maxDelay <= 0 {
		return 0
	}
	return time.Duration(n.rng.Int63n(int64(n.maxDelay)))
}

func (n *Network) timeout() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return time.Duration(50+n.rng.Intn(50)) * time.Millisecond
}

// service holds the callable methods of one registered object.
type service struct {
	rcvr    reflect.Value
	methods map[string]reflect.Method
}

func newService(svc any) *service {
	s := &service{rcvr: reflect.ValueOf(svc), methods: map[string]reflect.Method{}}
	t := reflect.TypeOf(svc)
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		mt := m.Type
		if mt.NumIn() == 3 && mt.NumOut() == 0 &&
			mt.In(1).Kind() == reflect.Pointer && mt.In(2).Kind() == reflect.Pointer {
			s.methods[m.Name] = m
		}
	}
	return s
}

func (s *service) dispatch(method string, reqBytes []byte) ([]byte, error) {
	m, ok := s.methods[method]
	if !ok {
		return nil, fmt.Errorf("unknown method %q", method)
	}
	args := reflect.New(m.Type.In(1).Elem())
	reply := reflect.New(m.Type.In(2).Elem())
	decode(reqBytes, args.Interface())
	m.Func.Call([]reflect.Value{s.rcvr, args, reply})
	return encode(reply.Interface()), nil
}

func encode(v any) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		panic(fmt.Sprintf("sim: encode %T: %v", v, err))
	}
	return buf.Bytes()
}

func decode(data []byte, v any) {
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(v); err != nil {
		panic(fmt.Sprintf("sim: decode %T: %v", v, err))
	}
}
