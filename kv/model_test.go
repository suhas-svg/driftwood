package kv

import (
	"fmt"
	"hash/fnv"

	"github.com/anishathalye/porcupine"
)

type modelInput struct {
	Op    OpKind
	Key   string
	Value string
}

// kvModel is the sequential spec of the store, for Porcupine: a single map
// where Put replaces, Append concatenates, and Get returns the current value
// ("" for a missing key). Keys are independent, so the history is checked one
// key at a time, which keeps the search small.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(modelInput).Key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	// Per-partition state: the value of one key.
	Init: func() any { return "" },
	// Lets the checker cache states it has already explored, which matters
	// when crashes leave operations pending across hundreds of others.
	Hash: func(state any) uint64 {
		h := fnv.New64a()
		h.Write([]byte(state.(string)))
		return h.Sum64()
	},
	Step: func(state, input, output any) (bool, any) {
		in := input.(modelInput)
		v := state.(string)
		switch in.Op {
		case OpGet:
			return output.(string) == v, v
		case OpPut:
			return true, in.Value
		case OpAppend:
			return true, v + in.Value
		}
		panic("unknown op " + in.Op)
	},
	DescribeOperation: func(input, output any) string {
		in := input.(modelInput)
		switch in.Op {
		case OpGet:
			return fmt.Sprintf("get(%q) -> %q", in.Key, output)
		case OpPut:
			return fmt.Sprintf("put(%q, %q)", in.Key, in.Value)
		default:
			return fmt.Sprintf("append(%q, %q)", in.Key, in.Value)
		}
	},
}
