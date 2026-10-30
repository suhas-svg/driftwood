package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// Persister is a node's stable storage. Raft must save currentTerm, votedFor,
// and log[] here before replying to any RPC that changed them.
//
// NewPersister keeps everything in memory, which is what the simulated tests
// use: the harness hands the same Persister (or a copy) to a restarted node.
// NewFilePersister also writes every Save to a file, so a real process can
// crash and come back.
type Persister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte

	path   string // state file; empty for memory only
	noSync bool   // skip fsync (benchmarks only: a crash can lose acknowledged writes)
}

func NewPersister() *Persister { return &Persister{} }

const (
	stateFile  = "state.bin"
	stateMagic = "DWv1"
)

// NewFilePersister opens (or creates) the state stored in dir. With sync
// false, Save skips fsync: much faster, but a machine crash can then lose
// state Raft already acted on, which breaks Raft's safety guarantees.
func NewFilePersister(dir string, sync bool) (*Persister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &Persister{path: filepath.Join(dir, stateFile), noSync: !sync}
	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return p, nil // fresh node
	}
	if err != nil {
		return nil, err
	}
	p.raftState, p.snapshot, err = decodeStateFile(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p.path, err)
	}
	return p, nil
}

// Copy returns an independent, memory-only Persister holding the same bytes.
func (p *Persister) Copy() *Persister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &Persister{raftState: clone(p.raftState), snapshot: clone(p.snapshot)}
}

// Save atomically writes Raft state and the snapshot together. For a file
// persister, it returns only once both are durable on disk.
func (p *Persister) Save(raftState, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.raftState = clone(raftState)
	p.snapshot = clone(snapshot)
	if p.path != "" {
		if err := p.writeFile(); err != nil {
			// Raft cannot continue safely if it cannot persist: replying to an
			// RPC after a failed save could break its guarantees.
			panic(fmt.Sprintf("persister: %v", err))
		}
	}
}

// writeFile replaces the state file in one step: write a temporary file,
// fsync it, then rename it over the old one. A crash at any point leaves
// either the old state or the new one, never a mix. Caller holds p.mu.
func (p *Persister) writeFile() error {
	tmp := p.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(encodeStateFile(p.raftState, p.snapshot)); err != nil {
		f.Close()
		return err
	}
	if !p.noSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, p.path); err != nil {
		return err
	}
	if !p.noSync {
		syncDir(filepath.Dir(p.path))
	}
	return nil
}

// syncDir makes the rename durable on systems that need it. Windows cannot
// fsync a directory, so errors are ignored there.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}

// State file layout: magic, then length-prefixed raft state and snapshot,
// then a CRC-32 of everything before it to catch a torn or corrupt file.
func encodeStateFile(raftState, snapshot []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString(stateMagic)
	binary.Write(&buf, binary.LittleEndian, uint64(len(raftState)))
	buf.Write(raftState)
	binary.Write(&buf, binary.LittleEndian, uint64(len(snapshot)))
	buf.Write(snapshot)
	binary.Write(&buf, binary.LittleEndian, crc32.ChecksumIEEE(buf.Bytes()))
	return buf.Bytes()
}

func decodeStateFile(data []byte) (raftState, snapshot []byte, err error) {
	if len(data) < len(stateMagic)+8+8+4 || string(data[:len(stateMagic)]) != stateMagic {
		return nil, nil, errors.New("not a driftwood state file")
	}
	body, sum := data[:len(data)-4], binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return nil, nil, errors.New("state file checksum mismatch")
	}
	r := bytes.NewReader(body[len(stateMagic):])
	read := func() ([]byte, error) {
		var n uint64
		if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
			return nil, err
		}
		if n > uint64(r.Len()) {
			return nil, errors.New("state file truncated")
		}
		b := make([]byte, n)
		r.Read(b)
		return b, nil
	}
	if raftState, err = read(); err != nil {
		return nil, nil, err
	}
	if snapshot, err = read(); err != nil {
		return nil, nil, err
	}
	return raftState, snapshot, nil
}

func (p *Persister) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.raftState)
}

func (p *Persister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raftState)
}

func (p *Persister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
