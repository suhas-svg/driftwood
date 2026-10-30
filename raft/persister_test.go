package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePersister(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if p.RaftStateSize() != 0 || len(p.ReadSnapshot()) != 0 {
		t.Fatal("fresh persister is not empty")
	}
	p.Save([]byte("state-1"), []byte("snap-1"))
	p.Save([]byte("state-2"), []byte("snap-2"))

	// A new process opening the same directory sees the last Save.
	q, err := NewFilePersister(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := q.ReadRaftState(); !bytes.Equal(got, []byte("state-2")) {
		t.Fatalf("raft state = %q, want state-2", got)
	}
	if got := q.ReadSnapshot(); !bytes.Equal(got, []byte("snap-2")) {
		t.Fatalf("snapshot = %q, want snap-2", got)
	}
}

// A crash midway through writing leaves a partial temp file next to the
// real one. The real file still holds the previous, complete state.
func TestFilePersisterIgnoresPartialWrite(t *testing.T) {
	dir := t.TempDir()
	p, _ := NewFilePersister(dir, true)
	p.Save([]byte("good"), nil)
	os.WriteFile(filepath.Join(dir, stateFile+".tmp"), []byte("DWv1 half-writ"), 0o644)

	q, err := NewFilePersister(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := q.ReadRaftState(); !bytes.Equal(got, []byte("good")) {
		t.Fatalf("raft state = %q, want good", got)
	}
}

// A corrupted state file must be refused, not silently read as empty: a node
// that forgot its term and vote could vote twice.
func TestFilePersisterRejectsCorruption(t *testing.T) {
	dir := t.TempDir()
	p, _ := NewFilePersister(dir, true)
	p.Save([]byte("some raft state"), []byte("a snapshot"))

	path := filepath.Join(dir, stateFile)
	data, _ := os.ReadFile(path)
	data[10] ^= 0xff
	os.WriteFile(path, data, 0o644)

	if _, err := NewFilePersister(dir, true); err == nil {
		t.Fatal("corrupted state file was accepted")
	}
}
