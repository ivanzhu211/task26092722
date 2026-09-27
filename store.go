package byodstore

import (
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrNotActive     = errors.New("byodstore: container not active")
	ErrNotFound      = errors.New("byodstore: container or entry not found")
	ErrAlreadyExists = errors.New("byodstore: container already exists")
	ErrStreamInvalid = errors.New("byodstore: stream invalidated")
	ErrStoreClosed   = errors.New("byodstore: store closed")
)

type State int

const (
	StateActive State = iota
	StateSuspended
	StateWiped
)

type Store struct {
	dir    string
	mu     sync.Mutex // guards cons and closed only; never held during I/O
	cons   map[[16]byte]*container
	closed bool
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, cons: make(map[[16]byte]*container)}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "c-") {
			continue
		}
		raw, err := hex.DecodeString(strings.TrimPrefix(e.Name(), "c-"))
		if err != nil || len(raw) != 16 {
			continue
		}
		var id [16]byte
		copy(id[:], raw)
		cdir := filepath.Join(dir, e.Name())
		st := StateActive
		if b, err := os.ReadFile(filepath.Join(cdir, stateFileName)); err == nil {
			switch strings.TrimSpace(string(b)) {
			case "suspended":
				st = StateSuspended
			case "wiped":
				st = StateWiped
			}
		}
		s.cons[id] = newContainer(cdir, st)
	}
	return s, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *Store) Create(id [16]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	if _, ok := s.cons[id]; ok {
		return ErrAlreadyExists
	}
	cdir := filepath.Join(s.dir, "c-"+hex.EncodeToString(id[:]))
	if err := os.MkdirAll(cdir, 0o700); err != nil {
		return err
	}
	c := newContainer(cdir, StateActive)
	c.mu.Lock()
	c.writeStateLocked()
	c.mu.Unlock()
	s.cons[id] = c
	return nil
}

func (s *Store) lookup(id [16]byte) (*container, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStoreClosed
	}
	c, ok := s.cons[id]
	if !ok {
		return nil, ErrNotFound
	}
	return c, nil
}

func (s *Store) Put(id [16]byte, name string, r io.Reader) error {
	c, err := s.lookup(id)
	if err != nil {
		return err
	}
	return c.put(name, r)
}

func (s *Store) Get(id [16]byte, name string) (io.ReadCloser, error) {
	c, err := s.lookup(id)
	if err != nil {
		return nil, err
	}
	return c.get(name)
}

func (s *Store) Suspend(id [16]byte) error {
	c, err := s.lookup(id)
	if err != nil {
		return err
	}
	c.suspend()
	return nil
}

func (s *Store) Resume(id [16]byte) error {
	c, err := s.lookup(id)
	if err != nil {
		return err
	}
	c.resume()
	return nil
}

func (s *Store) Wipe(id [16]byte) (bool, error) {
	c, err := s.lookup(id)
	if err != nil {
		return false, err
	}
	return c.wipe(), nil
}
