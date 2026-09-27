package byodstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type container struct {
	mu    sync.Mutex
	state State
	gen   uint64
	done  chan struct{}
	dir   string
	puts  map[string]struct{}
	seq   uint64
}

func newContainer(dir string, st State) *container {
	return &container{
		state: st,
		done:  make(chan struct{}),
		dir:   dir,
		puts:  make(map[string]struct{}),
	}
}

func entryPath(dir, name string) string {
	sum := sha256.Sum256([]byte(name))
	return filepath.Join(dir, "entry-"+hex.EncodeToString(sum[:]))
}

// bump invalidates all outstanding streams and in-flight puts.
// Caller must hold c.mu.
func (c *container) bump() {
	c.gen++
	close(c.done)
	c.done = make(chan struct{})
}

func (c *container) valid(gen uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == StateActive && c.gen == gen
}

// copyCancel streams src to dst in bounded-memory chunks. The source is
// pumped from a separate goroutine so that a blocked src.Read does not
// prevent cancellation when done is closed.
func copyCancel(dst *os.File, src io.Reader, done <-chan struct{}) error {
	type chunk struct {
		buf []byte
		n   int
		err error
	}
	ch := make(chan chunk, 2)
	go func() {
		for {
			buf := make([]byte, 32*1024)
			n, err := src.Read(buf)
			select {
			case ch <- chunk{buf, n, err}:
			case <-done:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	for {
		select {
		case ck := <-ch:
			if ck.n > 0 {
				if _, err := dst.Write(ck.buf[:ck.n]); err != nil {
					return err
				}
			}
			if ck.err != nil {
				if ck.err == io.EOF {
					return nil
				}
				return ck.err
			}
		case <-done:
			return ErrStreamInvalid
		}
	}
}

func (c *container) put(name string, r io.Reader) error {
	c.mu.Lock()
	if c.state != StateActive {
		c.mu.Unlock()
		return ErrNotActive
	}
	gen := c.gen
	done := c.done
	c.seq++
	tmp := fmt.Sprintf("tmp-%d-%d", c.seq, gen)
	tmpPath := filepath.Join(c.dir, tmp)
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	// Registered while still holding the lock, so a concurrent wipe
	// is guaranteed to unlink this file by name.
	c.puts[tmp] = struct{}{}
	c.mu.Unlock()

	copyErr := copyCancel(f, r, done)
	syncErr := error(nil)
	if copyErr == nil {
		syncErr = f.Sync()
	}
	f.Close()

	c.mu.Lock()
	delete(c.puts, tmp)
	if copyErr != nil || syncErr != nil || c.state != StateActive || c.gen != gen {
		c.mu.Unlock()
		os.Remove(tmpPath)
		if copyErr != nil {
			return copyErr
		}
		if syncErr != nil {
			return syncErr
		}
		return ErrStreamInvalid
	}
	err = os.Rename(tmpPath, entryPath(c.dir, name))
	c.mu.Unlock()
	if err != nil {
		os.Remove(tmpPath)
	}
	return err
}

func (c *container) get(name string) (io.ReadCloser, error) {
	c.mu.Lock()
	if c.state != StateActive {
		c.mu.Unlock()
		return nil, ErrNotActive
	}
	gen := c.gen
	c.mu.Unlock()

	f, err := os.Open(entryPath(c.dir, name))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &entryReader{c: c, gen: gen, f: f}, nil
}

func (c *container) suspend() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != StateActive {
		return
	}
	c.state = StateSuspended
	c.bump()
	c.writeStateLocked()
}

func (c *container) resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != StateSuspended {
		return
	}
	c.state = StateActive
	c.bump()
	c.writeStateLocked()
}

func (c *container) wipe() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	first := c.state != StateWiped
	if c.state != StateWiped {
		c.state = StateWiped
		c.bump()
	}
	// Unlink every committed entry and every in-flight temp file.
	// Temp files were registered under this same lock before being
	// created, and new ones can no longer be created (state is Wiped),
	// so after this point no file in the directory can contain
	// container bytes. Open fds keep writing to unlinked inodes only.
	entries, _ := os.ReadDir(c.dir)
	for _, e := range entries {
		name := e.Name()
		if name == stateFileName || name == stateFileName+".tmp" {
			continue
		}
		os.Remove(filepath.Join(c.dir, name))
	}
	for tmp := range c.puts {
		os.Remove(filepath.Join(c.dir, tmp))
	}
	c.writeStateLocked()
	return first
}

const stateFileName = "state"

func (c *container) writeStateLocked() {
	var s string
	switch c.state {
	case StateActive:
		s = "active"
	case StateSuspended:
		s = "suspended"
	default:
		s = "wiped"
	}
	tmp := filepath.Join(c.dir, stateFileName+".tmp")
	if err := os.WriteFile(tmp, []byte(s), 0o600); err != nil {
		return
	}
	os.Rename(tmp, filepath.Join(c.dir, stateFileName))
}

type entryReader struct {
	c   *container
	gen uint64
	f   *os.File
}

func (r *entryReader) Read(p []byte) (int, error) {
	if !r.c.valid(r.gen) {
		return 0, ErrStreamInvalid
	}
	return r.f.Read(p)
}

func (r *entryReader) Close() error {
	return r.f.Close()
}
