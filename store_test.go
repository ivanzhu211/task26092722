package byodstore

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newID(t *testing.T) [16]byte {
	t.Helper()
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestBasicRoundtrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id := newID(t)
	if err := s.Create(id); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(id); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	v1 := bytes.Repeat([]byte("v1-"), 10000)
	if err := s.Put(id, "a", bytes.NewReader(v1)); err != nil {
		t.Fatal(err)
	}
	rc, err := s.Get(id, "a")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil || !bytes.Equal(got, v1) {
		t.Fatalf("roundtrip mismatch err=%v", err)
	}

	// Failed put leaves no trace: old value preserved.
	flaky := &failAfterReader{data: bytes.Repeat([]byte("v2-"), 10000), failAt: 5000}
	if err := s.Put(id, "a", flaky); err == nil {
		t.Fatal("expected put error")
	}
	rc, _ = s.Get(id, "a")
	got, _ = io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, v1) {
		t.Fatal("failed put clobbered old value")
	}

	// Successful replace.
	v2 := bytes.Repeat([]byte("v2-"), 10000)
	if err := s.Put(id, "a", bytes.NewReader(v2)); err != nil {
		t.Fatal(err)
	}
	rc, _ = s.Get(id, "a")
	got, _ = io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, v2) {
		t.Fatal("replace not visible")
	}

	if _, err := s.Get(id, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

type failAfterReader struct {
	data   []byte
	off    int
	failAt int
}

func (r *failAfterReader) Read(p []byte) (int, error) {
	if r.off >= r.failAt {
		return 0, errors.New("boom")
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

func TestStreamInvalidation(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	defer s.Close()
	id := newID(t)
	s.Create(id)
	val := bytes.Repeat([]byte("x"), 1<<20)
	s.Put(id, "e", bytes.NewReader(val))

	rc, err := s.Get(id, "e")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 100)
	if n, err := rc.Read(buf); n != 100 || err != nil {
		t.Fatalf("pre-suspend read failed: n=%d err=%v", n, err)
	}
	if err := s.Suspend(id); err != nil {
		t.Fatal(err)
	}
	if n, err := rc.Read(buf); n != 0 || err == nil {
		t.Fatalf("read after suspend returned data: n=%d err=%v", n, err)
	}
	// Data access denied while suspended.
	if _, err := s.Get(id, "e"); !errors.Is(err, ErrNotActive) {
		t.Fatalf("get while suspended: %v", err)
	}
	if err := s.Put(id, "e", bytes.NewReader(val)); !errors.Is(err, ErrNotActive) {
		t.Fatalf("put while suspended: %v", err)
	}
	if err := s.Resume(id); err != nil {
		t.Fatal(err)
	}
	// Old stream stays dead after resume.
	if n, err := rc.Read(buf); n != 0 || err == nil {
		t.Fatalf("read after resume on old stream: n=%d err=%v", n, err)
	}
	rc.Close()
	// New stream works and content survived.
	rc2, err := s.Get(id, "e")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc2)
	rc2.Close()
	if !bytes.Equal(got, val) {
		t.Fatal("content lost across suspend/resume")
	}

	// Wipe invalidates too.
	rc3, _ := s.Get(id, "e")
	first, err := s.Wipe(id)
	if !first || err != nil {
		t.Fatalf("wipe: first=%v err=%v", first, err)
	}
	if n, err := rc3.Read(buf); n != 0 || err == nil {
		t.Fatalf("read after wipe: n=%d err=%v", n, err)
	}
	if _, err := s.Get(id, "e"); !errors.Is(err, ErrNotActive) {
		t.Fatalf("get after wipe: %v", err)
	}
	if err := s.Put(id, "e", bytes.NewReader(val)); !errors.Is(err, ErrNotActive) {
		t.Fatalf("put after wipe: %v", err)
	}
	// Wipe is terminal.
	s.Resume(id)
	s.Suspend(id)
	if err := s.Put(id, "e", bytes.NewReader(val)); !errors.Is(err, ErrNotActive) {
		t.Fatalf("wiped container revived: %v", err)
	}
}

func TestWipeConcurrentIdempotent(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	defer s.Close()
	id := newID(t)
	s.Create(id)
	s.Put(id, "e", bytes.NewReader([]byte("data")))

	const N = 16
	var wg sync.WaitGroup
	var firsts int64
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			first, err := s.Wipe(id)
			if err != nil {
				t.Errorf("wipe err: %v", err)
			}
			if first {
				atomic.AddInt64(&firsts, 1)
			}
		}()
	}
	wg.Wait()
	if firsts != 1 {
		t.Fatalf("expected exactly 1 first, got %d", firsts)
	}
	// Repeat after concurrency settles.
	first, err := s.Wipe(id)
	if first || err != nil {
		t.Fatalf("repeat wipe: first=%v err=%v", first, err)
	}
}

// gateReader emits data, then blocks until released.
type gateReader struct {
	data     []byte
	off      int
	gate     <-chan struct{}
	entered  chan struct{}
	signaled int32
}

func (g *gateReader) Read(p []byte) (int, error) {
	if g.off >= len(g.data) {
		return 0, io.EOF
	}
	if g.off > 0 {
		if atomic.CompareAndSwapInt32(&g.signaled, 0, 1) {
			close(g.entered)
		}
		<-g.gate
	}
	n := copy(p, g.data[g.off:])
	g.off += n
	return n, nil
}

func TestSuspendDoesNotWaitForPut(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	defer s.Close()
	id := newID(t)
	s.Create(id)
	s.Put(id, "e", bytes.NewReader([]byte("old")))

	gate := make(chan struct{})
	gr := &gateReader{data: bytes.Repeat([]byte("n"), 1<<20), gate: gate, entered: make(chan struct{})}
	putDone := make(chan error, 1)
	go func() { putDone <- s.Put(id, "e", gr) }()
	<-gr.entered // put is now stuck mid-stream

	start := time.Now()
	if err := s.Suspend(id); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("suspend blocked on stuck put: %v", d)
	}
	select {
	case err := <-putDone:
		if err == nil {
			t.Fatal("stuck put returned nil after suspend")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stuck put did not return after suspend")
	}
	close(gate)
	// Old value intact.
	s.Resume(id)
	rc, _ := s.Get(id, "e")
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "old" {
		t.Fatalf("entry changed by failed put: %q", got[:min(10, len(got))])
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	ids := [3][16]byte{newID(t), newID(t), newID(t)}
	for _, id := range ids {
		s.Create(id)
	}
	val := []byte("persist-me")
	s.Put(ids[0], "e", bytes.NewReader(val))
	s.Put(ids[1], "e", bytes.NewReader(val))
	s.Suspend(ids[1])
	s.Wipe(ids[2])
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	rc, err := s2.Get(ids[0], "e")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, val) {
		t.Fatal("active container data lost")
	}
	if _, err := s2.Get(ids[1], "e"); !errors.Is(err, ErrNotActive) {
		t.Fatalf("suspended state lost: %v", err)
	}
	s2.Resume(ids[1])
	rc, _ = s2.Get(ids[1], "e")
	got, _ = io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, val) {
		t.Fatal("suspended container data lost")
	}
	if first, err := s2.Wipe(ids[2]); first || err != nil {
		t.Fatalf("wiped state lost: first=%v err=%v", first, err)
	}
}

// scanFor fails the test if any file under dir contains marker.
func scanFor(t *testing.T, dir string, marker []byte) {
	t.Helper()
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		if bytes.Contains(data, marker) {
			t.Errorf("container bytes found in %s after wipe", path)
		}
		return nil
	})
}

func TestStress(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const NC = 5
	var ids [NC][16]byte
	var vals [NC][]byte
	var markers [NC][]byte
	for i := 0; i < NC; i++ {
		ids[i] = newID(t)
		if err := s.Create(ids[i]); err != nil {
			t.Fatal(err)
		}
		vals[i] = bytes.Repeat(ids[i][:], 4096) // 64 KiB
		markers[i] = bytes.Repeat(ids[i][:], 4)
		if err := s.Put(ids[i], "main", bytes.NewReader(vals[i])); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	stop := make([]chan struct{}, NC)
	for i := 0; i < NC; i++ {
		stop[i] = make(chan struct{})
	}

	// Getters: stream entries continuously; validate every byte read.
	for i := 0; i < NC; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 4096)
			for {
				select {
				case <-stop[i]:
					return
				default:
				}
				rc, err := s.Get(ids[i], "main")
				if err != nil {
					time.Sleep(time.Millisecond)
					continue
				}
				var acc []byte
				for {
					n, err := rc.Read(buf)
					acc = append(acc, buf[:n]...)
					if !bytes.Equal(acc, vals[i][:len(acc)]) {
						t.Errorf("container %d: stream returned foreign bytes", i)
						break
					}
					if err != nil {
						if err == io.EOF {
							if len(acc) != len(vals[i]) {
								t.Errorf("container %d: short read %d/%d", i, len(acc), len(vals[i]))
							}
						} else {
							// Invalidated stream: must stay dead.
							if n2, err2 := rc.Read(buf); n2 != 0 || err2 == nil {
								t.Errorf("container %d: invalidated stream revived n=%d err=%v", i, n2, err2)
							}
						}
						break
					}
				}
				rc.Close()
			}
		}()
	}

	// Putters: rewrite the entry with the same value until wiped.
	for i := 0; i < NC; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop[i]:
					return
				default:
				}
				err := s.Put(ids[i], "main", bytes.NewReader(vals[i]))
				if err == nil {
					continue
				}
				if errors.Is(err, ErrNotActive) || errors.Is(err, ErrStreamInvalid) {
					select {
					case <-stop[i]:
						return
					default:
						time.Sleep(time.Millisecond)
					}
					continue
				}
				t.Errorf("container %d: unexpected put error %v", i, err)
				return
			}
		}()
	}

	// Chaos agents: random suspend/resume, then a stuck put, then wipe,
	// then scan the whole data directory for the container's bytes.
	for i := 0; i < NC; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(stop[i])
			for j := 0; j < 5; j++ {
				s.Suspend(ids[i])
				time.Sleep(time.Duration(1+j) * time.Millisecond)
				s.Resume(ids[i])
			}
			// Start a put whose source blocks mid-stream.
			gate := make(chan struct{})
			gr := &gateReader{data: vals[i], gate: gate, entered: make(chan struct{})}
			putDone := make(chan error, 1)
			go func() { putDone <- s.Put(ids[i], "stuck", gr) }()
			<-gr.entered

			first, err := s.Wipe(ids[i])
			if err != nil || !first {
				t.Errorf("container %d: wipe first=%v err=%v", i, first, err)
			}
			// Property 2: immediately after Wipe returns, no file in the
			// data directory may contain this container's bytes.
			scanFor(t, dir, markers[i])
			// Release the stuck put; it must fail and leave no trace.
			close(gate)
			select {
			case err := <-putDone:
				if err == nil {
					t.Errorf("container %d: stuck put returned nil after wipe", i)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("container %d: stuck put never returned", i)
			}
			scanFor(t, dir, markers[i])
			// Concurrent repeat wipes are fine.
			first, err = s.Wipe(ids[i])
			if first || err != nil {
				t.Errorf("container %d: repeat wipe first=%v err=%v", i, first, err)
			}
		}()
	}
	wg.Wait()
}

func TestNoCrossContainerSerialization(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	defer s.Close()
	var ids [2][16]byte
	for i := range ids {
		ids[i] = newID(t)
		s.Create(ids[i])
	}
	// A stuck put on container 0 must not block container 1.
	gate := make(chan struct{})
	defer close(gate)
	gr := &gateReader{data: bytes.Repeat([]byte("a"), 1<<20), gate: gate, entered: make(chan struct{})}
	go s.Put(ids[0], "x", gr)
	<-gr.entered

	done := make(chan error, 1)
	go func() { done <- s.Put(ids[1], "y", bytes.NewReader([]byte("fast"))) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("container 1 put blocked by container 0")
	}
	rc, err := s.Get(ids[1], "y")
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	// Suspend of container 0 must not affect container 1 either.
	s.Suspend(ids[0])
	if _, err := s.Get(ids[1], "y"); err != nil {
		t.Fatal(err)
	}
}
