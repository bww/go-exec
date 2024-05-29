package exec

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	ErrRunning            = errors.New("Already running")
	ErrNotRunning         = errors.New("Not running")
	ErrCanceled           = errors.New("Canceled")
	errInvalidConcurrency = errors.New("Invalid concurrency")
)

type Stats struct {
	Processed int64
	InFlight  int64
}

func (s Stats) String() string {
	return fmt.Sprintf("processed: %d, in flight: %d", s.Processed, s.InFlight)
}

type Block func() error

type Dispatcher struct {
	sync.Mutex
	c, b     int
	work     chan Block
	errs     chan error
	done     chan struct{}
	cxt      context.Context
	total    int64
	inflight int64
	failfast bool
}

func NewDispatcher(concurrent, backlog int, opts ...Option) *Dispatcher {
	if concurrent < 1 {
		concurrent = 1
	}
	if backlog < 0 {
		backlog = 0
	}
	conf := Config{
		Failfast: true, // fail fast by default
	}.WithOptions(opts)
	return &Dispatcher{
		Mutex:    sync.Mutex{},
		c:        concurrent,
		b:        backlog,
		work:     nil,
		errs:     nil,
		done:     nil,
		cxt:      nil,
		total:    0,
		inflight: 0,
		failfast: conf.Failfast,
	}
}

func (d *Dispatcher) Run(cxt context.Context) error {
	d.Lock()
	defer d.Unlock()
	if d.work != nil {
		return ErrRunning
	}
	d.work = make(chan Block, d.b)
	d.errs = make(chan error, d.c+d.b)
	d.done = make(chan struct{})
	d.cxt = cxt
	go d.run(cxt, d.c, d.work, d.errs, d.done, &d.total, &d.inflight)
	return nil
}

func (d *Dispatcher) run(cxt context.Context, n int, work <-chan Block, errs chan<- error, done chan struct{}, total, inflight *int64) {
	sem := make(chan struct{}, n)
	rwg := &sync.WaitGroup{}

	var closer sync.Once
outer:
	for {
		var ok bool
		var e Block
		select {
		case <-cxt.Done():
			break outer
		case <-done:
			break outer
		case e, ok = <-work:
			if !ok {
				break outer
			}
		}

		atomic.AddInt64(total, 1)
		atomic.AddInt64(inflight, 1)
		select {
		case <-cxt.Done():
			break outer
		case <-done:
			break outer
		case sem <- struct{}{}:
			//continue
		}

		rwg.Add(1)
		go func(e Block) {
			defer func() {
				<-sem
				atomic.AddInt64(inflight, -1)
				rwg.Done()
			}()
			if err := e(); err != nil {
				select {
				case errs <- err:
				default:
				}
				if d.failfast {
					closer.Do(func() {
						close(done)
					})
				}
			}
		}(e)
	}

	rwg.Wait()
	close(errs)
}

func (d *Dispatcher) close() {
	if d.work != nil {
		close(d.work)
		d.work = nil
	}
	d.cxt = nil
}

func (d *Dispatcher) Stats() Stats {
	d.Lock()
	defer d.Unlock()
	return Stats{
		Processed: atomic.LoadInt64(&d.total),
		InFlight:  atomic.LoadInt64(&d.inflight),
	}
}

func (d *Dispatcher) Close() {
	d.Lock()
	defer d.Unlock()
	d.close()
}

func (d *Dispatcher) Error() error {
	d.Lock()
	errs := d.errs
	d.close()
	d.Unlock()
	if errs == nil {
		return nil
	}
	return <-errs
}

func (d *Dispatcher) Wait() {
	d.Error() // wait and discard result
}

func (d *Dispatcher) Exec(w Block) error {
	d.Lock()
	defer d.Unlock()
	if d.work == nil {
		return ErrNotRunning
	}
	select {
	case <-d.cxt.Done():
		return ErrCanceled
	case <-d.done:
		return ErrCanceled
	case d.work <- w:
		return nil
	}
}
