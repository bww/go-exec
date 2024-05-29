package exec

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bww/go-util/v1/debug"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	debug.DumpRoutinesOnInterrupt()
	os.Exit(m.Run())
}

func addLater(v *int32, n int32) func() error {
	return func() error {
		atomic.AddInt32(v, n)
		return nil
	}
}

func errorLater(err error) func() error {
	return func() error {
		return err
	}
}

func TestDispatch(t *testing.T) {
	concurrent, backlog := 20, 50
	disp := NewDispatcher(concurrent, backlog)

	// not yet started; returns immediately
	disp.Wait()

	cxt, cancel := context.WithTimeout(context.Background(), time.Minute) // don't wait forever on error
	defer cancel()

	// start up
	err := disp.Run(cxt)
	if !assert.Nil(t, err, fmt.Sprint(err)) {
		return
	}

	// can't do it twice
	err = disp.Run(cxt)
	if assert.NotNil(t, err, "Expected an error") {
		assert.Equal(t, ErrRunning, err)
	}

	var total, expect int32
	n := int32(500)

	// schedule some work and compute the correct answer
	for i := int32(0); i < n; i++ {
		addLater(&expect, i)()
		disp.Exec(addLater(&total, i))
		if (i+1)%50 == 0 {
			fmt.Println(i+1, disp.Stats())
		}
	}

	// wait for work to finish and check it
	disp.Wait()
	fmt.Println(disp.Stats())
	assert.Equal(t, expect, total)
}

func TestErrors(t *testing.T) {
	concurrent, backlog := 5, 10
	n := int32(500)

	t.Run("Process full set with errors", func(t *testing.T) {
		disp := NewDispatcher(concurrent, backlog, Failfast(false))
		cxt, cancel := context.WithTimeout(context.Background(), time.Minute) // don't wait forever on error
		defer cancel()

		err := disp.Run(cxt)
		if !assert.Nil(t, err, fmt.Sprint(err)) {
			return
		}

		// schedule some work, we'll produce an error from every block. we
		// should buffer up the first `backlog` errors and the rest will be
		// dropped.
		for i := int32(0); i < n; i++ {
			disp.Exec(errorLater(errInvalidConcurrency))
			if (i+1)%50 == 0 {
				fmt.Println(i+1, disp.Stats())
			}
		}

		// wait for work to finish and check errors
		var nerr int
		for {
			err := disp.Error()
			if err == nil {
				break
			}
			nerr++
		}

		// we can wait again but it returns immediately
		disp.Wait()

		// check it
		fmt.Println(disp.Stats())
		assert.GreaterOrEqual(t, nerr, concurrent+backlog)
	})

	t.Run("Fail fast(er) with errors", func(t *testing.T) {
		disp := NewDispatcher(concurrent, backlog)
		cxt, cancel := context.WithTimeout(context.Background(), time.Minute) // don't wait forever on error
		defer cancel()

		err := disp.Run(cxt)
		if !assert.Nil(t, err, fmt.Sprint(err)) {
			return
		}

		// schedule some work, we'll produce an error from the first block only, after which
		// subsequent work will be cancelled
		for i := int32(0); i < n; i++ {
			disp.Exec(errorLater(errInvalidConcurrency))
			if (i+1)%50 == 0 {
				fmt.Println(i+1, disp.Stats())
			}
		}

		// wait for work to finish and check errors
		var nerr int
		for {
			err := disp.Error()
			if err == nil {
				break
			}
			nerr++
		}

		// we can wait again but it returns immediately
		disp.Wait()

		// check it
		fmt.Println(disp.Stats())
		assert.GreaterOrEqual(t, nerr, concurrent)
	})
}

func TestCancellation(t *testing.T) {
	concurrent, backlog := 5, 0
	disp := NewDispatcher(concurrent, backlog, Failfast(false))

	cxt, cancel := context.WithCancel(context.Background())
	err := disp.Run(cxt)
	if !assert.Nil(t, err, fmt.Sprint(err)) {
		return
	}

	var proc int32

	// schedule some work, we'll cancel after 1/4 of the work
	// has been scheduled and the rest should be dropped.
	n := 500
	c := n / 4
	for i := 0; i < n; i++ {
		if i == c {
			cancel()
		}
		err := disp.Exec(addLater(&proc, 1))
		if (i+1)%50 == 0 {
			fmt.Println(i+1, disp.Stats())
		}
		if i < c {
			assert.Nil(t, err, fmt.Sprint(err))
		} else if i >= c {
			assert.Equal(t, ErrCanceled, err)
		}
	}

	// wait for work to finish and check errors
	var nerr int
	for {
		err := disp.Error()
		if err == nil {
			break
		}
		nerr++
	}

	// we can wait again but it returns immediately
	disp.Wait()
	// wait briefly for an in-flight task to finish executing
	<-time.After(time.Millisecond * 250)

	// check it
	fmt.Println(disp.Stats())
	assert.Equal(t, 0, nerr)
	assert.GreaterOrEqual(t, int(proc), c-backlog)
	assert.GreaterOrEqual(t, c, int(proc))
}
