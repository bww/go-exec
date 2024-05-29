package exec

import (
	"sync"
)

func Concurrent(w []interface{}, n int, f func(interface{}) error) error {
	var err error
	var wg sync.WaitGroup
	var lock sync.Mutex
	sem := make(chan struct{}, n)

	for _, e := range w {
		sem <- struct{}{}

		lock.Lock()
		stop := err != nil
		lock.Unlock()
		if stop {
			break
		}

		wg.Add(1)
		go func(e interface{}) {
			defer func() { <-sem; wg.Done() }()
			if ferr := f(e); ferr != nil {
				lock.Lock()
				err = ferr
				lock.Unlock()
			}
		}(e)

	}

	wg.Wait()
	return err
}
