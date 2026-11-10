// Copyright 2026 Digital Clever Solution LLC
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package validator

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.waterfall.network/waterfall/protocol/coordinator/testing/require"
)

func TestWarmUpRunner_OneAtATimeAndOneMoreAfterTrigger(t *testing.T) {
	var (
		calls   int32
		active  int32
		maxSeen int32
		release = make(chan struct{})
		started = make(chan struct{}, 8)
	)
	r := newWarmUpRunner(func() {
		n := atomic.AddInt32(&active, 1)
		for {
			m := atomic.LoadInt32(&maxSeen)
			if n <= m || atomic.CompareAndSwapInt32(&maxSeen, m, n) {
				break
			}
		}
		atomic.AddInt32(&calls, 1)
		started <- struct{}{}
		<-release
		atomic.AddInt32(&active, -1)
	})

	r.trigger()
	<-started
	// Triggers during a run collapse into one pending run.
	r.trigger()
	r.trigger()
	r.trigger()
	release <- struct{}{}
	<-started
	release <- struct{}{}

	require.NoError(t, waitIdle(r, time.Second))
	require.Equal(t, int32(2), atomic.LoadInt32(&calls))
	require.Equal(t, int32(1), atomic.LoadInt32(&maxSeen))
}

func TestWarmUpRunner_ConcurrentTriggers(t *testing.T) {
	var calls int32
	r := newWarmUpRunner(func() {
		atomic.AddInt32(&calls, 1)
		time.Sleep(time.Millisecond)
	})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.trigger()
		}()
	}
	wg.Wait()
	require.NoError(t, waitIdle(r, time.Second))
	got := atomic.LoadInt32(&calls)
	require.Equal(t, true, got >= 1 && got <= 50)
}

func waitIdle(r *warmUpRunner, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		idle := !r.running
		r.mu.Unlock()
		if idle {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return errTimeout
}

type timeoutError struct{}

func (timeoutError) Error() string { return "runner did not become idle" }

var errTimeout error = timeoutError{}
