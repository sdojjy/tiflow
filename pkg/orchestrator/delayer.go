// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package orchestrator

import (
	"sync"
	"time"
)

// ChannelDelayer provides a mechanism to inject delay in a channel.
type ChannelDelayer[T any] struct {
	inCh  <-chan T
	outCh chan T

	queue []entry[T]
	size  int

	closeCh chan struct{}
	wg      sync.WaitGroup

	delayBy time.Duration
}

type entry[T any] struct {
	elem   T
	inTime time.Time
}

// NewChannelDelayer creates a new ChannelDelayer.
func NewChannelDelayer[T any](delayBy time.Duration, in <-chan T, queueSize int, outChSize int) *ChannelDelayer[T] {
	ret := &ChannelDelayer[T]{
		inCh:    in,
		outCh:   make(chan T, outChSize),
		queue:   make([]entry[T], 0, queueSize),
		size:    queueSize,
		closeCh: make(chan struct{}),
		delayBy: delayBy,
	}

	ret.wg.Add(1)
	go func() {
		ret.run()
	}()

	return ret
}

// Out returns the delayed channel. The downstream logic
// should read from Out().
func (d *ChannelDelayer[T]) Out() <-chan T {
	return d.outCh
}

// Close closes the ChannelDelayer.
func (d *ChannelDelayer[T]) Close() {
	close(d.closeCh)
	d.wg.Wait()
}

func (d *ChannelDelayer[T]) run() {
	defer d.wg.Done()
	defer close(d.outCh)

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	currentTime := time.Now()

	outChIfReady := func() chan<- T {
		if len(d.queue) == 0 {
			return nil
		}
		if currentTime.Sub(d.queue[0].inTime) >= d.delayBy {
			return d.outCh
		}
		return nil
	}

	var dummyEntry entry[T]
	for {
		var nextOut *T
		if len(d.queue) > 0 {
			nextOut = &d.queue[0].elem
		} else {
			nextOut = &dummyEntry.elem
		}

		select {
		case <-d.closeCh:
			return
		case <-ticker.C:
			currentTime = time.Now()
		case inElem, ok := <-d.inChIfSizeOk():
			if !ok {
				if len(d.queue) == 0 {
					return
				}
				continue
			}
			d.queue = append(d.queue, entry[T]{
				elem:   inElem,
				inTime: time.Now(),
			})
		case outChIfReady() <- *nextOut:
			// Cleans any reference to *T if T is a pointer
			d.queue[0] = dummyEntry
			d.queue = d.queue[1:]

		LOOP:
			for {
				if len(d.queue) == 0 {
					break LOOP
				}
				if currentTime.Sub(d.queue[0].inTime) < d.delayBy {
					break
				}

				select {
				case d.outCh <- d.queue[0].elem:
					// Cleans any reference to *T if T is a pointer
					d.queue[0] = dummyEntry
					d.queue = d.queue[1:]
				default:
					break LOOP
				}
			}
		}
	}
}

func (d *ChannelDelayer[T]) inChIfSizeOk() <-chan T {
	if len(d.queue) < d.size {
		return d.inCh
	}
	return nil
}
