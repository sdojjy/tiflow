// Copyright 2024 PingCAP, Inc.
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

package coordinator

import (
	"github.com/pingcap/tiflow/new_arch/scheduler"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

var _ Scheduler = &balanceScheduler{}

// The scheduler for balancing tables among all captures.
type balanceScheduler struct {
	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
	// forceBalance forces the scheduler to produce schedule tasks regardless of
	// `checkBalanceInterval`.
	// It is set to true when the last time `Schedule` produces some tasks,
	// and it is likely there are more tasks will be produced in the next
	// `Schedule`.
	// It speeds up rebalance.
	forceBalance bool

	maxTaskConcurrency int
	changefeedID       model.ChangeFeedID
}

func newBalanceScheduler(interval time.Duration, concurrency int) *balanceScheduler {
	return &balanceScheduler{
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		checkBalanceInterval: interval,
		maxTaskConcurrency:   concurrency,
	}
}

func (b *balanceScheduler) Name() string {
	return "balance-scheduler"
}

func (b *balanceScheduler) Schedule(
	currentChangefeeds []*model.ChangeFeedInfo,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	replications map[model.ChangeFeedID]*changefeed,
) []*ScheduleTask {
	if !b.forceBalance {
		now := time.Now()
		if now.Sub(b.lastRebalanceTime) < b.checkBalanceInterval {
			// skip balance.
			return nil
		}
		b.lastRebalanceTime = now
	}

	for _, capture := range aliveCaptures {
		if capture.State == CaptureStateStopping {
			log.Debug("schedulerv3: capture is stopping, premature to balance table",
				zap.String("namespace", b.changefeedID.Namespace),
				zap.String("changefeed", b.changefeedID.ID))
			return nil
		}
	}

	tasks := buildBalanceMoveTables(
		b.random, aliveCaptures, replications, b.maxTaskConcurrency)
	b.forceBalance = len(tasks) != 0
	return tasks
}

func buildBalanceMoveTables(
	random *rand.Rand,
	captures map[model.CaptureID]*CaptureStatus,
	replications map[model.ChangeFeedID]*changefeed,
	maxTaskConcurrency int,
) []*ScheduleTask {
	moves := newBalanceMoveTables(
		random, captures, replications, maxTaskConcurrency)
	tasks := make([]*ScheduleTask, 0, len(moves))
	for i := 0; i < len(moves); i++ {
		// No need for accept callback here.
		tasks = append(tasks, &ScheduleTask{MoveChangefeed: &moves[i]})
	}
	return tasks
}

func newBalanceMoveTables(
	random *rand.Rand,
	captures map[model.CaptureID]*CaptureStatus,
	replications map[model.ChangeFeedID]*changefeed,
	maxTaskLimit int,
) []MoveChangefeed {
	tablesPerCapture := make(map[model.CaptureID]map[model.ChangeFeedID]*changefeed)
	for captureID := range captures {
		tablesPerCapture[captureID] = make(map[model.ChangeFeedID]*changefeed)
	}

	for _, rep := range replications {
		if rep.scheduleState == scheduler.SchedulerComponentStatusWorking {
			tablesPerCapture[rep.primary][rep.ID] = rep
		}
	}

	// findVictim return tables which need to be moved
	upperLimitPerCapture := int(math.Ceil(float64(len(replications)) / float64(len(captures))))

	victims := make([]*changefeed, 0)
	for _, ts := range tablesPerCapture {
		var changefeesds []*changefeed
		for _, t := range ts {
			changefeesds = append(changefeesds, t)
		}
		if random != nil {
			// Complexity note: Shuffle has O(n), where `n` is the number of tables.
			// Also, during a single call of `Schedule`, Shuffle can be called at most
			// `c` times, where `c` is the number of captures (TiCDC nodes).
			// Only called when a rebalance is triggered, which happens rarely,
			// we do not expect a performance degradation as a result of adding
			// the randomness.
			random.Shuffle(len(changefeesds), func(i, j int) {
				changefeesds[i], changefeesds[j] = changefeesds[j], changefeesds[i]
			})
		} else {
			// sort the spans here so that the result is deterministic,
			// which would aid testing and debugging.
			sort.Slice(changefeesds, func(i, j int) bool {
				return changefeesds[i].ID.ID < changefeesds[j].ID.ID
			})
		}

		tableNum2Remove := len(changefeesds) - upperLimitPerCapture
		if tableNum2Remove <= 0 {
			continue
		}

		for _, span := range changefeesds {
			if tableNum2Remove <= 0 {
				break
			}
			victims = append(victims, span)
			delete(ts, span.ID)
			tableNum2Remove--
		}
	}
	if len(victims) == 0 {
		return nil
	}

	captureWorkload := make(map[model.CaptureID]int)
	for captureID, ts := range tablesPerCapture {
		captureWorkload[captureID] = randomizeWorkload(random, len(ts))
	}
	// for each victim table, find the target for it
	moveTables := make([]MoveChangefeed, 0, len(victims))
	for idx, span := range victims {
		target := ""
		minWorkload := math.MaxInt64

		for captureID, workload := range captureWorkload {
			if workload < minWorkload {
				minWorkload = workload
				target = captureID
			}
		}

		if minWorkload == math.MaxInt64 {
			log.Panic("schedulerv3: rebalance meet unexpected min workload " +
				"when try to the the target capture")
		}
		if idx >= maxTaskLimit {
			// We have reached the task limit.
			break
		}

		moveTables = append(moveTables, MoveChangefeed{
			Changefeed:  span.ID,
			DestCapture: target,
		})
		tablesPerCapture[target][span.ID] = span
		captureWorkload[target] = randomizeWorkload(random, len(tablesPerCapture[target]))
	}

	return moveTables
}

const (
	randomPartBitSize = 8
	randomPartMask    = (1 << randomPartBitSize) - 1
)

// randomizeWorkload injects small randomness into the workload, so that
// when two captures tied in competing for the minimum workload, the result
// will not always be the same.
// The bitwise layout of the return value is:
// 63                8                0
// |----- input -----|-- random val --|
func randomizeWorkload(random *rand.Rand, input int) int {
	var randomPart int
	if random != nil {
		randomPart = int(random.Uint32() & randomPartMask)
	}
	// randomPart is a small random value that only affects the
	// result of comparison of workloads when two workloads are equal.
	return (input << randomPartBitSize) | randomPart
}
