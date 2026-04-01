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
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
)

var _ Scheduler = &rebalanceScheduler{}

type rebalanceScheduler struct {
	rebalance int32
	random    *rand.Rand
}

func newRebalanceScheduler() *rebalanceScheduler {
	return &rebalanceScheduler{
		rebalance: 0,
		random:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (r *rebalanceScheduler) Name() string {
	return "rebalance-scheduler"
}

func (r *rebalanceScheduler) Schedule(
	currentChangefeeds []*model.ChangeFeedInfo,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	replications map[model.ChangeFeedID]*changefeed,
) []*ScheduleTask {
	// rebalance is not triggered, or there is still some pending task,
	// do not generate new tasks.
	if atomic.LoadInt32(&r.rebalance) == 0 {
		return nil
	}

	if len(aliveCaptures) == 0 {
		return nil
	}

	for _, capture := range aliveCaptures {
		if capture.State == CaptureStateStopping {
			log.Warn("schedulerv3: capture is stopping, ignore manual rebalance request")
			atomic.StoreInt32(&r.rebalance, 0)
			return nil
		}
	}

	// only rebalance when all tables are replicating
	for _, span := range currentChangefeeds {
		rep, ok := replications[model.DefaultChangeFeedID(span.ID)]
		if !ok {
			return nil
		}
		if rep.scheduleState != scheduler.SchedulerComponentStatusWorking {
			log.Debug("schedulerv3: not all table replicating, premature to rebalance tables")
			return nil
		}
	}

	unlimited := math.MaxInt
	tasks := newBalanceMoveTables(r.random, aliveCaptures, replications, unlimited)
	if len(tasks) == 0 {
		return nil
	}
	accept := func() {
		atomic.StoreInt32(&r.rebalance, 0)
		log.Info("schedulerv3: manual rebalance request accepted")
	}
	return []*ScheduleTask{{
		BurstBalance: &BurstBalance{MoveChangefeeds: tasks},
		Accept:       accept,
	}}
}
