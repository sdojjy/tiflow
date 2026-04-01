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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
	"time"
)

// Manager manages schedulers and generates schedule tasks.
type Manager struct { //nolint:revive
	schedulers         []Scheduler
	tasksCounter       map[struct{ scheduler, task string }]int
	maxTaskConcurrency int
}

// NewSchedulerManager returns a new scheduler manager.
func NewSchedulerManager(cfg *config.SchedulerConfig) *Manager {
	sm := &Manager{
		maxTaskConcurrency: cfg.MaxTaskConcurrency,
		schedulers:         make([]Scheduler, schedulerPriorityMax),
		tasksCounter: make(map[struct {
			scheduler string
			task      string
		}]int),
	}

	sm.schedulers[schedulerPriorityBasic] = newBasicScheduler(
		cfg.AddTableBatchSize)
	sm.schedulers[schedulerPriorityDrainCapture] = newDrainCaptureScheduler(
		cfg.MaxTaskConcurrency)
	sm.schedulers[schedulerPriorityBalance] = newBalanceScheduler(
		time.Duration(cfg.CheckBalanceInterval), cfg.MaxTaskConcurrency)
	sm.schedulers[schedulerPriorityMoveTable] = newMoveTableScheduler()
	sm.schedulers[schedulerPriorityRebalance] = newRebalanceScheduler()
	return sm
}

// Schedule generates schedule tasks based on the inputs.
func (sm *Manager) Schedule(
	currentChangefeeds []*model.ChangeFeedInfo,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	replications map[model.ChangeFeedID]*changefeed,
	runTasking map[model.ChangeFeedID]*ScheduleTask,
) []*ScheduleTask {
	for sid, scheduler := range sm.schedulers {
		// Basic scheduler bypasses max task check, because it handles the most
		// critical scheduling, e.g. add table via CREATE TABLE DDL.
		if sid != int(schedulerPriorityBasic) {
			if len(runTasking) >= sm.maxTaskConcurrency {
				// Do not generate more scheduling tasks if there are too many
				// running tasks.
				return nil
			}
		}
		tasks := scheduler.Schedule(currentChangefeeds, aliveCaptures, replications)
		for _, t := range tasks {
			name := struct {
				scheduler, task string
			}{scheduler: scheduler.Name(), task: t.Name()}
			sm.tasksCounter[name]++
		}
		if len(tasks) != 0 {
			return tasks
		}
	}
	return nil
}

// MoveTable moves a table to the target capture.
func (sm *Manager) MoveTable(changeFeedID model.ChangeFeedID, target model.CaptureID) {
	scheduler := sm.schedulers[schedulerPriorityMoveTable]
	moveTableScheduler, ok := scheduler.(*moveTableScheduler)
	if !ok {
		log.Panic("schedulerv3: invalid move table scheduler found")
	}
	if !moveTableScheduler.addTask(changeFeedID, target) {
		log.Info("schedulerv3: manual move Table task ignored, "+
			"since the last triggered task not finished",
			zap.String("changeFeedID", changeFeedID.String()),
			zap.String("targetCapture", target))
	}
}
