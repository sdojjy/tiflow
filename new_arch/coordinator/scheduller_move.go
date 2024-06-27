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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

var _ Scheduler = &moveTableScheduler{}

type moveTableScheduler struct {
	mu    sync.Mutex
	tasks map[model.ChangeFeedID]*ScheduleTask
}

func newMoveTableScheduler() *moveTableScheduler {
	return &moveTableScheduler{
		tasks: make(map[model.ChangeFeedID]*ScheduleTask),
	}
}

func (m *moveTableScheduler) Name() string {
	return "move-table-scheduler"
}

func (m *moveTableScheduler) addTask(changefeedID model.ChangeFeedID, target model.CaptureID) bool {
	// previous triggered task not accepted yet, decline the new manual move table request.
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[changefeedID]; ok {
		return false
	}
	m.tasks[changefeedID] = &ScheduleTask{
		MoveChangefeed: &MoveChangefeed{
			Changefeed:  changefeedID,
			DestCapture: target,
		},
		Accept: func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			delete(m.tasks, changefeedID)
		},
	}
	return true
}

func (m *moveTableScheduler) Schedule(
	currentChangefeeds []*model.ChangeFeedInfo,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	replications map[model.ChangeFeedID]*changefeed,
) []*ScheduleTask {
	m.mu.Lock()
	defer m.mu.Unlock()

	// FIXME: moveTableScheduler is broken in the sense of range level replication.
	// It is impossible for users to pass valid start key and end key.

	result := make([]*ScheduleTask, 0)

	if len(m.tasks) == 0 {
		return result
	}

	if len(aliveCaptures) == 0 {
		return result
	}

	allSpans := make(map[string]*model.ChangeFeedInfo)
	for _, span := range currentChangefeeds {
		allSpans[span.ID] = span
	}

	toBeDeleted := []model.ChangeFeedID{}
	//filter tasks that can not be processed
	for changefeedID, task := range m.tasks {
		_, ok := allSpans[changefeedID.ID]
		// table may not in the all current tables
		// if it was removed after manual move table triggered.
		if !ok {
			log.Warn("schedulerv3: move changefeed ignored, since the changefeed cannot found",
				zap.String("changefeed", changefeedID.ID),
				zap.String("captureID", task.MoveChangefeed.DestCapture))
			toBeDeleted = append(toBeDeleted, changefeedID)
			continue
		}

		// the target capture may offline after manual move table triggered.
		status, ok := aliveCaptures[task.MoveChangefeed.DestCapture]
		if !ok {
			log.Info("schedulerv3: move table ignored, since the target capture cannot found",
				zap.String("changefeed", changefeedID.ID),
				zap.String("captureID", task.MoveChangefeed.DestCapture))
			toBeDeleted = append(toBeDeleted, changefeedID)
			continue
		}
		if status.State != CaptureStateInitialized {
			log.Warn("schedulerv3: move table ignored, target capture is not initialized",
				zap.String("changefeed", changefeedID.ID),
				zap.String("captureID", task.MoveChangefeed.DestCapture),
				zap.Any("state", status.State))
			toBeDeleted = append(toBeDeleted, changefeedID)
			continue
		}

		rep, ok := replications[changefeedID]
		if !ok {
			log.Warn("schedulerv3: move table ignored, table not found in the replication set",
				zap.String("changefeed", changefeedID.ID),
				zap.String("captureID", task.MoveChangefeed.DestCapture))
			toBeDeleted = append(toBeDeleted, changefeedID)
			continue
		}
		// only move replicating table.
		if rep.scheduleState != scheduler.SchedulerComponentStatusWorking {
			log.Info("schedulerv3: move table ignored, since the table is not replicating now",
				zap.String("changefeed", changefeedID.ID),
				zap.String("captureID", task.MoveChangefeed.DestCapture))
			toBeDeleted = append(toBeDeleted, changefeedID)
		}

		result = append(result, task)
	}
	for _, span := range toBeDeleted {
		delete(m.tasks, span)
	}
	return result
}
