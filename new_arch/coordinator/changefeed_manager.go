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
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/new_arch/scheduller"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type ChangefeedManager struct {
	runningTasks map[model.ChangeFeedID]*ScheduleTask
	changefeeds  map[model.ChangeFeedID]*changefeed

	maxTaskConcurrency int
}

func NewChangefeedManager(maxTaskConcurrency int) *ChangefeedManager {
	m := &ChangefeedManager{
		maxTaskConcurrency: maxTaskConcurrency,
		changefeeds:        make(map[model.ChangeFeedID]*changefeed),
		runningTasks:       make(map[model.ChangeFeedID]*ScheduleTask),
	}
	return m
}

func (r *ChangefeedManager) HandleTasks(tasks []*ScheduleTask) ([]*new_arch.Message, error) {
	// Check if a running task is finished.
	var toBeDeleted []model.ChangeFeedID
	for cfID, _ := range r.runningTasks {
		if cf, ok := r.changefeeds[cfID]; ok {
			// If table is back to Replicating or Removed,
			// the running task is finished.
			if cf.scheduleState == scheduller.SchedulerComponentStatusWorking || cf.hasRemoved() {
				toBeDeleted = append(toBeDeleted, cfID)
			}
		} else {
			// No table found, remove the task
			toBeDeleted = append(toBeDeleted, cfID)
		}
	}
	for _, span := range toBeDeleted {
		delete(r.runningTasks, span)
	}

	sentMsgs := make([]*new_arch.Message, 0)
	for _, task := range tasks {
		// Burst balance does not affect by maxTaskConcurrency.
		if task.BurstBalance != nil {
			msgs, err := r.handleBurstBalanceTasks(task.BurstBalance)
			if err != nil {
				return nil, errors.Trace(err)
			}
			sentMsgs = append(sentMsgs, msgs...)
			if task.Accept != nil {
				task.Accept()
			}
			continue
		}

		// Check if accepting one more task exceeds maxTaskConcurrency.
		if len(r.runningTasks) == r.maxTaskConcurrency {
			log.Debug("schedulerv3: too many running task")
			// Does not use break, in case there is burst balance task
			// in the remaining tasks.
			continue
		}

		var changefeedID model.ChangeFeedID
		if task.AddChangefeed != nil {
			changefeedID = task.AddChangefeed.Changefeed
		} else if task.RemoveChangefeed != nil {
			changefeedID = task.RemoveChangefeed.Changefeed
		} else if task.MoveChangefeed != nil {
			changefeedID = task.MoveChangefeed.Changefeed
		}

		// Skip task if the table is already running a task,
		// or the table has removed.
		if _, ok := r.runningTasks[changefeedID]; ok {
			log.Info("schedulerv3: ignore task, already exists",
				zap.Any("task", task))
			continue
		}
		if _, ok := r.changefeeds[changefeedID]; !ok && task.AddChangefeed == nil {
			log.Info("schedulerv3: ignore task, table not found",
				zap.Any("task", task))
			continue
		}

		var msgs []*new_arch.Message
		var err error
		if task.AddChangefeed != nil {
			msgs, err = r.handleAddTableTask(task.AddChangefeed)
		} else if task.RemoveChangefeed != nil {
			msgs, err = r.handleRemoveTableTask(task.RemoveChangefeed)
		} else if task.MoveChangefeed != nil {
			msgs, err = r.handleRemoveTableTask(task.RemoveChangefeed)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		r.runningTasks[changefeedID] = task
		if task.Accept != nil {
			task.Accept()
		}
	}
	return sentMsgs, nil
}

func (r *ChangefeedManager) handleBurstBalanceTasks(
	task *BurstBalance,
) ([]*new_arch.Message, error) {
	perCapture := make(map[model.CaptureID]int)
	for _, task := range task.AddChangefeeds {
		perCapture[task.CaptureID]++
	}
	for _, task := range task.RemoveChangefeeds {
		perCapture[task.CaptureID]++
	}
	fields := make([]zap.Field, 0)
	for captureID, count := range perCapture {
		fields = append(fields, zap.Int(captureID, count))
	}
	fields = append(fields, zap.Int("addTable", len(task.AddChangefeeds)))
	fields = append(fields, zap.Int("removeTable", len(task.RemoveChangefeeds)))
	fields = append(fields, zap.Int("moveTable", len(task.MoveChangefeeds)))
	log.Info("schedulerv3: handle burst balance task", fields...)

	sentMsgs := make([]*new_arch.Message, 0, len(task.AddChangefeeds))
	for i := range task.AddChangefeeds {
		addTable := task.AddChangefeeds[i]
		if _, ok := r.runningTasks[addTable.Changefeed]; ok {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleAddTableTask(&addTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding. it's in adding status, so we can filter the new task using running tasks
		r.runningTasks[addTable.Changefeed] = &ScheduleTask{}
	}
	for i := range task.RemoveChangefeeds {
		removeTable := task.RemoveChangefeeds[i]
		if _, ok := r.runningTasks[removeTable.Changefeed]; ok {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleRemoveTableTask(&removeTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding.
		r.runningTasks[removeTable.Changefeed] = &ScheduleTask{}
	}
	for i := range task.MoveChangefeeds {
		moveTable := task.MoveChangefeeds[i]
		if _, ok := r.runningTasks[moveTable.Changefeed]; ok {
			// Skip add table if the table is already running a task.
			continue
		}
		msgs, err := r.handleMoveChangefeedTask(&moveTable)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sentMsgs = append(sentMsgs, msgs...)
		// Just for place holding.
		r.runningTasks[moveTable.Changefeed] = &ScheduleTask{}
	}
	return sentMsgs, nil
}

func (r *ChangefeedManager) handleRemoveTableTask(
	task *RemoveChangefeed,
) ([]*new_arch.Message, error) {
	table, _ := r.changefeeds[task.Changefeed]
	if table.hasRemoved() {
		log.Info("schedulerv3: changefeed has removed",
			zap.String("changefeed", table.ID.ID))
		delete(r.changefeeds, task.Changefeed)
		return nil, nil
	}
	return table.handleRemoveChangefeed()
}

func (r *ChangefeedManager) handleMoveChangefeedTask(
	task *MoveChangefeed,
) ([]*new_arch.Message, error) {
	table, _ := r.changefeeds[task.Changefeed]
	return table.handleMoveTable(task.DestCapture)
}

func (r *ChangefeedManager) handleAddTableTask(
	task *AddChangefeed,
) ([]*new_arch.Message, error) {
	table, ok := r.changefeeds[task.Changefeed]
	if !ok {
		table = &changefeed{
			maintainerCaptureID: task.CaptureID,
			standbyCaptureID:    "",
			Captures:            make(map[model.CaptureID]Role),
			ID:                  task.Changefeed,
			Info:                nil,
			Status:              nil,
			state:               "",
			errors:              nil,
			maintainerStatus:    "",
			coordinator:         nil,
			scheduleState:       0,
		}
		r.changefeeds[task.Changefeed] = table
	}
	return table.handleAddTable(task.CaptureID)
}