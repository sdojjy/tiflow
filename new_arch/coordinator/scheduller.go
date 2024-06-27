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
	"github.com/pingcap/tiflow/new_arch/scheduler"
	"go.uber.org/zap"
)

// schedulerPriority is the priority of each scheduler.
// Lower value has higher priority.
type schedulerPriority int

const (
	// schedulerPriorityBasic has the highest priority.
	schedulerPriorityBasic schedulerPriority = iota
	// schedulerPriorityDrainCapture has higher priority than other schedulers.
	schedulerPriorityDrainCapture
	schedulerPriorityMoveTable
	schedulerPriorityRebalance
	schedulerPriorityBalance
	schedulerPriorityMax
)

// Callback is invoked when something is done.
type Callback func()

type Scheduler interface {
	Name() string
	Schedule(
		currentChangefeeds []*model.ChangeFeedInfo,
		aliveCaptures map[model.CaptureID]*CaptureStatus,
		replications map[model.ChangeFeedID]*changefeed,
	) []*ScheduleTask
}

// ScheduleTask is a schedule task that wraps add/move/remove table tasks.
type ScheduleTask struct { //nolint:revive
	MoveChangefeed   *MoveChangefeed
	AddChangefeed    *AddChangefeed
	RemoveChangefeed *RemoveChangefeed
	BurstBalance     *BurstBalance

	Accept Callback
}

func (t *ScheduleTask) Name() string {
	return "ScheduleTask"
}

// MoveChangefeed is a schedule task for moving a changefeed.
type MoveChangefeed struct {
	Changefeed  model.ChangeFeedID
	DestCapture model.CaptureID
}

// AddChangefeed is a schedule task for adding a changefeed.
type AddChangefeed struct {
	ChangeFeedID model.ChangeFeedID
	CaptureID    model.CaptureID
	Info         *model.ChangeFeedInfo
	Status       *model.ChangeFeedStatus
}

type RemoveChangefeed struct {
	Changefeed model.ChangeFeedID
	CaptureID  model.CaptureID
}

// BurstBalance for changefeed set up or unplanned TiCDC node failure.
// TiCDC needs to balance interrupted tables as soon as possible.
type BurstBalance struct {
	AddChangefeeds    []AddChangefeed
	RemoveChangefeeds []RemoveChangefeed
	MoveChangefeeds   []MoveChangefeed
}

var _ Scheduler = &basicScheduler{}

// The basic scheduler for adding and removing tables, it tries to keep
// every table get replicated.
//
// It handles the following scenario:
// 1. Initial table dispatch.
// 2. DDL CREATE/DROP/TRUNCATE TABLE
// 3. Capture offline.
type basicScheduler struct {
	batchSize int
}

func newBasicScheduler(batchSize int) *basicScheduler {
	return &basicScheduler{
		batchSize: batchSize,
	}
}

func (b *basicScheduler) Name() string {
	return "basic-scheduler"
}

func (b *basicScheduler) Schedule(
	currentChangefeeds []*model.ChangeFeedInfo,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	replications map[model.ChangeFeedID]*changefeed,
) []*ScheduleTask {
	tasks := make([]*ScheduleTask, 0)
	tablesLenEqual := len(currentChangefeeds) == len(replications)
	tablesAllFind := true
	newChangefeeds := make([]*changefeed, 0)
	for _, cf := range currentChangefeeds {
		if len(newChangefeeds) >= b.batchSize {
			break
		}
		rep, ok := replications[model.DefaultChangeFeedID(cf.ID)]
		if !ok {
			newChangefeeds = append(newChangefeeds, &changefeed{
				primary:       "",
				ID:            model.DefaultChangeFeedID(cf.ID),
				Info:          cf,
				scheduleState: scheduler.SchedulerComponentStatusAbsent,
				Captures:      make(map[model.CaptureID]Role),
			})
			// The table ID is not in the replication means the two sets are
			// not identical.
			tablesAllFind = false
			continue
		}
		if rep.scheduleState == scheduler.SchedulerComponentStatusAbsent {
			newChangefeeds = append(newChangefeeds, &changefeed{
				primary:       "",
				ID:            model.DefaultChangeFeedID(cf.ID),
				Info:          cf,
				scheduleState: scheduler.SchedulerComponentStatusAbsent,
				Captures:      make(map[model.CaptureID]Role),
			})
		}
	}

	// Build add table tasks.
	if len(newChangefeeds) > 0 {
		captureIDs := make([]model.CaptureID, 0, len(aliveCaptures))
		for captureID, status := range aliveCaptures {
			if status.State == CaptureStateStopping {
				log.Warn("schedulerv3: capture is stopping, "+
					"skip the capture when add new table",
					zap.Any("captureStatus", status))
				continue
			}
			captureIDs = append(captureIDs, captureID)
		}

		if len(captureIDs) == 0 {
			// this should never happen, if no capture can be found
			// the changefeed cannot make progress
			// for a cluster with n captures, n should be at least 2
			// only n - 1 captures can be in the `stopping` at the same time.
			log.Warn("schedulerv3: cannot found capture when add new table",
				zap.Any("allCaptureStatus", aliveCaptures))
			return tasks
		}
		tasks = append(
			tasks, newBurstAddTables(newChangefeeds, captureIDs))
	}

	// Build remove table tasks.
	// For most of the time, remove tables are unlikely to happen.
	//
	// Fast path for check whether two sets are identical:
	// If the length of currentTables and replications are equal,
	// and for all tables in currentTables have a record in replications.
	if !tablesLenEqual || !tablesAllFind {
		// The two sets are not identical. We need to build a map to find removed tables.
		intersectionTable := make(map[model.ChangeFeedID]struct{})
		for _, cf := range currentChangefeeds {
			_, ok := replications[model.DefaultChangeFeedID(cf.ID)]
			if ok {
				intersectionTable[model.DefaultChangeFeedID(cf.ID)] = struct{}{}
			}
		}
		rmSpans := make([]*changefeed, 0)
		for _, cf := range replications {
			_, ok := intersectionTable[cf.ID]
			if !ok {
				rmSpans = append(rmSpans, cf)
			}
		}
		removeTableTasks := newBurstRemoveTables(rmSpans, replications)
		if removeTableTasks != nil {
			tasks = append(tasks, removeTableTasks)
		}
	}
	return tasks
}

// newBurstAddTables add each new table to captures in a round-robin way.
func newBurstAddTables(newChangefeeds []*changefeed, captureIDs []model.CaptureID,
) *ScheduleTask {
	idx := 0
	changefeeds := make([]AddChangefeed, 0, len(newChangefeeds))
	for _, cf := range newChangefeeds {
		targetCapture := captureIDs[idx]
		changefeeds = append(changefeeds, AddChangefeed{
			ChangeFeedID: cf.ID,
			CaptureID:    targetCapture,
			Info:         cf.Info,
			Status:       cf.Status,
		})
		//log.Info("schedulerv3: burst add changefeed",
		//	zap.String("changefeed", cf.ID.ID),
		//	zap.String("captureID", targetCapture))

		idx++
		if idx >= len(captureIDs) {
			idx = 0
		}
	}
	return &ScheduleTask{
		BurstBalance: &BurstBalance{
			AddChangefeeds: changefeeds,
		},
	}
}

func newBurstRemoveTables(
	rmChangefeeds []*changefeed,
	currentChanfeeds map[model.ChangeFeedID]*changefeed,
) *ScheduleTask {
	changefeeds := make([]RemoveChangefeed, 0, len(rmChangefeeds))
	for _, cf := range rmChangefeeds {
		ccf := currentChanfeeds[cf.ID]
		var captureID model.CaptureID = ccf.primary

		if ccf.primary == "" {
			log.Warn("schedulerv3: primary or secondary not found for removed table,"+
				"this may happen if the capture shutdown",
				zap.Any("changefeed", cf))
			continue
		}
		changefeeds = append(changefeeds, RemoveChangefeed{
			Changefeed: cf.ID,
			CaptureID:  captureID,
		})
		log.Info("schedulerv3: burst remove table",
			zap.String("captureID", captureID),
			zap.Any("tableID", cf.ID))
	}

	if len(changefeeds) == 0 {
		return nil
	}

	return &ScheduleTask{
		BurstBalance: &BurstBalance{
			RemoveChangefeeds: changefeeds,
		},
	}
}
