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

package maintainer

import (
	"context"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/new_arch/scheduler"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
)

type Maintainer struct {
	upstreamManager *upstream.Manager
	cfg             *config.SchedulerConfig
	globalVars      *vars.GlobalVars
	ID              model.ChangeFeedID

	info   *model.ChangeFeedInfo
	status *model.ChangeFeedStatus

	task *dispatchMaintainerTask

	componentStatus scheduler.ComponentStatus
	changefeed      owner.Changefeed
}

type dispatchTaskStatus int32

const (
	dispatchTaskReceived = dispatchTaskStatus(iota + 1)
	dispatchTaskProcessed
)

type dispatchMaintainerTask struct {
	ID        model.ChangeFeedID
	IsRemove  bool
	IsPrepare bool
	status    dispatchTaskStatus
}

func NewMaintainer(
	ID model.ChangeFeedID,
	upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	globalVars *vars.GlobalVars,
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus) *Maintainer {
	m := &Maintainer{
		upstreamManager: upstreamManager,
		cfg:             cfg,
		globalVars:      globalVars,
		ID:              ID,
		info:            info,
		status:          status,
		componentStatus: scheduler.ComponentStatusAbsent,
	}
	return m
}

func (m *Maintainer) registerHandler() {
	_, _ = m.globalVars.MessageServer.SyncAddHandler(context.Background(),
		new_arch.GetChangefeedMaintainerTopic(m.ID),
		&new_arch.Message{}, func(sender string, messageI interface{}) error {
			message := messageI.(*new_arch.Message)
			m.HandleMessage(sender, message)
			return nil
		})
}

func (m *Maintainer) HandleMessage(send string, msg *new_arch.Message) {
	if msg.AddTableRangeMaintainerResponse != nil {
	}
}

func (m *Maintainer) SendMessage(ctx context.Context, capture string, topic string, msg *new_arch.Message) error {
	client := m.globalVars.MessageRouter.GetClient(capture)
	_, err := client.TrySendMessage(ctx, topic, msg)
	return errors.Trace(err)
}

func (m *Maintainer) ScheduleTableRangeManager(ctx context.Context) error {
	//load tables
	//stream, _ := m.upstreamManager.GetDefaultUpstream()
	//meta := kv.GetSnapshotMeta(stream.KVStorage, m.status.CheckpointTs)
	//filter, err := pfilter.NewFilter(m.info.Config, "")
	//snap, err := schema.NewSingleSnapshotFromMeta(
	//	model.DefaultChangeFeedID(m.info.ID),
	//	meta, m.status.CheckpointTs, m.info.Config.ForceReplicate, filter)
	//if err != nil {
	//	return errors.Trace(err)
	//}
	//
	//// list all tables
	//res := make([]model.TableID, 0)
	//snap.IterTables(true, func(tableInfo *model.TableInfo) {
	//	if filter.ShouldIgnoreTable(tableInfo.TableName.Schema, tableInfo.TableName.Table) {
	//		return
	//	}
	//	// Sequence is not supported yet, TiCDC needs to filter all sequence tables.
	//	// See https://github.com/pingcap/tiflow/issues/4559
	//	if tableInfo.IsSequence() {
	//		return
	//	}
	//	if pi := tableInfo.GetPartitionInfo(); pi != nil {
	//		for _, partition := range pi.Definitions {
	//			res = append(res, partition.ID)
	//		}
	//	} else {
	//		res = append(res, tableInfo.ID)
	//	}
	//})
	//// todo: load balance table id to table range maintainer
	//captures := m.globalVars.CaptureManager.GetCaptures()
	//var tableIDGroups = make([][]model.TableID, len(captures))
	//for range captures {
	//	tableIDGroups = append(tableIDGroups, make([]model.TableID, 0))
	//}
	//for idx, tableID := range res {
	//	tableIDGroups[idx%len(captures)] = append(tableIDGroups[idx%len(captures)], tableID)
	//}

	return nil
}
func (m *Maintainer) getAndUpdateTableSpanState(old scheduler.ComponentStatus) (scheduler.ComponentStatus, bool) {
	return m.componentStatus, m.componentStatus != old
}

func (m *Maintainer) initChangefeed() (bool, error) {
	m.registerHandler()
	up, _ := m.upstreamManager.GetDefaultUpstream()
	//state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID, m.ID)
	//manager := owner.NewFeedStateManager(up, state)
	var manager owner.FeedStateManager
	m.changefeed = owner.NewChangefeed(m.ID, m.info, m.status, manager, up, m.cfg, m.globalVars)
	//check change initialization ?
	//m.changefeed.initial =
	//m.componentStatus = scheduler.ComponentStatusPreparing

	m.componentStatus = scheduler.ComponentStatusPrepared
	return true, nil
}

func (m *Maintainer) finishAddChangefeed() bool {
	m.componentStatus = scheduler.ComponentStatusWorking
	//start to tick
	//m.changefeed.Tick()
	return true
}

func (m *Maintainer) CloseChangefeed() {
	if m.componentStatus != scheduler.ComponentStatusStopping &&
		m.componentStatus != scheduler.ComponentStatusStopped {
		m.componentStatus = scheduler.ComponentStatusStopping
		// async close
		go func() {
			//m.changefeed.Close(context.Background())
			m.componentStatus = scheduler.ComponentStatusStopped
		}()
	}
}

func (m *Maintainer) isRemoveFinished() bool {
	m.componentStatus = scheduler.ComponentStatusWorking
	//start to tick
	//m.changefeed.Tick()
	return true
}

func (m *Maintainer) handleAddTableTask() error {
	state, _ := m.getAndUpdateTableSpanState(m.componentStatus)
	changed := true
	for changed {
		switch state {
		case scheduler.ComponentStatusAbsent:
			done, err := m.initChangefeed()
			if err != nil || !done {
				log.Warn("schedulerv3: agent add table failed",
					zap.Any("changefeed", m.ID),
					zap.Error(err))
				return errors.Trace(err)
			}
			state, changed = m.getAndUpdateTableSpanState(m.componentStatus)
		case scheduler.ComponentStatusWorking:
			log.Info("schedulerv3: table is replicating")
			m.task = nil
			return nil
		case scheduler.ComponentStatusPrepared:
			if m.task.IsPrepare {
				// `prepared` is a stable state, if the task was to prepare the table.
				log.Info("schedulerv3: table is prepared",
					zap.String("changefeed", m.ID.ID))
				m.task = nil
				return nil
			}

			if m.task.status == dispatchTaskReceived {
				//done, err := t.executor.AddTableSpan(ctx, t.task.Span, t.task.Checkpoint, false)
				//if err != nil || !done {
				//	log.Warn("schedulerv3: agent add table failed",
				//		zap.String("namespace", t.changefeedID.Namespace),
				//		zap.String("changefeed", t.changefeedID.ID),
				//		zap.Any("tableSpan", t.span), zap.Stringer("state", state),
				//		zap.Error(err))
				//	status := t.getTableSpanStatus(false)
				//	return newAddTableResponseMessage(status), errors.Trace(err)
				//}
				m.task.status = dispatchTaskProcessed
			}

			// coordinator send start changefeed command
			done := m.finishAddChangefeed()
			if !done {
				//return newAddTableResponseMessage(t.getTableSpanStatus(false)), nil
				return nil
			}
			//state, changed = t.getAndUpdateTableSpanState()
			return nil
		case scheduler.ComponentStatusPreparing:
			// `preparing` is not stable state and would last a long time,
			// it's no need to return such a state, to make the coordinator become burdensome.
			//done := t.executor.IsAddTableSpanFinished(t.task.Span, t.task.IsPrepare)
			//if !done {
			//	return nil, nil
			//}
			//state, changed = t.getAndUpdateTableSpanState()
			//log.Info("schedulerv3: add table finished",
			//	zap.String("namespace", t.changefeedID.Namespace),
			//	zap.String("changefeed", t.changefeedID.ID),
			//	zap.Any("tableSpan", t.span), zap.Stringer("state", state))
			return nil
		case scheduler.ComponentStatusStopping,
			scheduler.ComponentStatusStopped:
			log.Warn("schedulerv3: ignore add table")
			m.task = nil
			return nil
		default:
			log.Panic("schedulerv3: unknown table state")
		}
	}
	return nil
}

func (m *Maintainer) handleRemoveTableTask() error {
	state, _ := m.getAndUpdateTableSpanState(m.componentStatus)
	changed := true
	for changed {
		switch state {
		case scheduler.ComponentStatusAbsent:
			log.Warn("schedulerv3: remove table, but table is absent")
			m.task = nil
			return nil
		case scheduler.ComponentStatusStopping, // stopping now is useless
			scheduler.ComponentStatusStopped:
			// release table resource, and get the latest checkpoint
			// this will let the table span become `absent`
			//checkpointTs, done := t.executor.IsRemoveTableSpanFinished(t.span)
			//if !done {
			//	// actually, this should never be hit, since we know that table is stopped.
			//	status := t.getTableSpanStatus(false)
			//	status.State = tablepb.TableStateStopping
			//	return newRemoveTableResponseMessage(status)
			//}
			log.Warn("schedulerv3: remove table, but table is stopping")
			m.task = nil
			//status := t.getTableSpanStatus(false)
			//status.State = tablepb.TableStateStopped
			//status.Checkpoint.CheckpointTs = checkpointTs
			//return newRemoveTableResponseMessage(status)
			return nil
		case scheduler.ComponentStatusPreparing,
			scheduler.ComponentStatusPrepared,
			scheduler.ComponentStatusWorking:
			m.CloseChangefeed()
			state, changed = m.getAndUpdateTableSpanState(m.componentStatus)
		default:
			log.Panic("schedulerv3: unknown table state")
		}
	}
	return nil
}

func (m *Maintainer) injectDispatchTableTask(task *dispatchMaintainerTask) {
	if m.ID != task.ID {
		log.Panic("schedulerv3: tableID not match",
			zap.String("changefeed", m.ID.ID),
			zap.Any("tableSpan", task.ID))
	}
	if m.task == nil {
		log.Info("schedulerv3: table found new task",
			zap.Any("task", task))
		m.task = task
		return
	}
	log.Warn("schedulerv3: table inject dispatch table task ignored,"+
		"since there is one not finished yet",
		zap.Any("nowTask", m.task),
		zap.Any("ignoredTask", task))
}

func (m *Maintainer) getStatus() *new_arch.ChangefeedStatus {
	return &new_arch.ChangefeedStatus{
		ID:              m.ID,
		ComponentStatus: int(m.componentStatus),
		CheckpointTs:    0,
		//CheckpointTs:    m.status.CheckpointTs,
	}
}
