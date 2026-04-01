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
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/new_arch/scheduler"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Role is the role of a capture.
type Role int

const (
	// RolePrimary primary role.
	RolePrimary = 1
	// RoleSecondary secondary role.
	RoleSecondary = 2
	// RoleUndetermined means that we don't know its state, it may be
	// replicating, stopping or stopped.
	RoleUndetermined = 3
)

type changefeed struct {
	primary model.CaptureID

	// Captures is a map of captures that has the table replica.
	// NB: Invariant, 1) at most one primary, 2) primary capture must be in
	//     CaptureRolePrimary.
	Captures map[model.CaptureID]Role

	ID     model.ChangeFeedID
	Info   *model.ChangeFeedInfo
	Status *model.ChangeFeedStatus

	state        model.FeedState
	checkpointTs atomic.Uint64
	errors       map[model.CaptureID]changefeedError

	maintainerStatus string
	coordinator      *coordinatorImpl

	scheduleState scheduler.SchedulerComponentStatus
}

// newChangefeed returns a new replication set.
func newChangefeed(
	id model.ChangeFeedID,
	tableStatus map[model.CaptureID]*ChangefeedStatus,
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus,
) (*changefeed, error) {
	r := &changefeed{
		ID:       id,
		Captures: make(map[string]Role),
		Info:     info,
		Status:   status,
	}
	// Count of captures that is in Stopping states.
	stoppingCount := 0
	committed := false
	for captureID, table := range tableStatus {
		if r.ID != table.ChangefeedID {
			return nil, errors.New("schedulerv3: table id inconsistent")
		}
		r.update(table)

		switch table.ComponentStatus {
		case scheduler.ComponentStatusWorking:
			if len(r.primary) != 0 {
				return nil, errors.New("schedulerv3: multiple primary")
			}
			// Recognize primary if it's table is in replicating state.
			err := r.setCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, errors.Trace(err)
			}
			err = r.promoteSecondary(captureID)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case scheduler.ComponentStatusPreparing:
			// Recognize secondary if it's table is in preparing state.
			err := r.setCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case scheduler.ComponentStatusPrepared:
			// Recognize secondary and Commit state if it's table is in prepared state.
			committed = true
			err := r.setCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case scheduler.ComponentStatusStopping:
			// The capture is stopping the table. It is possible that the
			// capture is primary, and is still replicating data to downstream.
			// We need to wait its state becomes Stopped or Absent before
			// proceeding further scheduling.
			log.Warn("schedulerv3: found a stopping capture during initializing",
				zap.Any("replicationSet", r),
				zap.Any("status", tableStatus))
			err := r.setCapture(captureID, RoleUndetermined)
			if err != nil {
				return nil, errors.Trace(err)
			}
			stoppingCount++
		case scheduler.ComponentStatusAbsent,
			scheduler.ComponentStatusStopped:
			// Ignore stop state.
		default:
			log.Warn("schedulerv3: unknown table state",
				zap.Any("replicationSet", r),
				zap.Any("status", tableStatus))
		}
	}

	// Build state from primary, secondary and captures.
	if len(r.primary) != 0 {
		r.scheduleState = scheduler.SchedulerComponentStatusWorking
	}
	// Move table or add table is in-progress.
	if r.hasRole(RoleSecondary) {
		r.scheduleState = scheduler.SchedulerComponentStatusPrepare
	}
	// Move table or add table is committed.
	if committed {
		r.scheduleState = scheduler.SchedulerComponentStatusCommit
	}
	if len(r.Captures) == 0 {
		r.scheduleState = scheduler.SchedulerComponentStatusAbsent
	}
	if len(r.Captures) == stoppingCount {
		r.scheduleState = scheduler.SchedulerComponentStatusRemoving
	}
	log.Info("schedulerv3: initialize replication set",
		zap.Any("replicationSet", r))

	return r, nil
}

func (c *changefeed) Stop(ctx context.Context) error {
	err := c.coordinator.SendMessage(ctx, c.primary, new_arch.GetChangefeedMaintainerManagerTopic(),
		&new_arch.Message{
			DispatchMaintainerRequest: &new_arch.DispatchMaintainerRequest{
				RemoveMaintainerRequest: &new_arch.RemoveMaintainerRequest{
					ID: c.Info.ID,
				},
			}})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *changefeed) GetCheckpointTs(ctx context.Context) uint64 {
	return c.checkpointTs.Load()
}

func (c *changefeed) EmitCheckpointTs(ctx context.Context, uint642 uint64) error {
	return nil
}

func (c *changefeed) hasRemoved() bool {
	// It has been removed successfully if it's state is Removing,
	// and there is no capture has it.
	return c.scheduleState == scheduler.SchedulerComponentStatusRemoving &&
		len(c.Captures) == 0
}

func (c *changefeed) pollOnAbsent(
	input *ChangefeedStatus, captureID model.CaptureID) (bool, error) {
	switch input.ComponentStatus {
	case scheduler.ComponentStatusAbsent:
		c.scheduleState = scheduler.SchedulerComponentStatusPrepare
		err := c.setCapture(captureID, RoleSecondary)
		return true, errors.Trace(err)

	case scheduler.ComponentStatusStopped:
		// Ignore stopped table state as a capture may shutdown unexpectedly.
		return false, nil
	case scheduler.ComponentStatusPreparing,
		scheduler.ComponentStatusPrepared,
		scheduler.ComponentStatusWorking,
		scheduler.ComponentStatusStopping:
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.String("changefeed", c.ID.ID),
		zap.String("captureID", captureID))
	return false, nil
}

func (c *changefeed) pollOnPrepare(
	input *ChangefeedStatus, captureID model.CaptureID) (*new_arch.Message, bool, error) {
	switch input.ComponentStatus {
	case scheduler.ComponentStatusAbsent:
		if c.isInRole(captureID, RoleSecondary) {
			return c.getAddChangefeedRequest(captureID, true), false, nil
		}
	case scheduler.ComponentStatusPreparing:
		if c.isInRole(captureID, RoleSecondary) {
			// Ignore secondary Preparing, it may take a long time.
			return nil, false, nil
		}
	case scheduler.ComponentStatusPrepared:
		if c.isInRole(captureID, RoleSecondary) {
			// Secondary is prepared, transit to Commit state.
			c.scheduleState = scheduler.SchedulerComponentStatusCommit
			return nil, true, nil
		}
	case scheduler.ComponentStatusWorking:
		if c.primary == captureID {
			c.update(input)
			return nil, false, nil
		}
	case scheduler.ComponentStatusStopping, scheduler.ComponentStatusStopped:
		if c.primary == captureID {
			// Primary is stopped, but we may still has secondary.
			// Clear primary and promote secondary when it's prepared.
			log.Info("schedulerv3: primary is stopped during Prepare",
				zap.String("changefeed", c.ID.ID),
				zap.Any("tableState", input),
				zap.String("captureID", captureID))
			c.clearPrimary()
			return nil, false, nil
		}
		if c.isInRole(captureID, RoleSecondary) {
			log.Info("schedulerv3: capture is stopped during Prepare",
				zap.String("changefeed", c.ID.ID),
				zap.Any("tableState", input),
				zap.String("captureID", captureID))
			err := c.clearCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			if c.primary != "" {
				// Secondary is stopped, and we still has primary.
				// Transit to Replicating.
				c.scheduleState = scheduler.SchedulerComponentStatusWorking
			} else {
				// Secondary is stopped, and we do not has primary.
				// Transit to Absent.
				c.scheduleState = scheduler.SchedulerComponentStatusAbsent
			}
			return nil, true, nil
		}
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.String("changefeed", c.ID.ID),
		zap.Any("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", c))
	return nil, false, nil
}

func (c *changefeed) pollOnReplicating(
	input *ChangefeedStatus, captureID model.CaptureID) (*new_arch.Message, bool, error) {
	switch input.ComponentStatus {
	case scheduler.ComponentStatusWorking:
		if c.primary == captureID {
			c.update(input)
			return nil, false, nil
		}
		return nil, false, errors.New("schedulerv3: multiple primary")

	case scheduler.ComponentStatusAbsent:
	case scheduler.ComponentStatusPreparing:
	case scheduler.ComponentStatusPrepared:
	case scheduler.ComponentStatusStopping:
	case scheduler.ComponentStatusStopped:
		if c.primary == captureID {
			c.update(input)
			// Primary is stopped, but we still has secondary.
			// Clear primary and promote secondary when it's prepared.
			log.Info("schedulerv3: primary is stopped during Replicating",
				zap.Any("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("replicationSet", c))
			c.clearPrimary()
			c.scheduleState = scheduler.SchedulerComponentStatusAbsent
			return nil, true, nil
		}
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.Any("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", c))
	return nil, false, nil
}

func (c *changefeed) getAddChangefeedRequest(capture string, secondary bool) *new_arch.Message {
	return &new_arch.Message{
		To: capture,
		DispatchMaintainerRequest: &new_arch.DispatchMaintainerRequest{
			AddMaintainerRequest: &new_arch.AddMaintainerRequest{
				ID:          c.ID,
				Config:      nil, //todo:
				Status:      c.Status,
				IsSecondary: secondary,
			}},
	}
}

func (c *changefeed) getRemoveChangefeedRequest(capture string) *new_arch.Message {
	return &new_arch.Message{
		To: capture,
		DispatchMaintainerRequest: &new_arch.DispatchMaintainerRequest{
			RemoveMaintainerRequest: &new_arch.RemoveMaintainerRequest{
				ID: c.Info.ID,
			},
		},
	}
}

func (c *changefeed) pollOnCommit(
	input *ChangefeedStatus, captureID model.CaptureID) (*new_arch.Message, bool, error) {
	switch input.ComponentStatus {
	case scheduler.ComponentStatusPrepared:
		if c.isInRole(captureID, RoleSecondary) {
			if c.primary != "" {
				// Secondary capture is prepared and waiting for stopping primary.
				// Send message to primary, ask for stopping.
				// 从primary 节点删除任务
				return c.getRemoveChangefeedRequest(c.primary), false, nil
			}
			if c.hasRole(RoleUndetermined) {
				// There are other captures that have the table.
				// Must waiting for other captures become stopped or absent
				// before promoting the secondary, otherwise there may be two
				// primary that write data and lead to data inconsistency.
				log.Info("schedulerv3: there are unknown captures during commit",
					zap.String("captureID", captureID))
				return nil, false, nil
			}
			// No primary, promote secondary to primary.
			err := c.promoteSecondary(captureID)
			if err != nil {
				return nil, false, errors.Trace(err)
			}

			log.Info("schedulerv3: promote secondary, no primary",
				zap.Any("tableState", input),
				zap.String("captureID", captureID))
		}
		// Secondary has been promoted, retry AddTableRequest.
		if c.primary == captureID && !c.hasRole(RoleSecondary) {
			return c.getAddChangefeedRequest(captureID, false), false, nil
		}

	case scheduler.ComponentStatusStopped, scheduler.ComponentStatusAbsent:
		if c.primary == captureID {
			// 停止前上报的状态，需要做最后一次处理
			c.update(input)
			original := c.primary
			c.clearPrimary()
			if !c.hasRole(RoleSecondary) {
				// If there is no secondary, transit to Absent.
				log.Info("schedulerv3: primary is stopped during Commit",
					zap.Any("tableState", input),
					zap.String("captureID", captureID),
					zap.Any("replicationSet", c))
				c.scheduleState = scheduler.SchedulerComponentStatusAbsent
				return nil, true, nil
			}
			// Primary is stopped, promote secondary to primary.
			secondary, _ := c.getRole(RoleSecondary)
			err := c.promoteSecondary(secondary)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			log.Info("schedulerv3: replication state promote secondary",
				zap.String("original", original),
				zap.String("captureID", secondary))
			// 发送消息给secondary 节点，开始真正工作
			return c.getAddChangefeedRequest(c.primary, false), false, nil
		} else if c.isInRole(captureID, RoleSecondary) {
			// As it sends RemoveTableRequest to the original primary
			// upon entering Commit state. Do not change state and wait
			// the original primary reports its table.
			log.Info("schedulerv3: secondary is stopped during Commit")
			err := c.clearCapture(captureID, RoleSecondary)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			if c.primary == "" {
				// If there is no primary, transit to Absent.
				c.scheduleState = scheduler.SchedulerComponentStatusAbsent
			}
			return nil, true, nil
		} else if c.isInRole(captureID, RoleUndetermined) {
			log.Info("schedulerv3: capture is stopped during Commit",
				zap.String("captureID", captureID),
				zap.Any("replicationSet", c))
			err := c.clearCapture(captureID, RoleUndetermined)
			return nil, false, errors.Trace(err)
		}

	case scheduler.ComponentStatusWorking:
		if c.primary == captureID {
			c.update(input)
			if c.hasRole(RoleSecondary) {
				// Original primary is not stopped, ask for stopping.
				// remove Table
				return c.getRemoveChangefeedRequest(captureID), false, nil
			}

			// There are three cases for empty secondary.
			//
			// 1. Secondary has promoted to primary, and the new primary is
			//    replicating, transit to Replicating.
			// 2. Secondary has shutdown during Commit, the original primary
			//    does not receives RemoveTable request and continues to
			//    replicate, transit to Replicating.
			// 3. Secondary has shutdown during Commit, we receives a message
			//    before the original primary receives RemoveTable request.
			//    Transit to Replicating, and wait for the next table state of
			//    the primary, Stopping or Stopped.
			c.scheduleState = scheduler.SchedulerComponentStatusWorking
			return nil, true, nil
		}
		return nil, false, errors.New("schedulerv3: multiple primary")

	case scheduler.ComponentStatusStopping:
		if c.primary == captureID && c.hasRole(RoleSecondary) {
			c.update(input)
			return nil, false, nil
		} else if c.isInRole(captureID, RoleUndetermined) {
			log.Info("schedulerv3: capture is stopping during Commit",
				zap.String("captureID", captureID))
			return nil, false, nil
		}

	case scheduler.ComponentStatusPreparing:
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.Any("tableState", input))
	return nil, false, nil
}

func (c *changefeed) pollOnRemoving(
	input *ChangefeedStatus, captureID model.CaptureID) (*new_arch.Message, bool, error) {
	switch input.ComponentStatus {
	case scheduler.ComponentStatusPreparing,
		scheduler.ComponentStatusPrepared,
		scheduler.ComponentStatusWorking:
		return c.getRemoveChangefeedRequest(captureID), false, nil
	case scheduler.ComponentStatusAbsent, scheduler.ComponentStatusStopped:
		var err error
		if c.primary == captureID {
			c.clearPrimary()
		} else if c.isInRole(captureID, RoleSecondary) {
			err = c.clearCapture(captureID, RoleSecondary)
		} else {
			err = c.clearCapture(captureID, RoleUndetermined)
		}
		if err != nil {
			log.Warn("schedulerv3: replication state remove capture with error",
				zap.Any("tableState", input),
				zap.String("captureID", captureID),
				zap.Error(err))
		}
		return nil, false, nil
	case scheduler.ComponentStatusStopping:
		return nil, false, nil
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.Any("tableState", input),
		zap.String("captureID", captureID))
	return nil, false, nil
}

func (c *changefeed) update(
	input *ChangefeedStatus) {
	//todo: real logic handled here
}

type ChangefeedStatus struct {
	ComponentStatus scheduler.ComponentStatus
	ChangefeedID    model.ChangeFeedID
}

// poll transit state based on input and the current state.
func (c *changefeed) poll(
	input *ChangefeedStatus, captureID model.CaptureID,
) ([]*new_arch.Message, error) {
	// check if the message belongs to this changefeed
	if _, ok := c.Captures[captureID]; !ok {
		return nil, nil
	}

	// output message that should be sent remote
	msgBuf := make([]*new_arch.Message, 0)

	stateChanged := true
	var err error
	for stateChanged {
		//err := r.checkInvariant(input, captureID)
		//if err != nil {
		//	return nil, errors.Trace(err)
		//}
		oldState := c.scheduleState
		var msg *new_arch.Message
		switch c.scheduleState {
		case scheduler.SchedulerComponentStatusAbsent:
			stateChanged, err = c.pollOnAbsent(input, captureID)
		case scheduler.SchedulerComponentStatusPrepare:
			msg, stateChanged, err = c.pollOnPrepare(input, captureID)
		case scheduler.SchedulerComponentStatusCommit:
			msg, stateChanged, err = c.pollOnCommit(input, captureID)
		case scheduler.SchedulerComponentStatusWorking:
			msg, stateChanged, err = c.pollOnReplicating(input, captureID)
		case scheduler.SchedulerComponentStatusRemoving:
			msg, stateChanged, err = c.pollOnRemoving(input, captureID)
		default:
			return nil, errors.New("schedulerv3: table state unknown")
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if msg != nil {
			msgBuf = append(msgBuf, msg)
		}
		if stateChanged {
			log.Info("schedulerv3: replication state transition, poll",
				zap.String("changefeed", c.ID.ID),
				zap.Any("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("old", oldState),
				zap.Any("new", c.scheduleState))
		}
	}
	return msgBuf, nil
}

func (r *changefeed) handleTableStatus(
	from model.CaptureID, status *ChangefeedStatus,
) ([]*new_arch.Message, error) {
	return r.poll(status, from)
}

func (r *changefeed) handleMove(
	dest model.CaptureID,
) ([]*new_arch.Message, error) {
	// Ignore move table if it has been removed already.
	if r.hasRemoved() {
		log.Warn("schedulerv3: move table is ignored",
			zap.Any("changefeed", r.ID.ID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	// Ignore move table if
	// 1) it's not in Replicating state or
	// 2) the dest capture is the primary.
	if r.scheduleState != scheduler.SchedulerComponentStatusWorking || r.primary == dest {
		log.Warn("schedulerv3: move table is ignored",
			zap.String("changefeed", r.ID.ID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	r.scheduleState = scheduler.SchedulerComponentStatusPrepare
	err := r.setCapture(dest, RoleSecondary)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("schedulerv3: replication state transition, move table",
		zap.String("changefeed", r.ID.ID),
		zap.Any("replicationSet", r))
	status := ChangefeedStatus{
		ChangefeedID:    r.ID,
		ComponentStatus: scheduler.ComponentStatusAbsent,
	}
	return r.poll(&status, dest)
}

func (r *changefeed) handleAdd(
	captureID model.CaptureID,
) ([]*new_arch.Message, error) {
	// Ignore add table if it's not in Absent state.
	if r.scheduleState != scheduler.SchedulerComponentStatusAbsent {
		log.Warn("schedulerv3: add table is ignored",
			zap.String("changefeed", r.ID.ID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	err := r.setCapture(captureID, RoleSecondary)
	if err != nil {
		return nil, errors.Trace(err)
	}
	status := ChangefeedStatus{
		ChangefeedID:    r.ID,
		ComponentStatus: scheduler.ComponentStatusAbsent,
	}
	msgs, err := r.poll(&status, captureID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("schedulerv3: replication state transition, add table",
		zap.String("changefeed", r.ID.ID),
		zap.Any("replicationSet", r))
	return msgs, nil
}

func (r *changefeed) handleRemove() ([]*new_arch.Message, error) {
	// Ignore remove table if it has been removed already.
	if r.hasRemoved() {
		log.Warn("schedulerv3: remove table is ignored",
			zap.String("changefeed", r.ID.ID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	// Ignore remove table if it's not in Replicating state.
	if r.scheduleState != scheduler.SchedulerComponentStatusWorking {
		log.Warn("schedulerv3: remove table is ignored",
			zap.String("changefeed", r.ID.ID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	r.scheduleState = scheduler.SchedulerComponentStatusRemoving
	log.Info("schedulerv3: replication state transition, remove table",
		zap.String("changefeed", r.ID.ID),
		zap.Any("replicationSet", r))
	status := ChangefeedStatus{
		ChangefeedID:    r.ID,
		ComponentStatus: scheduler.ComponentStatusWorking,
	}
	return r.poll(&status, r.primary)
}

// handleCaptureShutdown handle capture shutdown event.
// Besides returning messages and errors, it also returns a bool to indicate
// whether r is affected by the capture shutdown.
func (r *changefeed) handleCaptureShutdown(
	captureID model.CaptureID,
) ([]*new_arch.Message, bool, error) {
	_, ok := r.Captures[captureID]
	if !ok {
		// r is not affected by the capture shutdown.
		return nil, false, nil
	}
	// The capture has shutdown, the table has stopped.
	status := ChangefeedStatus{
		ChangefeedID:    r.ID,
		ComponentStatus: scheduler.ComponentStatusStopped,
	}
	oldState := r.scheduleState
	msgs, err := r.poll(&status, captureID)
	log.Info("schedulerv3: replication state transition, capture shutdown",
		zap.String("changefeed", r.ID.ID),
		zap.Any("replicationSet", r),
		zap.Any("old", oldState), zap.Any("new", r.scheduleState))
	return msgs, true, errors.Trace(err)
}

func (r *changefeed) setCapture(captureID model.CaptureID, role Role) error {
	cr, ok := r.Captures[captureID]
	if ok && cr != role {
		jsonR, _ := json.Marshal(r)
		return errors.ErrReplicationSetInconsistent.GenWithStackByArgs(fmt.Sprintf(
			"can not set %s as %s, it's %s, %v", captureID, role, cr, string(jsonR)))
	}
	r.Captures[captureID] = role
	return nil
}

func (r *changefeed) getRole(role Role) (model.CaptureID, bool) {
	for captureID, cr := range r.Captures {
		if cr == role {
			return captureID, true
		}
	}
	return "", false
}

func (r *changefeed) hasRole(role Role) bool {
	_, has := r.getRole(role)
	return has
}

func (r *changefeed) isInRole(captureID model.CaptureID, role Role) bool {
	rc, ok := r.Captures[captureID]
	if !ok {
		return false
	}
	return rc == role
}

func (r *changefeed) clearCapture(captureID model.CaptureID, role Role) error {
	cr, ok := r.Captures[captureID]
	if ok && cr != role {
		jsonR, _ := json.Marshal(r)
		return errors.ErrReplicationSetInconsistent.GenWithStackByArgs(fmt.Sprintf(
			"can not clear %s as %s, it's %s, %v", captureID, role, cr, string(jsonR)))
	}
	delete(r.Captures, captureID)
	return nil
}

func (r *changefeed) promoteSecondary(captureID model.CaptureID) error {
	if r.primary == captureID {
		log.Warn("schedulerv3: capture is already promoted as the primary",
			zap.String("captureID", captureID),
			zap.Any("replicationSet", r))
		return nil
	}
	role, ok := r.Captures[captureID]
	if ok && role != RoleSecondary {
		jsonR, _ := json.Marshal(r)
		return errors.ErrReplicationSetInconsistent.GenWithStackByArgs(fmt.Sprintf(
			"can not promote %s to primary, it's %s, %v", captureID, role, string(jsonR)))
	}
	if r.primary != "" {
		delete(r.Captures, r.primary)
	}
	r.primary = captureID
	r.Captures[r.primary] = RolePrimary
	return nil
}

func (r *changefeed) clearPrimary() {
	delete(r.Captures, r.primary)
	r.primary = ""
}

// SetHeap is a max-heap, it implements heap.Interface.
type SetHeap []*changefeed

// Len returns the length of the heap.
func (h SetHeap) Len() int { return len(h) }

// Less returns true if the element at i is less than the element at j.
func (h SetHeap) Less(i, j int) bool {
	if h[i].ID.ID > h[j].ID.ID {
		return true
	}
	return false
}

// Swap swaps the elements with indexes i and j.
func (h SetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push pushes an element to the heap.
func (h *SetHeap) Push(x interface{}) {
	*h = append(*h, x.(*changefeed))
}

// Pop pops an element from the heap.
func (h *SetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return x
}
