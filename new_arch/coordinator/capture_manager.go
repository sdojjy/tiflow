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
	"github.com/pingcap/tiflow/new_arch/scheduler"
	"go.uber.org/zap"
)

// CaptureState is the state of a capture.
//
//	    ┌───────────────┐ Heartbeat Resp ┌─────────────┐
//	    │ Uninitialized ├───────────────>│ Initialized │
//	    └──────┬────────┘                └──────┬──────┘
//	           │                                │
//	IsStopping │          ┌──────────┐          │ IsStopping
//	           └────────> │ Stopping │ <────────┘
//	                      └──────────┘
type CaptureState int

const (
	// CaptureStateUninitialized means the capture status is unknown,
	// no heartbeat response received yet.
	CaptureStateUninitialized CaptureState = 1
	// CaptureStateInitialized means owner has received heartbeat response.
	CaptureStateInitialized CaptureState = 2
	// CaptureStateStopping means the capture is removing, e.g., shutdown.
	CaptureStateStopping CaptureState = 3
)

var captureStateMap = map[CaptureState]string{
	CaptureStateUninitialized: "CaptureStateUninitialized",
	CaptureStateInitialized:   "CaptureStateInitialized",
	CaptureStateStopping:      "CaptureStateStopping",
}

func (s CaptureState) String() string {
	return captureStateMap[s]
}

// CaptureStatus represent capture's status.
type CaptureStatus struct {
	OwnerRev    int64
	State       CaptureState
	Changefeeds []*ChangefeedStatus
	ID          model.CaptureID
	Addr        string
	IsOwner     bool
}

func newCaptureStatus(
	rev int64, id model.CaptureID,
	addr string, isOwner bool,
) *CaptureStatus {
	return &CaptureStatus{
		OwnerRev: rev,
		State:    CaptureStateUninitialized,
		ID:       id,
		Addr:     addr,
		IsOwner:  isOwner,
	}
}

func (c *CaptureStatus) handleHeartbeatResponse(
	resp *new_arch.ChangefeedHeartbeatResponse,
) {
	if c.State == CaptureStateUninitialized {
		c.State = CaptureStateInitialized
		log.Info("schedulerv3: capture initialized",
			zap.String("capture", c.ID),
			zap.String("captureAddr", c.Addr))
	}
	if resp.Liveness == int32(model.LivenessCaptureStopping) {
		c.State = CaptureStateStopping
		log.Info("schedulerv3: capture stopping",
			zap.String("capture", c.ID),
			zap.String("captureAddr", c.Addr))
	}
	for _, cf := range resp.Changefeeds {
		c.Changefeeds = append(c.Changefeeds, &ChangefeedStatus{
			ChangefeedID:    cf.ID,
			ComponentStatus: scheduler.ComponentStatus(cf.ComponentStatus),
		})
	}
}

// CaptureChanges wraps changes of captures.
type CaptureChanges struct {
	Init    map[model.CaptureID][]*ChangefeedStatus
	Removed map[model.CaptureID][]*ChangefeedStatus
}

// CaptureManager manages capture status.
type CaptureManager struct {
	OwnerRev int64
	Captures map[model.CaptureID]*CaptureStatus

	initialized bool
	changes     *CaptureChanges

	ownerID model.CaptureID
}

// NewCaptureManager returns a new capture manager.
func NewCaptureManager(
	ownerID model.CaptureID,
	rev int64,
) *CaptureManager {
	return &CaptureManager{
		OwnerRev: rev,
		Captures: make(map[model.CaptureID]*CaptureStatus),
		ownerID:  ownerID,
	}
}

// CheckAllCaptureInitialized check if all capture is initialized.
func (c *CaptureManager) CheckAllCaptureInitialized() bool {
	return c.initialized && c.checkAllCaptureInitialized()
}

func (c *CaptureManager) checkAllCaptureInitialized() bool {
	for _, captureStatus := range c.Captures {
		// CaptureStateStopping is also considered initialized, because when
		// a capture shutdown, it becomes stopping, we need to move its tables
		// to other captures.
		if captureStatus.State == CaptureStateUninitialized {
			return false
		}
	}
	return len(c.Captures) != 0
}

// HandleMessage handles messages sent from other captures.
func (c *CaptureManager) HandleMessage(
	msgs []*new_arch.Message,
) {
	for _, msg := range msgs {
		captureStatus, ok := c.Captures[msg.From]
		if !ok {
			log.Warn("schedulerv3: heartbeat response from unknown capture",
				zap.String("capture", msg.From))
			continue
		}
		captureStatus.handleHeartbeatResponse(msg.ChangefeedHeartbeatResponse)
	}
}

// HandleAliveCaptureUpdate update captures liveness.
func (c *CaptureManager) HandleAliveCaptureUpdate(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) []*new_arch.Message {
	msgs := make([]*new_arch.Message, 0)
	for id, info := range aliveCaptures {
		if _, ok := c.Captures[id]; !ok {
			// A new capture.
			c.Captures[id] = newCaptureStatus(
				c.OwnerRev, id, info.AdvertiseAddr, c.ownerID == id)
			log.Info("schedulerv3: find a new capture",
				zap.String("captureAddr", info.AdvertiseAddr),
				zap.String("capture", id))
			// 发送 bootstrap 消息
			msgs = append(msgs, &new_arch.Message{
				To:               id,
				From:             c.ownerID,
				BootstrapRequest: &new_arch.BootstrapRequest{},
			})
		}
	}

	// Find removed captures.
	for id, capture := range c.Captures {
		if _, ok := aliveCaptures[id]; !ok {
			log.Info("schedulerv3: removed a capture",
				zap.String("captureAddr", capture.Addr),
				zap.String("capture", id))
			delete(c.Captures, id)

			// Only update changes after initialization.
			if !c.initialized {
				continue
			}
			if c.changes == nil {
				c.changes = &CaptureChanges{}
			}
			if c.changes.Removed == nil {
				c.changes.Removed = make(map[string][]*ChangefeedStatus)
			}
			c.changes.Removed[id] = capture.Changefeeds
		}
	}

	// Check if this is the first time all captures are initialized.
	if !c.initialized && c.checkAllCaptureInitialized() {
		c.changes = &CaptureChanges{Init: make(map[string][]*ChangefeedStatus)}
		for id, capture := range c.Captures {
			c.changes.Init[id] = capture.Changefeeds
		}
		log.Info("schedulerv3: all capture initialized",
			zap.Int("captureCount", len(c.Captures)))
		c.initialized = true
	}

	return msgs
}

// TakeChanges takes the changes of captures that it sees so far.
func (c *CaptureManager) TakeChanges() *CaptureChanges {
	// Only return changes when it's initialized.
	if !c.initialized {
		return nil
	}
	changes := c.changes
	c.changes = nil
	return changes
}
