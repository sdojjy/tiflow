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

package supervisor

import (
	"context"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type coordinatorImpl struct {
	upstreamManager *upstream.Manager
	cfg             *config.SchedulerConfig
	globalVars      *vars.GlobalVars

	selfCaptureID model.CaptureID
	version       int64
	epoch         int64

	msgLock sync.RWMutex
	msgBuf  []*new_arch.Message

	super *Supervisor

	Captures    map[model.CaptureID]*model.CaptureInfo
	Changefeeds map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState
}

type CoordinatorID string

func (c CoordinatorID) String() string {
	return string(c)
}
func (c CoordinatorID) Equal(id InferiorID) bool {
	return c.String() == id.String()
}
func (c CoordinatorID) Less(id InferiorID) bool {
	return c.String() < id.String()
}

type MaintainerID model.ChangeFeedID

func (m MaintainerID) String() string {
	return model.ChangeFeedID(m).String()
}
func (m MaintainerID) Equal(id InferiorID) bool {
	return model.ChangeFeedID(m).String() == id.String()
}
func (m MaintainerID) Less(id InferiorID) bool {
	return model.ChangeFeedID(m).String() < id.String()
}

type changefeed struct {
	ID    model.ChangeFeedID
	State *ChangefeedStatus

	Info   *model.ChangeFeedInfo
	Status *model.ChangeFeedStatus
}

func (c *changefeed) UpdateStatus(status InferiorStatus) {
	c.State = status.(*ChangefeedStatus)
}

func (c *changefeed) NewInferiorStatus(status ComponentStatus) InferiorStatus {
	return &ChangefeedStatus{
		ID:     MaintainerID(c.ID),
		Status: status,
	}
}

func (c *changefeed) NewAddInferiorMessage(capture model.CaptureID, secondary bool) *new_arch.Message {
	return &new_arch.Message{
		To: capture,
		DispatchMaintainerRequest: &new_arch.DispatchMaintainerRequest{
			AddMaintainerRequest: &new_arch.AddMaintainerRequest{
				ID:          c.ID,
				Config:      nil, //todo
				Status:      c.Status,
				IsSecondary: secondary,
			}},
	}
}
func (c *changefeed) NewRemoveInferiorMessage(capture model.CaptureID) *new_arch.Message {
	return &new_arch.Message{
		To: capture,
		DispatchMaintainerRequest: &new_arch.DispatchMaintainerRequest{
			RemoveMaintainerRequest: &new_arch.RemoveMaintainerRequest{
				ID: c.Info.ID,
			},
		},
	}
}

type ChangefeedStatus struct {
	ID     MaintainerID
	Status ComponentStatus
}

func (c *ChangefeedStatus) GetInferiorID() InferiorID {
	return InferiorID(c.ID)
}

func (c *ChangefeedStatus) GetInferiorState() ComponentStatus {
	return c.Status
}

func NewCoordinator(
	upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	globalVars *vars.GlobalVars,
) owner.Owner {
	c := &coordinatorImpl{
		upstreamManager: upstreamManager,
		cfg:             cfg,
		globalVars:      globalVars,
		selfCaptureID:   globalVars.CaptureInfo.ID,
	}
	_, _ = globalVars.MessageServer.SyncAddHandler(context.Background(), new_arch.GetCoordinatorTopic(),
		&new_arch.Message{}, func(sender string, messageI interface{}) error {
			c.msgLock.Lock()
			c.msgBuf = append(c.msgBuf, messageI.(*new_arch.Message))
			c.msgLock.Unlock()
			return nil
		})
	c.super = NewSupervisor(CoordinatorID(c.selfCaptureID),
		NewBtreeMap[InferiorID, *StateMachine](),
		NewBtreeMap[InferiorID, *ScheduleTask](),
		c.NewChangefeed, c.NewBootstrapMessage)
	return c
}
func (c *coordinatorImpl) NewBootstrapMessage(capture model.CaptureID) *new_arch.Message {
	return &new_arch.Message{
		To:               capture,
		BootstrapRequest: &new_arch.BootstrapRequest{},
	}
}

func (c *coordinatorImpl) NewChangefeed(id InferiorID) Inferior {
	cf := &changefeed{
		ID: model.ChangeFeedID(id.(MaintainerID)),
	}
	info, ok := c.Changefeeds[cf.ID]
	if ok {
		cf.Info = info.Info
		cf.Status = info.Status
	} else {
		log.Warn("changefeed is not found", zap.Stringer("changefeed", cf.ID))
	}
	return cf
}

func (c *coordinatorImpl) Tick(ctx context.Context,
	rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	state := rawState.(*orchestrator.GlobalReactorState)
	//var newChangefeeds = make(map[model.ChangeFeedID]struct{})

	// todo: cache it
	c.Changefeeds = state.Changefeeds
	c.Captures = state.Captures

	//
	c.msgLock.Lock()
	buf := c.msgBuf
	c.msgBuf = nil
	c.msgLock.Unlock()

	for _, msg := range buf {
		if msg.ChangefeedHeartbeatResponse != nil {
			var status []InferiorStatus
			for _, st := range msg.ChangefeedHeartbeatResponse.Changefeeds {
				status = append(status, &ChangefeedStatus{
					Status: ComponentStatus(st.ComponentStatus),
					ID:     MaintainerID(st.ID),
				})
			}
			c.super.UpdateCaptureStatus(msg.From, status)
		}
	}

	cmsgs, removed := c.super.HandleAliveCaptureUpdate(state.Captures)
	var msgs []*new_arch.Message
	msgs = append(msgs, cmsgs...)
	if c.super.CheckAllCaptureInitialized() {
		//tasks
		//c.super.HandleScheduleTasks()
		c.super.HandleCaptureChanges(removed)
	}

	if err := c.sendMsgs(ctx, msgs); err != nil {
		return nil, errors.Trace(err)
	}
	return state, nil
}

func (c *coordinatorImpl) sendMsgs(ctx context.Context, msgs []*new_arch.Message) error {
	for _, msg := range msgs {
		msg.From = c.selfCaptureID
		msg.Header = &new_arch.MessageHeader{
			SenderVersion: c.version,
			SenderEpoch:   c.epoch,
		}
		client := c.globalVars.MessageRouter.GetClient(msg.To)
		if client == nil {
			log.Warn("schedulerv3: no message client found, retry later",
				zap.String("to", msg.To))
			time.Sleep(2 * time.Second)
			client = c.globalVars.MessageRouter.GetClient(msg.To)
		}
		_, err := client.TrySendMessage(ctx, new_arch.GetChangefeedMaintainerManagerTopic(), msg)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *coordinatorImpl) EnqueueJob(adminJob model.AdminJob, done chan<- error) {

}

func (c *coordinatorImpl) RebalanceTables(cfID model.ChangeFeedID, done chan<- error) {

}

func (c *coordinatorImpl) ScheduleTable(
	cfID model.ChangeFeedID, toCapture model.CaptureID,
	tableID model.TableID, done chan<- error,
) {

}
func (c *coordinatorImpl) DrainCapture(query *scheduler.Query, done chan<- error) {

}
func (c *coordinatorImpl) WriteDebugInfo(w io.Writer, done chan<- error) {

}
func (c *coordinatorImpl) Query(query *owner.Query, done chan<- error) {

}
func (c *coordinatorImpl) AsyncStop() {

}
func (c *coordinatorImpl) UpdateChangefeedAndUpstream(ctx context.Context,
	upstreamInfo *model.UpstreamInfo,
	changeFeedInfo *model.ChangeFeedInfo,
) error {
	return nil
}
func (c *coordinatorImpl) UpdateChangefeed(ctx context.Context,
	changeFeedInfo *model.ChangeFeedInfo) error {
	return nil
}
func (c *coordinatorImpl) CreateChangefeed(context.Context,
	*model.UpstreamInfo,
	*model.ChangeFeedInfo,
) error {
	return nil
}
