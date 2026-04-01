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
	"fmt"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/new_arch"
	ns "github.com/pingcap/tiflow/new_arch/scheduler"
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
	changefeeds     map[model.ChangeFeedID]*changefeed
	upstreamManager *upstream.Manager
	cfg             *config.SchedulerConfig
	globalVars      *vars.GlobalVars

	captureManager    *CaptureManager
	selfCaptureID     model.CaptureID
	schedulerManager  *Manager
	changefeedManager *ChangefeedManager

	version int64
	epoch   int64

	msgLock sync.RWMutex
	msgBuf  []*new_arch.Message
}

var allChangefeeds []*model.ChangeFeedInfo

func init() {
	for i := 0; i < 80000; i++ {
		allChangefeeds = append(allChangefeeds, &model.ChangeFeedInfo{
			ID:      fmt.Sprintf("%d", i),
			Config:  config.GetDefaultReplicaConfig(),
			SinkURI: "blackhole://",
		})
	}
}

func NewCoordinator(
	upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	globalVars *vars.GlobalVars,
) owner.Owner {
	c := &coordinatorImpl{
		upstreamManager:   upstreamManager,
		cfg:               cfg,
		globalVars:        globalVars,
		changefeeds:       make(map[model.ChangeFeedID]*changefeed),
		selfCaptureID:     globalVars.CaptureInfo.ID,
		schedulerManager:  NewSchedulerManager(cfg),
		changefeedManager: NewChangefeedManager(cfg.MaxTaskConcurrency),
	}
	_, _ = globalVars.MessageServer.SyncAddHandler(context.Background(), new_arch.GetCoordinatorTopic(),
		&new_arch.Message{}, func(sender string, messageI interface{}) error {
			c.msgLock.Lock()
			c.msgBuf = append(c.msgBuf, messageI.(*new_arch.Message))
			c.msgLock.Unlock()
			return nil
		})
	c.captureManager = NewCaptureManager(c.selfCaptureID, globalVars.OwnerRevision)
	return c
}

func (c *coordinatorImpl) HandleMessage(msgs []*new_arch.Message) ([]*new_arch.Message, error) {
	var rsp []*new_arch.Message
	for _, msg := range msgs {
		//todo: 和tick 是一个多线程读写处理, 使用 actor system
		if msg.ChangefeedHeartbeatResponse != nil {
			c.captureManager.HandleMessage([]*new_arch.Message{msg})
			r, err := c.changefeedManager.handleMessageHeartbeatResponse(msg.From, msg.ChangefeedHeartbeatResponse)
			if err != nil {
				log.Warn("failed to handle message heartbeat", zap.Error(err))
				return nil, errors.Trace(err)
			}
			rsp = append(rsp, r...)
		}
	}
	return rsp, nil
}

func (c *coordinatorImpl) SendMessage(ctx context.Context, capture string, topic string, m *new_arch.Message) error {
	client := c.globalVars.MessageRouter.GetClient(capture)
	_, err := client.TrySendMessage(ctx, topic, m)
	return errors.Trace(err)
}

type changefeedError struct {
	warning *model.RunningError
	failErr *model.RunningError
}

var lastCheckTime = time.Now()

func (c *coordinatorImpl) Tick(ctx context.Context,
	rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	state := rawState.(*orchestrator.GlobalReactorState)
	// update gc safe point
	//if err = c.updateGCSafepoint(ctx, state); err != nil {
	//	return nil, errors.Trace(err)
	//}

	//var newChangefeeds = make(map[model.ChangeFeedID]struct{})
	//// Tick all changefeeds.
	//for changefeedID, reactor := range state.Changefeeds {
	//	_, exist := c.changefeeds[changefeedID]
	//	if !exist {
	//		// check if changefeed should running
	//		if reactor.Info.State == model.StateStopped ||
	//			reactor.Info.State == model.StateFailed ||
	//			reactor.Info.State == model.StateFinished {
	//			continue
	//		}
	//		// create
	//		newChangefeeds[changefeedID] = struct{}{}
	//	}
	//}
	//
	//// Cleanup changefeeds that are not in the state.
	//// save status to etcd
	//for changefeedID, cf := range c.changefeeds {
	//	if reactor, exist := state.Changefeeds[changefeedID]; exist {
	//		//todo: handle error
	//		cf.EmitCheckpointTs(ctx, reactor.Status.CheckpointTs)
	//
	//		checkpointTs := cf.GetCheckpointTs(ctx)
	//		reactor.PatchStatus(
	//			func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
	//				changed := false
	//				if status.CheckpointTs != checkpointTs {
	//					status.CheckpointTs = checkpointTs
	//					changed = true
	//				}
	//				return status, changed, nil
	//			})
	//		//save error info
	//		var lastWarning *model.RunningError
	//		var lastErr *model.RunningError
	//		for captureID, errs := range cf.errors {
	//			lastWarning = errs.warning
	//			lastErr = errs.failErr
	//			reactor.PatchTaskPosition(captureID,
	//				func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
	//					if position == nil {
	//						position = &model.TaskPosition{}
	//					}
	//					changed := false
	//					if position.Warning != errs.warning {
	//						position.Warning = errs.warning
	//						changed = true
	//					}
	//					if position.Error != errs.failErr {
	//						position.Error = errs.failErr
	//						changed = true
	//					}
	//					return position, changed, nil
	//				})
	//		}
	//		reactor.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
	//			info.Error = lastErr
	//			info.Warning = lastWarning
	//			return info, true, nil
	//		})
	//
	//		// reported changefeed state
	//		switch cf.state {
	//		case model.StateFailed, model.StateFinished:
	//			reactor.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
	//				info.State = cf.state
	//				return info, false, nil
	//			})
	//			// stop changefeed, and remove
	//			cf.Stop(ctx)
	//		}
	//		continue
	//	}
	//
	//	// stop changefeed
	//	cf.Stop(ctx)
	//	delete(c.changefeeds, changefeedID)
	//}

	if time.Since(lastCheckTime) > time.Second*20 {
		workingTask := 0
		prepareTask := 0
		absentTask := 0
		commitTask := 0
		removingTask := 0
		for _, cf := range c.changefeedManager.changefeeds {
			switch cf.scheduleState {
			case ns.SchedulerComponentStatusAbsent:
				absentTask++
			case ns.SchedulerComponentStatusPrepare:
				prepareTask++
			case ns.SchedulerComponentStatusCommit:
				commitTask++
			case ns.SchedulerComponentStatusWorking:
				workingTask++
			case ns.SchedulerComponentStatusRemoving:
				removingTask++
			}
		}
		log.Info("changefeed status",
			zap.Int("absent", absentTask),
			zap.Int("prepare", prepareTask),
			zap.Int("commit", commitTask),
			zap.Int("working", workingTask),
			zap.Int("removing", removingTask))
		lastCheckTime = time.Now()
	}

	c.changefeedManager.allChangefeedConfig = state.Changefeeds

	c.msgLock.Lock()
	buf := c.msgBuf
	c.msgBuf = nil
	c.msgLock.Unlock()
	msgs, err := c.HandleMessage(buf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cmsgs := c.captureManager.HandleAliveCaptureUpdate(state.Captures)
	msgs = append(msgs, cmsgs...)
	if err := c.sendMsgs(ctx, msgs); err != nil {
		return nil, errors.Trace(err)
	}

	// only balance tables when all capture is replied bootstrap messages
	if c.captureManager.CheckAllCaptureInitialized() {
		c.ScheduleChangefeedMaintainer(ctx, allChangefeeds)
		//try to rebalance tables if needed
		c.BalanceTables(ctx)
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

func (c *coordinatorImpl) BalanceTables(ctx context.Context) error {
	return nil
}

func (c *coordinatorImpl) ScheduleChangefeedMaintainer(ctx context.Context,
	changefeeds []*model.ChangeFeedInfo) error {

	//var captures []*model.CaptureInfo
	//for _, capture := range state.Captures {
	//	captures = append(captures, capture)
	//}
	if changes := c.captureManager.TakeChanges(); changes != nil {
		c.changefeedManager.HandleCaptureChanges(changes.Init, changes.Removed)
	}

	tasks := c.schedulerManager.Schedule(
		changefeeds,
		c.captureManager.Captures,
		c.changefeedManager.changefeeds,
		c.changefeedManager.runningTasks)

	messages, err := c.changefeedManager.HandleTasks(tasks)
	if err != nil {
		return errors.Trace(err)
	}

	//var mmsgs []*new_arch.Message
	//captureMap := make(map[string]*new_arch.Message)
	//for _, m := range messages {
	//	_, ok := captureMap[m.To]
	//	if !ok {
	//		captureMap[m.To] = &new_arch.Message{
	//			Header: m.Header,
	//			From:   m.From,
	//			To:     m.To,
	//			DispatchMaintainerRequest: &new_arch.DispatchMaintainerRequest{
	//				BatchAddMaintainerRequest: &new_arch.BatchAddMaintainerRequest{},
	//				//BatchRemoveMaintainerRequest: &new_arch.BatchRemoveMaintainerRequest{},
	//			},
	//		}
	//		mmsgs = append(mmsgs, captureMap[m.To])
	//	}
	//	if m.DispatchMaintainerRequest.AddMaintainerRequest != nil {
	//		captureMap[m.To].DispatchMaintainerRequest.BatchAddMaintainerRequest.Requests =
	//			append(captureMap[m.To].DispatchMaintainerRequest.BatchAddMaintainerRequest.Requests,
	//				m.DispatchMaintainerRequest.AddMaintainerRequest)
	//	}
	//	if m.DispatchMaintainerRequest.RemoveMaintainerRequest != nil {
	//		captureMap[m.To].DispatchMaintainerRequest.BatchRemoveMaintainerRequest.Requests =
	//			append(captureMap[m.To].DispatchMaintainerRequest.BatchRemoveMaintainerRequest.Requests,
	//				m.DispatchMaintainerRequest.RemoveMaintainerRequest)
	//	}
	//}

	if err := c.sendMsgs(ctx, messages); err != nil {
		return errors.Trace(err)
	}

	//idx := 0
	//for changefeedID := range newChangefeeds {
	//	cf := state.Changefeeds[changefeedID]
	//	//todo: select a capture to schedule maintainer
	//	impl := newChangefeed(captures[idx%len(captures)].ID, cf.ID, cf.Info, cf.Status, c)
	//	c.changefeeds[cf.ID] = impl
	//	go impl.Run(ctx)
	//	idx++
	//}
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
