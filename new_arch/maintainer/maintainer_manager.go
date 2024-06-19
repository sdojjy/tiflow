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
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
	"sync"
	"time"
)

// MaintainerManager runs on every capture, receive command from coordinator
type MaintainerManager struct {
	//随机生成，coordinator 初始化时上报上去
	Epoch       string
	maintainers map[string]*Maintainer

	upstreamManager *upstream.Manager
	cfg             *config.SchedulerConfig
	globalVars      *vars.GlobalVars

	masterID      string
	masterVersion int64

	selfCaptureID model.CaptureID
	msgLock       sync.RWMutex
	msgBuf        []*new_arch.Message
}

func NewMaintainerManager(upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	globalVars *vars.GlobalVars) *MaintainerManager {
	m := &MaintainerManager{
		upstreamManager: upstreamManager,
		cfg:             cfg,
		globalVars:      globalVars,
		maintainers:     make(map[string]*Maintainer),
		selfCaptureID:   globalVars.CaptureInfo.ID,
	}
	_, _ = m.globalVars.MessageServer.SyncAddHandler(context.Background(), new_arch.GetChangefeedMaintainerManagerTopic(),
		&new_arch.Message{}, func(sender string, messageI interface{}) error {
			message := messageI.(*new_arch.Message)
			m.msgLock.Lock()
			m.msgBuf = append(m.msgBuf, message)
			m.msgLock.Unlock()
			return nil
		})
	return m
}

func (m *MaintainerManager) Tick(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 50)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-tick.C:
			m.msgLock.Lock()
			buf := m.msgBuf
			m.msgBuf = nil
			m.msgLock.Unlock()
			for _, msg := range buf {
				m.HandleMessage(msg.From, msg)
			}
			if m.masterID != "" {
				msgs, err := m.handleMessageHeartbeat()
				if err != nil {
					return errors.Trace(err)
				}
				m.sendMsg(msgs)
			}
		}
	}
}

func (m *MaintainerManager) HandleMessage(send string, msg *new_arch.Message) {
	var err error
	if msg.DispatchMaintainerRequest != nil {
		err = m.handleDispatchMaintainerRequest(msg.DispatchMaintainerRequest, "")
		if err != nil {
			log.Error("handle message failed", zap.Error(err))
		}
	} else if msg.BootstrapRequest != nil {
		m.masterVersion = msg.Header.SenderVersion
		m.masterID = msg.From
	}
}

func (m *MaintainerManager) sendMsg(msgs []*new_arch.Message) {
	for _, msg := range msgs {
		msg.From = m.masterID
		msg.To = m.masterID
		msg.Header = &new_arch.MessageHeader{
			SenderVersion: 0,
			SenderEpoch:   0,
		}
		if err := m.SendMessage(context.Background(), m.masterID, new_arch.GetCoordinatorTopic(), msg); err != nil {
			log.Error("send message failed", zap.Error(err))
		}
	}
}

func (m *MaintainerManager) handleDispatchMaintainerRequest(
	request *new_arch.DispatchMaintainerRequest,
	epoch string,
) error {
	if m.Epoch != epoch {
		log.Info("schedulerv3: agent receive dispatch table request " +
			"epoch does not match, ignore it")
		return nil
	}
	// make the assumption that all tables are tracked by the agent now.
	// this should be guaranteed by the caller of the method.
	if request.AddMaintainerRequest != nil {
		span := model.ChangeFeedID{ID: request.AddMaintainerRequest.Config.ID}
		task := &dispatchMaintainerTask{
			ID:        span,
			IsRemove:  false,
			IsPrepare: request.AddMaintainerRequest.IsSecondary,
			status:    dispatchTaskReceived,
		}
		cf, ok := m.maintainers[request.AddMaintainerRequest.Config.ID]
		if !ok {
			cf = NewMaintainer(span, m.upstreamManager, m.cfg, m.globalVars,
				request.AddMaintainerRequest.Config, request.AddMaintainerRequest.Status)
			m.maintainers[request.AddMaintainerRequest.Config.ID] = cf
		}
		cf.injectDispatchTableTask(task)
	} else if request.RemoveMaintainerRequest != nil {
		span := model.ChangeFeedID{ID: request.AddMaintainerRequest.Config.ID}
		cf, ok := m.maintainers[request.AddMaintainerRequest.Config.ID]
		if !ok {
			log.Warn("schedulerv3: agent ignore remove table request, "+
				"since the table not found",
				zap.String("changefeed", span.ID),
				zap.String("span", span.String()),
				zap.Any("request", request))
			return nil
		}
		task := &dispatchMaintainerTask{
			ID:       span,
			IsRemove: true,
			status:   dispatchTaskReceived,
		}
		cf.injectDispatchTableTask(task)
	} else {
		log.Warn("schedulerv3: agent ignore unknown dispatch table request",
			zap.Any("request", request))
		return nil
	}
	return m.handleTasks()
}

func (m *MaintainerManager) handleTasks() error {
	var err error
	for _, cf := range m.maintainers {
		if cf.task == nil {
			continue
		}
		if cf.task.IsRemove {
			err = cf.handleRemoveTableTask()
		} else {
			err = cf.handleAddTableTask()
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *MaintainerManager) handleMessageHeartbeat() ([]*new_arch.Message, error) {
	msg := &new_arch.Message{}
	cfs := make([]*new_arch.ChangefeedStatus, 0, len(m.maintainers))
	for _, cf := range m.maintainers {
		cfs = append(cfs, cf.getStatus())
	}
	msg.ChangefeedHeartbeatResponse = &new_arch.ChangefeedHeartbeatResponse{Changefeeds: cfs}
	return []*new_arch.Message{msg}, nil
}

func (m *MaintainerManager) SendMessage(ctx context.Context, capture string, topic string, msg *new_arch.Message) error {
	client := m.globalVars.MessageRouter.GetClient(capture)
	_, err := client.TrySendMessage(ctx, topic, msg)
	return errors.Trace(err)
}
