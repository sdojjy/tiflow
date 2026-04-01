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

package heartbeat

import (
	"context"
	"fmt"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/p2p"
	"sync"
	"time"
)

type Maintainer struct {
	selfVersion   int64
	masterVersion int64
	masterID      string

	// MessageServer and MessageRouter are for peer-messaging
	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	// 组件的版本
	version int
	// 父组件的版本，收到消息后需要检查
	parentVersion int
	// 子组件
	name string

	captureInfo map[model.CaptureID]*model.CaptureInfo

	// 组件的状态
	// starting, 正在启动，不能工作
	// running 启动完成对外服务
	// pending 和上级失联，不再工作
	// disconnected 和上级失联过长，主动退出
	state int

	mu struct {
		sync.Mutex
		// FIXME it's an unbounded buffer, and may cause OOM!
		msgBuf []*new_arch.Message
	}

	initialized bool

	AddComponent    func()
	RemoveComponent func()
	Iterate         func(func(m *Module))
}

func NewMaintainer(ctx context.Context,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	name string,
	version int64) (*Maintainer, error) {
	c := &Maintainer{
		messageServer: messageServer,
		messageRouter: messageRouter,
		name:          name,
		state:         1,
		captureInfo:   make(map[model.CaptureID]*model.CaptureInfo),
		initialized:   false,
		selfVersion:   version,
	}
	_, err := c.messageServer.SyncAddHandler(ctx,
		c.getSelfTopic(),
		&new_arch.Message{}, func(sender string, messageI interface{}) error {
			c.mu.Lock()
			c.mu.msgBuf = append(c.mu.msgBuf, messageI.(*new_arch.Message))
			c.mu.Unlock()
			return nil
		})
	return c, err
}

func (c *Maintainer) Tick(ctx context.Context,
	aliveCaptures map[model.CaptureID]*model.CaptureInfo) error {

	if !c.initialized {
		// send bootstrap message to all active caputres
		for _, capture := range c.captureInfo {
			client := c.messageRouter.GetClient(capture.ID)
			_, err := client.TrySendMessage(ctx, c.getPeerTopic(),
				new_arch.Message{
					MasterVersion:    c.selfVersion,
					BootstrapRequest: &new_arch.BootstrapRequest{}})
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}
	inboundMessages, err := c.Recv(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// check master bootstrap message first, so we can filter message
	for _, message := range inboundMessages {
		// master bootstraps
		if message.BootstrapRequest != nil {
			c.masterVersion = message.MasterVersion
			c.masterID = message.From
			// get all component information and send it to master
			var status []string
			c.Iterate(func(m *Module) {
				status = append(status, m.status)
			})
		}
	}
	// filter messages
	inboundMessages = c.filterMsg()

	// handle messages from master
	for _, message := range inboundMessages {
		// check masterID and master version in message, and compare it with memroy info
		if message.DispatchComponentRequest != nil {
			if message.DispatchComponentRequest.AddComponent != nil {
				//add a sub component
				c.AddComponent()
			} else if message.DispatchComponentRequest.RemoveComponent != nil {
				// remove a sub component
				c.RemoveComponent()
			}
		}
	}

	return nil
}

func (c *Maintainer) filterMsg() []*new_arch.Message {
	// check master id and master version
	return c.mu.msgBuf
}

func (t *Maintainer) Recv(ctx context.Context) ([]*new_arch.Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	recvMsgs := t.mu.msgBuf
	t.mu.msgBuf = make([]*new_arch.Message, 0)
	return recvMsgs, nil
}

func (c *Maintainer) getSelfTopic() string {
	return fmt.Sprintf("maintainer/%s", c.name)
}

func (c *Maintainer) getPeerTopic() string {
	return fmt.Sprintf("maintainer/%s/peer", c.name)
}

type Module struct {
	lastHeartbeatTime time.Time

	status string
}
