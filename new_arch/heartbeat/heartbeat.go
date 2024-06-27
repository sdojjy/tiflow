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
	"time"
)

type Liveness[T any] interface {
	// IsAlive check weather the component is alive, if not,  this component must exit
	IsAlive() bool
	// IsWorkable check weather the component can workload, if not, this component must be paused
	IsWorkable() bool
	// UpdateHeartbeat saves last heartbeat from peer
	UpdateHeartbeat(t T)
}

// 1. 组件启动的时候需要通知所有的 capture 一次 . 告诉子组件有新的 master 产生了， 让他们更改 master 地址 ，收集信息
// 2. heartbeat 是从 client 向 master 发送？
type AliveDetectionServer struct {
	// MessageServer and MessageRouter are for peer-messaging
	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	SubComponents []ComponentMaintainer
}

func NewAliveDetectionServer(ctx context.Context,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter) *AliveDetectionServer {
	return &AliveDetectionServer{
		messageServer: messageServer,
		messageRouter: messageRouter,
	}
}

type ComponentMaintainer struct {
	// MessageServer and MessageRouter are for peer-messaging
	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter

	// 组件的版本
	version int
	// 父组件的版本，收到消息后需要检查
	parentVersion int
	// 子组件
	subcomponent map[model.CaptureID]*SubComponent
	name         string

	captureInfo map[model.CaptureID]*model.CaptureInfo

	// 组件的状态
	// starting, 正在启动，不能工作
	// running 启动完成对外服务
	// pending 和上级失联，不再工作
	// disconnected 和上级失联过长，主动退出
	state int
}

const (
	stateStarting     = 0
	stateRunning      = 1
	stateStopping     = 2
	stateDisconnected = 3
)

func NewComponentMaintainer(ctx context.Context,
	messageServer *p2p.MessageServer,
	messageRouter p2p.MessageRouter,
	name string) *ComponentMaintainer {
	c := &ComponentMaintainer{
		messageServer: messageServer,
		messageRouter: messageRouter,
		name:          name,
		state:         1,
		subcomponent:  make(map[model.CaptureID]*SubComponent),
		captureInfo:   make(map[model.CaptureID]*model.CaptureInfo),
	}
	return c
}

type SubComponent struct {
	lastHeartbeatTime time.Time
}

func (c *ComponentMaintainer) Bootstrap() error {
	// 1. 向所有的 alive capture 发送 bootstrap 消息，直到所有的都回复了
	// 2. 变成可用前处理 新的capture 上线，新上线的 capture 也要回复
	// 3. 处理下线的 capture, capture 下线直接删掉
	return nil
}

func (c *ComponentMaintainer) Name() string {
	return c.name
}

func (c *ComponentMaintainer) Version() int {
	return c.version
}

func (c *ComponentMaintainer) GetMasterComponentBootstrapTopic() string {
	return fmt.Sprintf("bootstrap/%s/master", c.name)
}

func (c *ComponentMaintainer) GetCaptureComponentManagerBootstrapTopic() string {
	return fmt.Sprintf("bootstrap/%s/manager", c.name)
}

// 向所有的节点都发送一个 heartbeat 事件，收集所有的子组件
func (c *ComponentMaintainer) Broadcast(ctx context.Context, captures []model.Capture) error {
	for _, capture := range captures {
		// 发送信息到 capture
		client := c.messageRouter.GetClient(capture.ID)
		_, err := client.TrySendMessage(ctx, c.GetMasterComponentBootstrapTopic(),
			new_arch.Message{BootstrapRequest: &new_arch.BootstrapRequest{}})
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *ComponentMaintainer) GetActiveSubComponents(captureID model.Capture, component *SubComponent) {

}

// UpdateCaptureInfo update the latest alive capture info.
// Returns true if capture info has changed.
func (c *ComponentMaintainer) UpdateCaptureInfo(
	aliveCaptures map[model.CaptureID]*model.CaptureInfo,
) bool {
	if len(aliveCaptures) != len(c.captureInfo) {
		c.captureInfo = aliveCaptures
		return true
	}
	for id, alive := range aliveCaptures {
		info, ok := c.captureInfo[id]
		if !ok || info.Version != alive.Version {
			c.captureInfo = aliveCaptures
			return true
		}
	}
	return false
}
