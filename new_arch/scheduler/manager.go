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

package scheduler

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/new_arch"
)

// ComponentStatus is the state in component side
// Absent -> Preparing -> Prepared -> Working -> Stopping -> Stopped
// todo: define it in pb file
type ComponentStatus int

// SchedulerComponentStatus is the state in scheduler side
type SchedulerComponentStatus int

const (
	SchedulerComponentStatusAbsent SchedulerComponentStatus = iota
	SchedulerComponentStatusPrepare
	SchedulerComponentStatusCommit
	SchedulerComponentStatusWorking
	SchedulerComponentStatusRemoving
)

type Component struct {
	PrimaryCaptureID string
	// move component to another capture
	SecondaryCaptureID string
	Status             SchedulerComponentStatus
}

// 组件自身的调度变化
func (c *Component) handleMoveComponent() ([]*new_arch.Message, error) {
	return nil, nil
}
func (c *Component) handleAddComponent() ([]*new_arch.Message, error) {
	return nil, nil
}
func (c *Component) handleRemoveComponent() ([]*new_arch.Message, error) {
	return nil, nil
}

// 组件依赖变化
func (c *Component) handleCaptureDown() ([]*new_arch.Message, error) {
	return nil, nil
}

// 组件状态上报产生的变化
func (c *Component) handleStatusChanged() ([]*new_arch.Message, error) {
	return nil, nil
}

// 最终所有的状态都由这个函数来做组件状态机驱动
// 对比自身的调度状态和组件上报的状态
func (c *Component) poll(status *ComponentStatus) ([]*new_arch.Message, error) {
	return []*new_arch.Message{}, nil
}

// 判断是否被移除了
func (c *Component) isRemoved(status *ComponentStatus) bool {
	return false
}

// ComponentManager 管理所有的组件，并驱动组件状态机
type ComponentManager struct {
	// 创建好了的组件状态机
	components   map[int] /*componentID*/ Component
	runningTasks map[int] /*componentID*/ *ScheduleTask
}

// capture 变更，新加或者下线
func (m *ComponentManager) HandleCaptureChanges(init map[model.CaptureID]*Component,
	removed []model.CaptureID) ([]*new_arch.Message, error) {
	// c.handleCaptureDown()
	return nil, nil
}

// 组件状态的变化处理， 1. 客户端上报heartbeat 来更新状态， 2. 定期检查所有状态是否存活，如果不存活了，需要模拟一个stop的消息
func (m *ComponentManager) HandleStatusUpdated() ([]*new_arch.Message, error) {
	return nil, nil
}

// 处理正常的调度任务
func (m *ComponentManager) HandleTask(tasks []*ScheduleTask) ([]*new_arch.Message, error) {
	return nil, nil
}

type ScheduleTask struct {
	//AddComponent
	//RemoveComponent
	//MoveComponent
	//BurstTasks
}

type ClientComponent struct {
	Status ComponentStatus
}

type ClientComponentManager struct {
	components []ClientComponent
}

const (
	ComponentStatusUnknown ComponentStatus = iota
	ComponentStatusAbsent
	ComponentStatusPreparing
	ComponentStatusPrepared
	ComponentStatusWorking
	ComponentStatusStopping
	ComponentStatusStopped
)
