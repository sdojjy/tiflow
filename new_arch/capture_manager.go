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

package new_arch

import (
	"context"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/orchestrator"
)

type CaptureManager struct {
	captures map[model.CaptureID]*model.CaptureInfo
}

func NewCaptureManager() *CaptureManager {
	return &CaptureManager{}
}

func (c *CaptureManager) Tick(ctx context.Context, rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	state := rawState.(*orchestrator.GlobalReactorState)
	c.captures = state.Captures
	return rawState, nil
}

func (c *CaptureManager) GetCaptures() map[model.CaptureID]*model.CaptureInfo {
	return c.captures
}
