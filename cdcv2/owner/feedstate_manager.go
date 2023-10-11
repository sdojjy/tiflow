// Copyright 2023 PingCAP, Inc.
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

package owner

import (
	"github.com/pingcap/tiflow/cdc/model"
)

type feedStateManagerImpl struct {
	finished bool
}

func newFeedStateManager() *feedStateManagerImpl {
	return &feedStateManagerImpl{}
}

func (f feedStateManagerImpl) PushAdminJob(job *model.AdminJob) {
	//TODO implement me
}

func (f feedStateManagerImpl) Tick(resolvedTs model.Ts) bool {
	//TODO implement me
	return false
}

func (f feedStateManagerImpl) HandleError(errs ...*model.RunningError) {
	//TODO implement me
}

func (f feedStateManagerImpl) HandleWarning(warnings ...*model.RunningError) {
	//TODO implement me
}

func (f feedStateManagerImpl) ShouldRunning() bool {
	//TODO implement me
	return !f.finished
}

func (f feedStateManagerImpl) ShouldRemoved() bool {
	//TODO implement me
	return false
}

func (f feedStateManagerImpl) MarkFinished() {
	//TODO implement me
	f.finished = true
}