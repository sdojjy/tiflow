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
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"time"
)

type TableRange struct {
	globalVars             *vars.GlobalVars
	ID                     model.ChangeFeedID
	tableRangeMaintainerID model.CaptureID
	tables                 []model.TableID
	info                   *model.ChangeFeedInfo
	status                 *model.ChangeFeedStatus

	tableRangeStatus string
}

const (
	tableRangeStatusPending  = "pending"
	tableRangeStatusStarting = "starting"
	tableRangeStatusRunning  = "running"
	tableRangeStatusStopped  = "stopped"
)

func NewTableRange(globalVars *vars.GlobalVars,
	ID model.ChangeFeedID,
	captureID model.CaptureID,
	tables []model.TableID,
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus) *TableRange {
	t := &TableRange{
		globalVars:             globalVars,
		ID:                     ID,
		tableRangeMaintainerID: captureID,
		tables:                 tables,
		info:                   info,
		tableRangeStatus:       tableRangeStatusPending,
		status:                 status,
	}
	return t
}

func (c *TableRange) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 50)
	loged := false
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-tick.C:
			if c.tableRangeStatus == tableRangeStatusPending {
				err := c.SendMessage(ctx, c.tableRangeMaintainerID,
					new_arch.GetTableRangeMaintainerManagerTopic(),
					&new_arch.Message{
						AddTableRangeMaintainerRequest: &new_arch.AddTableRangeMaintainerRequest{
							Tables: c.tables,
							Config: c.info,
							Status: c.status,
						},
					})
				if err != nil {
					return errors.Trace(err)
				}
				c.tableRangeStatus = tableRangeStatusStarting
			}
			if c.tableRangeStatus == tableRangeStatusRunning {
				if !loged {
					log.Info("table range maintainer is running",
						zap.String("ID", c.ID.String()),
						zap.String("maintainer", c.tableRangeMaintainerID))
					loged = true
				}
			}
		}
	}
}

func (c *TableRange) Stop(ctx context.Context) error {
	return nil
}

func (m *TableRange) HandleMessage(send string, msg *new_arch.Message) {

}

func (m *TableRange) SendMessage(ctx context.Context, capture string, topic string, msg *new_arch.Message) error {
	client := m.globalVars.MessageRouter.GetClient(capture)
	_, err := client.TrySendMessage(ctx, topic, msg)
	return errors.Trace(err)
}
