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

package table_range_maintainer

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
	"time"
)

type TableRangeMaintainer struct {
	upstreamManager *upstream.Manager
	cfg             *config.SchedulerConfig
	globalVars      *vars.GlobalVars
	tables          []model.TableID
	ID              model.ChangeFeedID
	info            *model.ChangeFeedInfo
	status          *model.ChangeFeedStatus

	maintainerStatus string
}

func NewTableRangeMaintainer(ID model.ChangeFeedID,
	upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	globalVars *vars.GlobalVars,
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus,
	tables []model.TableID) *TableRangeMaintainer {
	m := &TableRangeMaintainer{
		upstreamManager: upstreamManager,
		cfg:             cfg,
		globalVars:      globalVars,
		tables:          tables,
		ID:              ID,
		info:            info,
		status:          status,
	}
	_, _ = globalVars.MessageServer.SyncAddHandler(context.Background(),
		new_arch.GetTableRangeMaintainerTopic(ID),
		&new_arch.Message{}, func(sender string, messageI interface{}) error {
			message := messageI.(*new_arch.Message)
			m.HandleMessage(sender, message)
			return nil
		})
	return m
}

func (c *TableRangeMaintainer) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 50)
	loged := false
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-tick.C:
			if c.maintainerStatus == "running" {
				if !loged {
					log.Info("table range maintainer is running",
						zap.String("ID", c.ID.String()))
					loged = true
				}
			}
		}
	}
}

func (m *TableRangeMaintainer) HandleMessage(send string, msg *new_arch.Message) {

}

func (m *TableRangeMaintainer) SendMessage(ctx context.Context, capture string, topic string, msg *new_arch.Message) error {
	client := m.globalVars.MessageRouter.GetClient(capture)
	_, err := client.TrySendMessage(ctx, topic, msg)
	return errors.Trace(err)
}
