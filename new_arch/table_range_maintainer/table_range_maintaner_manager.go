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
)

type TableRangeMaintainerManager struct {
	maintainers map[string]*TableRangeMaintainer

	upstreamManager *upstream.Manager
	cfg             *config.SchedulerConfig
	globalVars      *vars.GlobalVars
}

func NewTableRangeMaintainerManager(upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	globalVars *vars.GlobalVars) *TableRangeMaintainerManager {
	m := &TableRangeMaintainerManager{
		upstreamManager: upstreamManager,
		cfg:             cfg,
		globalVars:      globalVars,
		maintainers:     make(map[string]*TableRangeMaintainer),
	}
	_, _ = globalVars.MessageServer.SyncAddHandler(context.Background(), new_arch.GetTableRangeMaintainerManagerTopic(),
		&new_arch.Message{}, func(sender string, messageI interface{}) error {
			message := messageI.(*new_arch.Message)
			m.HandleMessage(sender, message)
			return nil
		})
	return m
}

func (m *TableRangeMaintainerManager) HandleMessage(send string, msg *new_arch.Message) {
	if msg.AddTableRangeMaintainerRequest != nil {
		req := msg.AddTableRangeMaintainerRequest
		maintainer := NewTableRangeMaintainer(model.DefaultChangeFeedID(req.Config.ID),
			m.upstreamManager, m.cfg, m.globalVars,
			req.Config,
			req.Status, req.Tables)
		m.maintainers[req.Config.ID] = maintainer
		maintainer.SendMessage(context.Background(), send,
			new_arch.GetChangefeedMaintainerTopic(model.DefaultChangeFeedID(req.Config.ID)),
			&new_arch.Message{
				AddTableRangeMaintainerResponse: &new_arch.AddTableRangeMaintainerResponse{
					ID:     msg.AddTableRangeMaintainerRequest.Config.ID,
					Status: "running",
				},
			})
		log.Info("table range maintainer sent",
			zap.String("id", msg.AddTableRangeMaintainerRequest.Config.ID))
	}
}

func (m *TableRangeMaintainerManager) SendMessage(ctx context.Context, capture string, topic string, msg *new_arch.Message) error {
	client := m.globalVars.MessageRouter.GetClient(capture)
	_, err := client.TrySendMessage(ctx, topic, msg)
	return errors.Trace(err)
}
