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
	"fmt"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/config"
)

// Coordinator is the interface to implement the logic of ticdc coordinator
// coordinator handles:
// 1. changefeed operations and queries
// 2. changefeed maintainer management
// 3. rebalance tables
type Coordinator interface {
	Querer

	Run(ctx context.Context) error
	AsyncStop()

	// api layer alreayd saved to etcd, coordinator is a etcd worker, and compares memory and etcd info,
	OnChangefeedCreated(ctx context.Context, ID model.ChangeFeedID, config config.ReplicaConfig) error
	OnChangefeedPaused(ctx context.Context, ID model.ChangeFeedID) error
	OnChangefeedResumed(ctx context.Context, ID model.ChangeFeedID) error
	OnChangefeedRemoved(ctx context.Context, ID model.ChangeFeedID) error

	CreateChangefeedMaintainer(ctx context.Context, ID model.ChangeFeedID,
		config config.ReplicaConfig, capture model.Capture) error
}

type ChangefeedMaintainerManager interface {
	CreateChangefeedMaintainer(ctx context.Context, ID model.ChangeFeedID,
		config config.ReplicaConfig, capture model.Capture) error
}

type ChangefeedMaintainer interface {
}

type TableRaangeMaintainerManager interface {
	CreateTableRangeMaintainer(ctx context.Context, ID model.ChangeFeedID, config config.ReplicaConfig)
	RemoveTableRangeMaintainer(ctx context.Context, ID model.ChangeFeedID)
}

type TableRangeMaintainer interface {
	AddTables(ctx context.Context, tables []tablepb.Span)
	RemoveTables(ctx context.Context, tables []tablepb.Span)
}

// for Open API query
type Querer interface {
	ListChangefeeds(ctx context.Context) ([]model.ChangefeedCommonInfo, error)
	QueryChangefeed(ctx context.Context, id model.ChangeFeedID) (model.ChangefeedDetail, error)
	GetChangeFeedSyncedStatus(ctx context.Context, id model.ChangeFeedID) (model.ChangeFeedSyncedStatusForAPI, error)

	ListProcessors(ctx context.Context) ([]model.ProcessorDetail, error)
	ListCaptures(ctx context.Context) ([]model.ProcessorDetail, error)
	IsHealthy(ctx context.Context) bool
}

func GetCoordinatorTopic() string {
	return "coordinator"
}

func GetChangefeedMaintainerManagerTopic() string {
	return "changefeedMaintainerManager"
}

func GetChangefeedMaintainerTopic(ID model.ChangeFeedID) string {
	return fmt.Sprintf("changefeedMaintainer/%s/%s", ID.Namespace, ID.ID)
}

func GetTableRangeMaintainerManagerTopic() string {
	return "tableRangeMaintainerManager"
}

func GetTableRangeMaintainerTopic(ID model.ChangeFeedID) string {
	return fmt.Sprintf("tableRangeMaintainer/%s/%s", ID.Namespace, ID.ID)
}
