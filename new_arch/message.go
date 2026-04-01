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
	"github.com/pingcap/tiflow/cdc/model"
)

type MessageHeader struct {
	SenderVersion int64 `json:"sender_version,omitempty,string"`
	SenderEpoch   int64 `json:"sender_epoch,omitempty,string"`
}

type Message struct {
	Header *MessageHeader `json:"header"`
	Type   string         `json:"type"`

	MasterVersion int64  `json:"master_version,omitempty"`
	From          string `json:"sender,omitempty"`
	To            string `json:"to,omitempty"`

	DispatchMaintainerRequest *DispatchMaintainerRequest `json:"dispatch_maintainer_request,omitempty"`

	AddTableRangeMaintainerRequest  *AddTableRangeMaintainerRequest  `json:"add_table_range_maintainer_request,omitempty"`
	AddTableRangeMaintainerResponse *AddTableRangeMaintainerResponse `json:"add_table_range_maintainer_response,omitempty"`

	BootstrapRequest *BootstrapRequest `json:"bootstrap_request,omitempty"`

	DispatchComponentRequest *DispatchComponentRequest `json:"msg_dispatch_component_request,omitempty"`

	ChangefeedHeartbeatResponse *ChangefeedHeartbeatResponse `json:"changefeed_heartbeat_response,omitempty"`
}

type DispatchMaintainerRequest struct {
	AddMaintainerRequest         *AddMaintainerRequest         `json:"add_maintainer_request,omitempty"`
	RemoveMaintainerRequest      *RemoveMaintainerRequest      `json:"remove_maintainer_request,omitempty"`
	BatchAddMaintainerRequest    *BatchAddMaintainerRequest    `json:"batch_add_maintainer_request,omitempty"`
	BatchRemoveMaintainerRequest *BatchRemoveMaintainerRequest `json:"batch_remove_maintainer_request,omitempty"`
}

type BatchAddMaintainerRequest struct {
	Requests []*AddMaintainerRequest `json:"requests,omitempty"`
}
type BatchRemoveMaintainerRequest struct {
	Requests []*RemoveMaintainerRequest `json:"requests,omitempty"`
}

type DispatchComponentRequest struct {
	AddComponent    []byte `json:"add_component,omitempty"`
	RemoveComponent []byte `json:"remove_component,omitempty"`
}

type BootstrapRequest struct {
}

type AddTableRangeMaintainerRequest struct {
	Tables []model.TableID         `json:"tables,omitempty"`
	Config *model.ChangeFeedInfo   `json:"config,omitempty"`
	Status *model.ChangeFeedStatus `json:"status,omitempty"`
}

type AddTableRangeMaintainerResponse struct {
	Status string `json:"status,omitempty"`
	ID     string `json:"id,omitempty"`
}

type AddMaintainerRequest struct {
	ID          model.ChangeFeedID      `json:"id,omitempty"`
	Config      *model.ChangeFeedInfo   `json:"config,omitempty"`
	Status      *model.ChangeFeedStatus `json:"status,omitempty"`
	IsSecondary bool                    `json:"is_secondary,omitempty"`
}

type RemoveMaintainerRequest struct {
	ID string `json:"id,omitempty"`
}

type ChangefeedHeartbeatResponse struct {
	Liveness    int32               `json:"liveness,omitempty"`
	Changefeeds []*ChangefeedStatus `json:"changefeeds,omitempty"`
}

type ChangefeedStatus struct {
	ID              model.ChangeFeedID `json:"id,omitempty"`
	State           string             `json:"state,omitempty"`
	ComponentStatus int                `json:"scheduler_state,omitempty"`
	CheckpointTs    uint64             `json:"checkpoint_ts,omitempty,string"`
}
