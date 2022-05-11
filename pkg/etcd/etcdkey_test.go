// Copyright 2021 PingCAP, Inc.
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

package etcd

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestEtcdKey(t *testing.T) {
	testcases := []struct {
		key      string
		expected *CDCKey
	}{{
		key: "/tidb/cdc/default/__cdc_meta__/owner/223176cb44d20a13",
		expected: &CDCKey{
			Tp:           CDCKeyTypeOwner,
			OwnerLeaseID: "223176cb44d20a13",
			ClusterID:    "default",
		},
	}, {
		key: "/tidb/cdc/default/__cdc_meta__/owner",
		expected: &CDCKey{
			Tp:           CDCKeyTypeOwner,
			OwnerLeaseID: "",
			ClusterID:    "default",
		},
	}, {
		key: "/tidb/cdc/default/__cdc_meta__/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		expected: &CDCKey{
			Tp:        CDCKeyTypeCapture,
			CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			ClusterID: "default",
		},
	}, {
		key: "/tidb/cdc/default/default/changefeed/info/test-_@#$%changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeChangefeedInfo,
			ChangefeedID: model.DefaultChangeFeedID("test-_@#$%changefeed"),
			ClusterID:    "default",
		},
	}, {
		key: "/tidb/cdc/default/default/changefeed/info/test/changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeChangefeedInfo,
			ChangefeedID: model.DefaultChangeFeedID("test/changefeed"),
			ClusterID:    "default",
		},
	}, {
		key: "/tidb/cdc/default/default/job/test-changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeChangeFeedStatus,
			ChangefeedID: model.DefaultChangeFeedID("test-changefeed"),
			ClusterID:    "default",
		},
	}, {
		key: "/tidb/cdc/default/name/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test-changefeed",
		expected: &CDCKey{
			Tp: CDCKeyTypeTaskPosition,
			ChangefeedID: model.ChangeFeedID{
				Namespace: "name",
				ID:        "test-changefeed",
			},
			CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			ClusterID: "default",
		},
	}, {
		key: "/tidb/cdc/default/default/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test/changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeTaskPosition,
			ChangefeedID: model.DefaultChangeFeedID("test/changefeed"),
			CaptureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			ClusterID:    "default",
		},
	}}
	for _, tc := range testcases {
		k := new(CDCKey)
		err := k.Parse(tc.key)
		require.NoError(t, err)
		require.Equal(t, k, tc.expected)
		require.Equal(t, k.String(), tc.key)
	}
}

func TestEtcdKeyParseError(t *testing.T) {
	testCases := []struct {
		key   string
		error bool
	}{{
		key:   "/tidb/cdc/default/default/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test/changefeed",
		error: false,
	}, {
		key:   "/tidb/cdc/default/default/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/",
		error: false,
	}, {
		key:   "/tidb/cdc/default/default/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		error: true,
	}, {
		key:   "/tidb/cd",
		error: true,
	}, {
		key:   "/tidb/cdc/",
		error: true,
	}}
	for _, tc := range testCases {
		k := new(CDCKey)
		err := k.Parse(tc.key)
		if tc.error {
			require.NotNil(t, err)
		} else {
			require.Nil(t, err)
		}
	}
}
