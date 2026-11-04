// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package customdrain

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/nvidia/nvsentinel/node-drainer/pkg/config"
)

func TestCompletionMatchesObservedGeneration(t *testing.T) {
	c := &Client{config: config.CustomDrainConfig{StatusConditionType: "DrainComplete", StatusConditionStatus: "True"}}
	for _, tc := range []struct {
		name     string
		observed any
		present  bool
		want     bool
	}{{"current", int64(2), true, true}, {"stale", int64(1), true, false}, {"legacy", nil, false, true}, {"malformed", "2", true, false}, {"explicit-null", nil, true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			condition := map[string]any{"type": "DrainComplete", "status": "True"}
			if tc.present {
				condition["observedGeneration"] = tc.observed
			}
			cr := unstructured.Unstructured{Object: map[string]any{
				"metadata": map[string]any{"generation": int64(2)},
				"status":   map[string]any{"conditions": []any{condition}},
			}}
			require.Equal(t, tc.want, c.isCRComplete(cr))
		})
	}
}
