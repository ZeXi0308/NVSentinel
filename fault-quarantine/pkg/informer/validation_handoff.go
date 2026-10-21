// Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
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

package informer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/nvidia/nvsentinel/commons/pkg/cordonownership"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/common"
)

// CompleteValidationHandoff clears only the quarantine represented by the created
// request. Its validation source remains until lifecycle-manager registers it.
func (c *FaultQuarantineClient) CompleteValidationHandoff(ctx context.Context, expected *corev1.Node,
	quarantine string, annotationKeys, labelsToRemove []string) error {
	mu, _ := c.operationMutex.LoadOrStore(expected.Name, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	nodes := &handoffNodeClient{NodeInterface: c.Clientset.CoreV1().Nodes()}
	_, err := c.nodePatcher.Patch(ctx, nodes, expected.Name, nil, func(node *corev1.Node) error {
		if err := matchingHandoffNode(node, expected, quarantine); err != nil {
			return err
		}

		nodes.uid, nodes.version = node.UID, node.ResourceVersion

		for _, key := range annotationKeys {
			if key != common.QuarantineValidationHealthEventAnnotationKey && key != cordonownership.AnnotationKey &&
				key != cordonownership.ValidationSessionAnnotationKey {
				delete(node.Annotations, key)
			}
		}

		c.removeLabels(ctx, node, labelsToRemove, node.Name)

		return nil
	})

	return err
}

func matchingHandoffNode(node, expected *corev1.Node, quarantine string) error {
	if expected.UID == "" || node.UID != expected.UID {
		return fmt.Errorf("node %s identity changed during validation handoff", expected.Name)
	}

	if node.Annotations[common.QuarantineHealthEventAnnotationKey] != quarantine {
		return fmt.Errorf("node %s quarantine changed during validation handoff", expected.Name)
	}

	for _, key := range common.QuarantineAnnotationKeys {
		// Lifecycle manager can register the request before this handoff completes.
		// Its session is preserved; the producer's claim and quarantine must still match.
		if key == cordonownership.ValidationSessionAnnotationKey {
			continue
		}

		current, present := node.Annotations[key]
		previous, expectedPresent := expected.Annotations[key]

		if current != previous || present != expectedPresent {
			return fmt.Errorf("node %s quarantine changed during validation handoff", expected.Name)
		}
	}

	return nil
}

// The shared patcher versions spec changes; handoff also needs versioned metadata
// changes so a new quarantine cannot be cleared between the read and the patch.
type handoffNodeClient struct {
	typedcorev1.NodeInterface
	uid     types.UID
	version string
}

func (n *handoffNodeClient) Patch(ctx context.Context, name string, patchType types.PatchType,
	data []byte, options metav1.PatchOptions, subresources ...string) (*corev1.Node, error) {
	var patch map[string]any
	if err := json.Unmarshal(data, &patch); err != nil {
		return nil, fmt.Errorf("decode validation handoff patch: %w", err)
	}

	metadata, ok := patch["metadata"].(map[string]any)
	if !ok {
		metadata = make(map[string]any)
		patch["metadata"] = metadata
	}

	metadata["uid"] = string(n.uid)
	metadata["resourceVersion"] = n.version

	guarded, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("encode validation handoff patch: %w", err)
	}

	return n.NodeInterface.Patch(ctx, name, patchType, guarded, options, subresources...)
}
