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

package reconciler

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/nvidia/nvsentinel/commons/pkg/statemanager"
	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/common"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/config"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/evaluator"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/healthEventsAnnotation"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/informer"
)

const (
	gpuRecheckAnnotation = "nvsentinel.nvidia.com/gpu-health-recheck"
	gpuRecheckRequestKey = "gpuRecheckRequest"
	gpuRecheckNodeKey    = "gpuRecheckNodeUID"
	gpuRecheckECC        = "DCGM_FR_VOLATILE_DBE_DETECTED"
)

var errStaleGPURecheck = errors.New("GPU recheck no longer belongs to an eligible enrollment")

type gpuRecheckRequest struct {
	ID         string   `json:"id"`
	NodeUID    string   `json:"nodeUID"`
	ErrorCodes []string `json:"errorCodes"`
}

type gpuRecheckEnrollment struct {
	request gpuRecheckRequest
	skipped bool
	podUID  types.UID
	podName string
	handled bool
}

type gpuRechecker struct {
	client   *informer.FaultQuarantineClient
	config   config.GPUHealthRecheck
	selector labels.Selector
	mu       sync.Mutex
	enrolled map[string]*gpuRecheckEnrollment
}

func newGPURechecker(client *informer.FaultQuarantineClient, cfg config.GPUHealthRecheck) (*gpuRechecker, error) {
	if cfg.NodeSelector == "" || cfg.Namespace == "" || cfg.PodSelector == "" {
		return nil, fmt.Errorf("gpuHealthRecheck requires nodeSelector, namespace and podSelector")
	}

	selector, err := labels.Parse(cfg.NodeSelector)
	if err != nil || selector.Empty() {
		return nil, fmt.Errorf("invalid GPU recheck node selector: %q", cfg.NodeSelector)
	}

	pods, err := labels.Parse(cfg.PodSelector)
	if err != nil || pods.Empty() {
		return nil, fmt.Errorf("invalid GPU recheck pod selector: %q", cfg.PodSelector)
	}

	return &gpuRechecker{
		client: client, config: cfg, selector: selector,
		enrolled: make(map[string]*gpuRecheckEnrollment),
	}, nil
}

func (c *gpuRechecker) run(ctx context.Context) {
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		if err := c.reconcile(attempt); err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "GPU health recheck enrollment failed", "error", err)
		}
	}, 30*time.Second)
}

// The existing informer supplies membership. Only enrollments still waiting for
// a monitor make API calls; completing repair does not rearm an enrollment.
// This opt-in requester supports one FQ replica.
func (c *gpuRechecker) reconcile(ctx context.Context) error {
	nodes, err := c.client.NodeInformer.Lister().List(c.selector)
	if err != nil {
		return fmt.Errorf("list cached recheck nodes: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	present := make(map[string]bool, len(nodes))

	var failures []error

	for _, node := range nodes {
		present[node.Name] = true
		if err := c.enroll(ctx, node); err != nil {
			failures = append(failures, err)
		}
	}

	for name := range c.enrolled {
		if !present[name] {
			delete(c.enrolled, name)
		}
	}

	return errors.Join(failures...)
}

func (c *gpuRechecker) enroll(ctx context.Context, node *corev1.Node) error {
	entry := c.enrolled[node.Name]
	if entry == nil || entry.request.NodeUID != string(node.UID) {
		entry = &gpuRecheckEnrollment{
			request: gpuRecheckRequest{ID: rand.Text(), NodeUID: string(node.UID), ErrorCodes: []string{gpuRecheckECC}},
			skipped: !recheckEligible(node),
		}
		c.enrolled[node.Name] = entry
	}

	if entry.skipped || entry.podUID != "" {
		return nil
	}

	return c.request(ctx, node, entry)
}

func (c *gpuRechecker) request(ctx context.Context, node *corev1.Node, entry *gpuRecheckEnrollment) error {
	current, err := c.client.Clientset.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read recheck node: %w", err)
	}

	if current.UID != node.UID || !c.selector.Matches(labels.Set(current.Labels)) || !recheckEligible(current) {
		entry.skipped = true
		return nil
	}

	pods, err := c.client.Clientset.CoreV1().Pods(c.config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: c.config.PodSelector, FieldSelector: "spec.nodeName=" + node.Name,
	})
	if err != nil {
		return fmt.Errorf("list GPU monitor on %s: %w", node.Name, err)
	}

	pod := recheckMonitorPod(node.Name, pods.Items)
	if pod == nil {
		return nil
	}

	raw, err := json.Marshal(entry.request)
	if err != nil {
		return fmt.Errorf("encode GPU recheck request: %w", err)
	}

	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{
		"uid": pod.UID, "resourceVersion": pod.ResourceVersion,
		"annotations": map[string]string{gpuRecheckAnnotation: string(raw)},
	}})
	if err != nil {
		return fmt.Errorf("encode GPU recheck patch: %w", err)
	}

	_, err = c.client.Clientset.CoreV1().Pods(pod.Namespace).Patch(
		ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("request GPU recheck on %s: %w", node.Name, err)
	}

	entry.podUID, entry.podName = pod.UID, pod.Name
	slog.InfoContext(ctx, "Requested fresh GPU ECC observation", "node", node.Name, "request", entry.request.ID)

	return nil
}

func recheckMonitorPod(nodeName string, pods []corev1.Pod) *corev1.Pod {
	var candidate *corev1.Pod

	for i := range pods {
		pod := &pods[i]

		owner := metav1.GetControllerOf(pod)
		if pod.Spec.NodeName != nodeName || pod.DeletionTimestamp != nil || owner == nil ||
			owner.Kind != "DaemonSet" || owner.APIVersion != "apps/v1" {
			continue
		}

		if candidate != nil {
			return nil // Wait for a monitor rollout to settle.
		}

		candidate = pod
	}

	return candidate
}

func recheckManualHold(node *corev1.Node) bool {
	return node.DeletionTimestamp != nil ||
		node.Annotations[common.QuarantinedNodeUncordonedManuallyAnnotationKey] != "" ||
		node.Annotations[common.QuarantinedNodeIsUntaintedManuallyAnnotationKey] != ""
}

func recheckEligible(node *corev1.Node) bool {
	return !recheckManualHold(node) && !node.Spec.Unschedulable &&
		node.Labels[statemanager.NVSentinelStateLabelKey] == "" &&
		isEmptyHealthEventAnnotation(node.Annotations[common.QuarantineHealthEventAnnotationKey])
}

// The quarantine annotation already owns per-GPU deduplication and recovery.
// Use it to recognize this repair, including after an FQ restart.
func recheckRecorded(node *corev1.Node, event *protos.HealthEvent) (linked, sameRequest bool) {
	events := healthEventsAnnotation.NewHealthEventsAnnotationMap()
	if json.Unmarshal([]byte(node.Annotations[common.QuarantineHealthEventAnnotationKey]), events) != nil {
		return false, false
	}

	for _, current := range events.Events {
		linked = linked || current.Id == event.Id
		sameRequest = sameRequest || (current.Metadata[gpuRecheckRequestKey] == event.Metadata[gpuRecheckRequestKey] &&
			current.Metadata[gpuRecheckNodeKey] == event.Metadata[gpuRecheckNodeKey])
	}

	return linked, sameRequest
}

// Runs again against the live Node inside the optimistic quarantine patch.
// A nil requester permits only recovery of an already-recorded event.
func (c *gpuRechecker) guard(node *corev1.Node, event *protos.HealthEvent) error {
	if string(node.UID) != event.Metadata[gpuRecheckNodeKey] || recheckManualHold(node) ||
		(c != nil && !c.selector.Matches(labels.Set(node.Labels))) {
		return errStaleGPURecheck
	}

	linked, sameRequest := recheckRecorded(node, event)
	if linked {
		return nil
	}

	if c == nil {
		return errStaleGPURecheck
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.enrolled[node.Name].accepts(node, event, sameRequest)
}

func (entry *gpuRecheckEnrollment) accepts(node *corev1.Node, event *protos.HealthEvent, sameRequest bool) error {
	if entry == nil || entry.skipped || entry.podUID == "" || entry.request.ID != event.Metadata[gpuRecheckRequestKey] {
		return errStaleGPURecheck
	}

	if (!entry.handled && recheckEligible(node)) || sameRequest {
		return nil
	}

	return errStaleGPURecheck
}

func recheckPayloadValid(event *protos.HealthEvent) bool {
	return event.Id != "" && event.Agent == "gpu-health-monitor" && event.CheckName == "GpuMemWatch" &&
		!event.IsHealthy && event.IsFatal && event.ProcessingStrategy == protos.ProcessingStrategy_EXECUTE_REMEDIATION &&
		slices.Equal(event.ErrorCode, []string{gpuRecheckECC}) &&
		len(event.EntitiesImpacted) > 0 && !slices.ContainsFunc(event.EntitiesImpacted, func(e *protos.Entity) bool {
		return e.GetEntityType() != "GPU" || e.GetEntityValue() == ""
	})
}

func (c *gpuRechecker) validateMonitor(ctx context.Context, event *protos.HealthEvent) error {
	c.mu.Lock()

	entry := c.enrolled[event.NodeName]
	if entry == nil {
		c.mu.Unlock()
		return errStaleGPURecheck
	}

	podName, podUID := entry.podName, entry.podUID
	c.mu.Unlock()

	pod, err := c.client.Clientset.CoreV1().Pods(c.config.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return errStaleGPURecheck
	}

	if err != nil {
		return fmt.Errorf("read GPU recheck monitor: %w", err)
	}

	var request gpuRecheckRequest
	if pod.UID != podUID || pod.DeletionTimestamp != nil || pod.Spec.NodeName != event.NodeName ||
		json.Unmarshal([]byte(pod.Annotations[gpuRecheckAnnotation]), &request) != nil ||
		request.ID != event.Metadata[gpuRecheckRequestKey] || request.NodeUID != event.Metadata[gpuRecheckNodeKey] {
		return errStaleGPURecheck
	}

	return nil
}

func (c *gpuRechecker) complete(event *protos.HealthEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry := c.enrolled[event.NodeName]; entry != nil && entry.request.ID == event.Metadata[gpuRecheckRequestKey] {
		entry.handled = true
	}
}

func (r *Reconciler) initializeGPURecheck() error {
	if !r.config.TomlConfig.GPUHealthRecheck.Enabled || r.config.DryRun {
		return nil
	}

	var err error

	r.gpuRecheck, err = newGPURechecker(r.k8sClient, r.config.TomlConfig.GPUHealthRecheck)

	return err
}

func (r *Reconciler) gpuRecheckGuard(ctx context.Context, event *protos.HealthEvent) (func(*corev1.Node) error, error) {
	if event.Metadata[gpuRecheckRequestKey] == "" {
		return nil, nil
	}

	if !recheckPayloadValid(event) {
		return nil, errStaleGPURecheck
	}

	node, err := r.k8sClient.Clientset.CoreV1().Nodes().Get(ctx, event.NodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, errStaleGPURecheck
	}

	if err != nil {
		return nil, fmt.Errorf("read GPU recheck node: %w", err)
	}

	guard := func(node *corev1.Node) error { return r.gpuRecheck.guard(node, event) }
	if err := guard(node); err != nil {
		return nil, err
	}

	if linked, _ := recheckRecorded(node, event); !linked {
		if err := r.gpuRecheck.validateMonitor(ctx, event); err != nil {
			return nil, err
		}
	}

	return guard, nil
}

func (r *Reconciler) handleEventWithGPURecheck(
	ctx context.Context, event *model.HealthEventWithStatus,
	evals []evaluator.RuleSetEvaluatorIface, rules rulesetsConfig,
) (*model.Status, error) {
	guard, err := r.gpuRecheckGuard(ctx, event.HealthEvent)

	var status *model.Status

	if err == nil {
		status, err = r.handleEvent(informer.WithNodeGuard(ctx, guard), event, evals, rules)
		if err == nil && guard != nil && r.gpuRecheck != nil {
			r.gpuRecheck.complete(event.HealthEvent)
		}
	}

	if errors.Is(err, errStaleGPURecheck) {
		return nil, nil
	}

	return status, err
}
