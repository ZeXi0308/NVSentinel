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
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/nvidia/nvsentinel/commons/pkg/statemanager"
	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/common"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/config"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/healthEventsAnnotation"
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
	// Successful effects are remembered until scope exit. Completing a repair
	// does not issue another request or accept a delayed duplicate.
	effects map[string]string
}
type gpuRechecker struct {
	client   kubernetes.Interface
	config   config.GPUHealthRecheck
	selector labels.Selector
	mu       sync.Mutex
	enrolled map[string]*gpuRecheckEnrollment
}

func newGPURechecker(client kubernetes.Interface, cfg config.GPUHealthRecheck) (*gpuRechecker, error) {
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
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := c.reconcile(attempt)

		cancel()

		if err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "GPU health recheck enrollment failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Reconcile membership, not fault history. Monitor callbacks provide the new
// observation through the ordinary health-event stream.
func (c *gpuRechecker) reconcile(ctx context.Context) error {
	nodes, err := c.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: c.config.NodeSelector})
	if err != nil {
		return fmt.Errorf("list recheck nodes: %w", err)
	}

	pods, err := c.client.CoreV1().Pods(c.config.Namespace).List(ctx,
		metav1.ListOptions{LabelSelector: c.config.PodSelector})
	if err != nil {
		return fmt.Errorf("list GPU monitors: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	present := make(map[string]bool, len(nodes.Items))

	var failures []error

	for i := range nodes.Items {
		node := &nodes.Items[i]

		present[node.Name] = true
		if err := c.enroll(ctx, node, pods.Items); err != nil {
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

func (c *gpuRechecker) enroll(ctx context.Context, node *corev1.Node, pods []corev1.Pod) error {
	entry := c.enrolled[node.Name]
	if entry == nil || entry.request.NodeUID != string(node.UID) {
		entry = &gpuRecheckEnrollment{
			request: gpuRecheckRequest{ID: rand.Text(), NodeUID: string(node.UID), ErrorCodes: []string{gpuRecheckECC}},
			skipped: !recheckEligible(node), effects: make(map[string]string),
		}
		c.enrolled[node.Name] = entry
	}

	if entry.skipped || entry.podUID != "" {
		return nil
	}
	// Clearing a hold or finishing repair does not rearm the enrollment.
	if !recheckEligible(node) {
		entry.skipped = true
		return nil
	}

	pod := recheckMonitorPod(node.Name, pods)
	if pod == nil {
		return nil
	}

	if err := c.writeRequest(ctx, pod, entry.request); err != nil {
		return err
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
		if pod.Spec.NodeName != nodeName || pod.DeletionTimestamp != nil || owner == nil {
			continue
		}

		if owner.Kind != "DaemonSet" || owner.APIVersion != "apps/v1" {
			continue
		}

		if candidate != nil {
			return nil
		} // Wait for a monitor rollout to settle.

		candidate = pod
	}

	return candidate
}

func (c *gpuRechecker) writeRequest(ctx context.Context, pod *corev1.Pod, request gpuRecheckRequest) error {
	raw, err := json.Marshal(request)
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

	_, err = c.client.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("request GPU recheck on %s: %w", pod.Spec.NodeName, err)
	}

	return nil
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
func recheckEffects(event *protos.HealthEvent) []string {
	var keys []string

	for _, entity := range event.EntitiesImpacted {
		if entity.EntityType == "GPU" && entity.EntityValue != "" {
			keys = append(keys, entity.EntityValue)
		}
	}

	return keys
}
func recheckLinked(node *corev1.Node, event *protos.HealthEvent) bool {
	events := healthEventsAnnotation.NewHealthEventsAnnotationMap()

	raw := node.Annotations[common.QuarantineHealthEventAnnotationKey]
	if event.Id == "" || json.Unmarshal([]byte(raw), events) != nil {
		return false
	}

	for _, current := range events.Events {
		if current.Id == event.Id {
			return true
		}
	}

	return false
}

// Called again inside the optimistic node patch, against the exact Node being
// changed, so a concurrent scope/UID/manual-hold change cannot pass a stale read.
func (c *gpuRechecker) guard(node *corev1.Node, event *protos.HealthEvent) error {
	if !c.selector.Matches(labels.Set(node.Labels)) || !recheckIdentityValid(node, event) {
		return errStaleGPURecheck
	}

	if recheckLinked(node, event) {
		return nil
	} // Resume an already recorded quarantine after controller restart.

	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.enrolled[node.Name]
	if entry == nil || entry.skipped || entry.podUID == "" || entry.request.ID != event.Metadata[gpuRecheckRequestKey] {
		return errStaleGPURecheck
	}

	return entry.accepts(node, event)
}

func recheckIdentityValid(node *corev1.Node, event *protos.HealthEvent) bool {
	return string(node.UID) == event.Metadata[gpuRecheckNodeKey] && !recheckManualHold(node)
}

func (entry *gpuRecheckEnrollment) accepts(node *corev1.Node, event *protos.HealthEvent) error {
	if len(entry.effects) == 0 {
		if !recheckEligible(node) {
			return errStaleGPURecheck
		}
	} else if isEmptyHealthEventAnnotation(node.Annotations[common.QuarantineHealthEventAnnotationKey]) {
		return errStaleGPURecheck // Repair finished or an operator released it.
	}

	for _, key := range recheckEffects(event) {
		if entry.effects[key] != "" {
			return errStaleGPURecheck
		}
	}

	return nil
}
func (c *gpuRechecker) validate(ctx context.Context, event *protos.HealthEvent) error {
	if !recheckPayloadValid(event) {
		return errStaleGPURecheck
	}

	node, err := c.client.CoreV1().Nodes().Get(ctx, event.NodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return errStaleGPURecheck
	}

	if err != nil {
		return fmt.Errorf("read GPU recheck node: %w", err)
	}

	if err := c.guard(node, event); err != nil {
		return err
	}

	if recheckLinked(node, event) {
		return nil
	}

	return c.validateMonitor(ctx, node, event)
}

func recheckPayloadValid(event *protos.HealthEvent) bool {
	return event.Id != "" && event.Agent == "gpu-health-monitor" && event.CheckName == "GpuMemWatch" &&
		!event.IsHealthy && event.IsFatal && event.ProcessingStrategy == protos.ProcessingStrategy_EXECUTE_REMEDIATION &&
		len(event.ErrorCode) == 1 && event.ErrorCode[0] == gpuRecheckECC && len(recheckEffects(event)) > 0
}

func (c *gpuRechecker) validateMonitor(ctx context.Context, node *corev1.Node, event *protos.HealthEvent) error {
	c.mu.Lock()

	entry := c.enrolled[node.Name]
	if entry == nil {
		c.mu.Unlock()
		return errStaleGPURecheck
	}

	podName, podUID := entry.podName, entry.podUID
	c.mu.Unlock()

	pod, err := c.client.CoreV1().Pods(c.config.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return errStaleGPURecheck
	}

	if err != nil {
		return fmt.Errorf("read GPU recheck monitor: %w", err)
	}

	var request gpuRecheckRequest
	if pod.UID != podUID || pod.DeletionTimestamp != nil || pod.Spec.NodeName != node.Name ||
		json.Unmarshal([]byte(pod.Annotations[gpuRecheckAnnotation]), &request) != nil ||
		request.ID != event.Metadata[gpuRecheckRequestKey] || request.NodeUID != string(node.UID) {
		return errStaleGPURecheck
	}

	return nil
}
func (c *gpuRechecker) complete(event *protos.HealthEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.enrolled[event.NodeName]
	if entry != nil && entry.request.ID == event.Metadata[gpuRecheckRequestKey] {
		for _, key := range recheckEffects(event) {
			entry.effects[key] = event.Id
		}
	}
}

func (r *Reconciler) initializeGPURecheck() error {
	if !r.config.TomlConfig.GPUHealthRecheck.Enabled || r.config.DryRun {
		return nil
	}

	var err error

	r.gpuRecheck, err = newGPURechecker(r.k8sClient.Clientset, r.config.TomlConfig.GPUHealthRecheck)

	return err
}

func (r *Reconciler) gpuRecheckGuard(ctx context.Context, event *protos.HealthEvent) (func(*corev1.Node) error, error) {
	if event.Metadata[gpuRecheckRequestKey] == "" {
		return nil, nil
	}

	if r.gpuRecheck == nil {
		return r.recordedRecheckGuard(ctx, event)
	}

	if err := r.gpuRecheck.validate(ctx, event); err != nil {
		return nil, err
	}

	return func(node *corev1.Node) error { return r.gpuRecheck.guard(node, event) }, nil
}

// Turning off new requests preserves recovery of already recorded quarantines.
func (r *Reconciler) recordedRecheckGuard(
	ctx context.Context, event *protos.HealthEvent,
) (func(*corev1.Node) error, error) {
	guard := func(node *corev1.Node) error {
		if !recheckIdentityValid(node, event) || !recheckLinked(node, event) {
			return errStaleGPURecheck
		}

		return nil
	}

	node, err := r.k8sClient.Clientset.CoreV1().Nodes().Get(ctx, event.NodeName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, errStaleGPURecheck
	}

	if err != nil {
		return nil, fmt.Errorf("read recorded GPU recheck: %w", err)
	}

	return guard, guard(node)
}

func (r *Reconciler) finishGPURecheck(
	event *protos.HealthEvent, status *model.Status, err error,
) (*model.Status, error) {
	if errors.Is(err, errStaleGPURecheck) {
		return nil, nil
	}

	if err == nil && r.gpuRecheck != nil && event.Metadata[gpuRecheckRequestKey] != "" {
		r.gpuRecheck.complete(event)
	}

	return status, err
}
