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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nvidia/nvsentinel/commons/pkg/statemanager"
	"github.com/nvidia/nvsentinel/data-models/pkg/model"
	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/common"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/config"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/informer"
	"github.com/nvidia/nvsentinel/fault-quarantine/pkg/nodecache"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func recheckFixture(t *testing.T) (context.Context, *gpuRechecker, *corev1.Node, *corev1.Pod) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	name := "recheck-" + generateShortTestID()
	node, err := e2eTestClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"repair-test": name}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	controller := true
	pod, err := e2eTestClient.CoreV1().Pods("default").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"monitor-test": name}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "gpu-health-monitor", UID: types.UID(name), Controller: &controller}}},
		Spec:       corev1.PodSpec{NodeName: name, Containers: []corev1.Container{{Name: "monitor", Image: "example.invalid/monitor"}}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = e2eTestClient.CoreV1().Pods("default").Delete(ctx, name, metav1.DeleteOptions{})
		_ = e2eTestClient.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{})
	})
	cfg := config.GPUHealthRecheck{Enabled: true, NodeSelector: "repair-test=" + name, Namespace: "default", PodSelector: "monitor-test=" + name}
	nodes, err := informer.NewNodeInformer(e2eTestClient, 0, informer.GPUNodeLabel, informer.GPUNodeLabelValue,
		nodecache.Derive(config.TomlConfig{GPUHealthRecheck: cfg}, nodecache.Operational{}))
	require.NoError(t, err)
	require.NoError(t, nodes.Run(ctx.Done()))
	c, err := newGPURechecker(&informer.FaultQuarantineClient{Clientset: e2eTestClient, NodeInformer: nodes}, cfg)
	require.NoError(t, err)
	return ctx, c, node, pod
}
func validateRecheck(ctx context.Context, c *gpuRechecker, event *protos.HealthEvent) error {
	r := &Reconciler{gpuRecheck: c, k8sClient: c.client}
	_, err := r.gpuRecheckGuard(ctx, event)
	return err
}

func updateRecheckNode(t *testing.T, ctx context.Context, c *gpuRechecker, node *corev1.Node) *corev1.Node {
	t.Helper()
	current, err := e2eTestClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		cached, err := c.client.NodeInformer.GetNode(node.Name)
		return err == nil && cached.ResourceVersion == current.ResourceVersion
	}, 5*time.Second, 10*time.Millisecond)
	return current
}

func TestGPURecheckEnrollment(t *testing.T) {
	ctx, c, node, pod := recheckFixture(t)
	require.NoError(t, c.reconcile(ctx))
	current, err := e2eTestClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var request map[string]any
	require.NoError(t, json.Unmarshal([]byte(current.Annotations[gpuRecheckAnnotation]), &request))
	require.Equal(t, string(node.UID), request["nodeUID"])
	require.Equal(t, []any{"DCGM_FR_VOLATILE_DBE_DETECTED"}, request["errorCodes"])
	require.NotEmpty(t, request["id"])
	require.NoError(t, c.reconcile(ctx))
	again, err := e2eTestClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, current.ResourceVersion, again.ResourceVersion, "no repeated request on each scan")
}
func TestGPURecheckSkipsExistingRepair(t *testing.T) {
	ctx, c, node, pod := recheckFixture(t)
	node.Labels[statemanager.NVSentinelStateLabelKey] = "remediation-failed"
	node = updateRecheckNode(t, ctx, c, node)
	require.NoError(t, c.reconcile(ctx))
	current, err := e2eTestClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, current.Annotations[gpuRecheckAnnotation])
}

func issuedRecheckEvent(t *testing.T, ctx context.Context, c *gpuRechecker, node *corev1.Node, pod *corev1.Pod) *protos.HealthEvent {
	t.Helper()
	require.NoError(t, c.reconcile(ctx))
	current, err := e2eTestClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var request gpuRecheckRequest
	require.NoError(t, json.Unmarshal([]byte(current.Annotations[gpuRecheckAnnotation]), &request))
	return &protos.HealthEvent{Id: generateTestID(), Agent: "gpu-health-monitor", ComponentClass: "GPU", CheckName: "GpuMemWatch", IsFatal: true, NodeName: node.Name, ProcessingStrategy: protos.ProcessingStrategy_EXECUTE_REMEDIATION, ErrorCode: []string{gpuRecheckECC}, RecommendedAction: protos.RecommendedAction_COMPONENT_RESET, GeneratedTimestamp: timestamppb.Now(), EntitiesImpacted: []*protos.Entity{{EntityType: "GPU", EntityValue: "1"}}, Metadata: map[string]string{gpuRecheckRequestKey: request.ID, gpuRecheckNodeKey: request.NodeUID}}
}

func TestGPURecheckRejectsChangedScopeAndIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*corev1.Node)
	}{
		{"scope", func(n *corev1.Node) { delete(n.Labels, "repair-test") }},
		{"manual cordon", func(n *corev1.Node) { n.Spec.Unschedulable = true }},
		{"manual release", func(n *corev1.Node) {
			n.Annotations = map[string]string{common.QuarantinedNodeUncordonedManuallyAnnotationKey: "True"}
		}},
		{"active repair", func(n *corev1.Node) { n.Labels[statemanager.NVSentinelStateLabelKey] = "remediating" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, c, node, pod := recheckFixture(t)
			event := issuedRecheckEvent(t, ctx, c, node, pod)
			require.NoError(t, validateRecheck(ctx, c, event))
			test.change(node)
			_, err := e2eTestClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
			require.NoError(t, err)
			require.ErrorIs(t, validateRecheck(ctx, c, event), errStaleGPURecheck)
		})
	}
	ctx, c, node, pod := recheckFixture(t)
	event := issuedRecheckEvent(t, ctx, c, node, pod)
	event.Metadata[gpuRecheckNodeKey] = "replaced-node"
	require.ErrorIs(t, validateRecheck(ctx, c, event), errStaleGPURecheck)
}

func TestGPURecheckEnrollmentChanges(t *testing.T) {
	ctx, c, node, pod := recheckFixture(t)
	first := issuedRecheckEvent(t, ctx, c, node, pod)
	original := node.Labels["repair-test"]
	delete(node.Labels, "repair-test")
	node = updateRecheckNode(t, ctx, c, node)
	require.NoError(t, c.reconcile(ctx))
	node.Labels = map[string]string{"repair-test": original}
	node = updateRecheckNode(t, ctx, c, node)
	second := issuedRecheckEvent(t, ctx, c, node, pod)
	require.NotEqual(t, first.Metadata[gpuRecheckRequestKey], second.Metadata[gpuRecheckRequestKey])
	require.ErrorIs(t, validateRecheck(ctx, c, first), errStaleGPURecheck)
	require.NoError(t, validateRecheck(ctx, c, second))
}

func TestGPURecheckManualHoldDoesNotRearm(t *testing.T) {
	ctx, c, node, pod := recheckFixture(t)
	node.Spec.Unschedulable = true
	node = updateRecheckNode(t, ctx, c, node)
	require.NoError(t, c.reconcile(ctx))
	node.Spec.Unschedulable = false
	node = updateRecheckNode(t, ctx, c, node)
	require.NoError(t, c.reconcile(ctx))
	current, err := e2eTestClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, current.Annotations[gpuRecheckAnnotation])
}

func TestGPURecheckNodeWriteRechecksLiveScope(t *testing.T) {
	ctx, c, node, pod := recheckFixture(t)
	event := issuedRecheckEvent(t, ctx, c, node, pod)
	require.NoError(t, validateRecheck(ctx, c, event))
	guarded := informer.WithNodeGuard(ctx, func(n *corev1.Node) error { return c.guard(n, event) })
	delete(node.Labels, "repair-test")
	_, err := e2eTestClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	client := &informer.FaultQuarantineClient{Clientset: e2eTestClient}
	err = client.UpdateNode(guarded, node.Name, func(n *corev1.Node) error { n.Spec.Unschedulable = true; return nil })
	require.ErrorIs(t, err, errStaleGPURecheck)
	current, err := e2eTestClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.False(t, current.Spec.Unschedulable)
}

func TestGPURecheckUsesNormalQuarantineAndClosesCycle(t *testing.T) {
	ctx, c, node, pod := recheckFixture(t)
	node.Labels[informer.GPUNodeLabel] = informer.GPUNodeLabelValue
	node = updateRecheckNode(t, ctx, c, node)
	event := issuedRecheckEvent(t, ctx, c, node, pod)
	cfg := config.TomlConfig{LabelPrefix: "k8s.nvidia.com/", RuleSets: []config.QuarantineRuleSet{{RuleSetMeta: config.RuleSetMeta{Enabled: true, Name: "ecc", Version: "1", Match: config.Match{All: []config.Rule{{Kind: "HealthEvent", Expression: "event.isFatal"}}}}, Cordon: config.Cordon{ShouldCordon: true}}}}
	r, _, _, _ := setupE2EReconciler(t, ctx, cfg, nil)
	r.gpuRecheck = c
	evals, err := r.initializeRuleSetEvaluators()
	require.NoError(t, err)
	rules := rulesetsConfig{CordonConfigMap: map[string]bool{"ecc": true}}
	status, err := r.ProcessEvent(ctx, &model.HealthEventWithStatus{HealthEvent: event}, evals, rules)
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, model.Quarantined, *status)
	node, err = e2eTestClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.True(t, node.Spec.Unschedulable)
	linked, _ := recheckRecorded(node, event)
	require.True(t, linked)
	disabled := &Reconciler{k8sClient: r.k8sClient}
	guard, err := disabled.gpuRecheckGuard(ctx, event)
	require.NoError(t, err)
	require.NoError(t, guard(node))
	// Cold-start recovery can finish this recorded event even after FQ restarts.
	restarted, err := newGPURechecker(c.client, c.config)
	require.NoError(t, err)
	require.NoError(t, validateRecheck(ctx, restarted, event))
	// Another GPU confirmed in the same request joins the same quarantine.
	second := proto.Clone(event).(*protos.HealthEvent)
	second.Id = generateTestID()
	second.EntitiesImpacted = []*protos.Entity{{EntityType: "GPU", EntityValue: "2"}}
	require.NoError(t, validateRecheck(ctx, c, second))
	require.Eventually(t, func() bool {
		cached, err := r.k8sClient.NodeInformer.GetNode(node.Name)
		return err == nil && cached.ResourceVersion == node.ResourceVersion
	}, 5*time.Second, 10*time.Millisecond)
	status, err = r.ProcessEvent(ctx, &model.HealthEventWithStatus{HealthEvent: second}, evals, rules)
	require.NoError(t, err)
	require.Equal(t, model.AlreadyQuarantined, *status)
	node, err = e2eTestClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	annotation := node.Annotations[common.QuarantineHealthEventAnnotationKey]
	var recorded []*protos.HealthEvent
	require.NoError(t, json.Unmarshal([]byte(annotation), &recorded))
	require.Len(t, recorded, 2)
	require.Eventually(t, func() bool {
		cached, err := r.k8sClient.NodeInformer.GetNode(node.Name)
		return err == nil && cached.ResourceVersion == node.ResourceVersion
	}, 5*time.Second, 10*time.Millisecond)
	duplicate := proto.Clone(second).(*protos.HealthEvent)
	duplicate.Id = generateTestID()
	_, err = r.ProcessEvent(ctx, &model.HealthEventWithStatus{HealthEvent: duplicate}, evals, rules)
	require.NoError(t, err)
	node, err = e2eTestClient.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.JSONEq(t, annotation, node.Annotations[common.QuarantineHealthEventAnnotationKey])
	// A release ends the request. Delayed or retried confirmations cannot cordon again.
	node.Spec.Unschedulable = false
	delete(node.Annotations, common.QuarantineHealthEventAnnotationKey)
	delete(node.Labels, statemanager.NVSentinelStateLabelKey)
	_, err = e2eTestClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.ErrorIs(t, validateRecheck(ctx, c, event), errStaleGPURecheck)
	require.ErrorIs(t, validateRecheck(ctx, c, second), errStaleGPURecheck)
	_, err = disabled.gpuRecheckGuard(ctx, event)
	require.ErrorIs(t, err, errStaleGPURecheck)
	require.NoError(t, c.reconcile(ctx))
	require.Equal(t, event.Metadata[gpuRecheckRequestKey], c.enrolled[node.Name].request.ID)
}

func TestGPURecheckWaitsForColdStartSuccess(t *testing.T) {
	ctx, c, node, pod := recheckFixture(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	watcher := &coldStartCallbackWatcherStub{}
	r := &Reconciler{gpuRecheck: c, eventWatcher: watcher, setColdStartCutoff: func(context.Context, string, time.Time) error { return nil }}
	store := &emptyHealthEventStoreStub{findErr: errors.New("store unavailable")}
	r.configureColdStart(ctx, false, false, time.Time{}, store)
	require.Error(t, watcher.callback(ctx))
	current, err := e2eTestClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, current.Annotations[gpuRecheckAnnotation])
	store.findErr = nil
	require.NoError(t, watcher.callback(ctx))
	require.Eventually(t, func() bool {
		p, err := e2eTestClient.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		return err == nil && p.Annotations[gpuRecheckAnnotation] != ""
	}, 5*time.Second, 10*time.Millisecond, node.Name)
}

func TestGPURecheckConfigRequiresExplicitScope(t *testing.T) {
	_, err := newGPURechecker(nil, config.GPUHealthRecheck{Namespace: "default", PodSelector: "app=monitor"})
	require.Error(t, err)
}

func TestGPURecheckLateConfirmationCannotJoinAnotherRepair(t *testing.T) {
	ctx, c, node, pod := recheckFixture(t)
	event := issuedRecheckEvent(t, ctx, c, node, pod)
	c.complete(event)
	// The original repair ended; a different fault now owns the quarantine.
	unrelated := proto.Clone(event).(*protos.HealthEvent)
	unrelated.Id = generateTestID()
	unrelated.Metadata = nil
	raw, err := json.Marshal([]*protos.HealthEvent{unrelated})
	require.NoError(t, err)
	node.Annotations = map[string]string{common.QuarantineHealthEventAnnotationKey: string(raw)}
	node.Spec.Unschedulable = true
	_, err = e2eTestClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	event.EntitiesImpacted = []*protos.Entity{{EntityType: "GPU", EntityValue: "2"}}
	require.ErrorIs(t, validateRecheck(ctx, c, event), errStaleGPURecheck)
}

func TestGPURecheckRejectsReplacedMonitor(t *testing.T) {
	ctx, c, node, pod := recheckFixture(t)
	event := issuedRecheckEvent(t, ctx, c, node, pod)
	require.NoError(t, validateRecheck(ctx, c, event))
	require.NoError(t, e2eTestClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
	require.ErrorIs(t, validateRecheck(ctx, c, event), errStaleGPURecheck)
}

func TestGPURecheckDryRunDoesNotRequestHardwareCheck(t *testing.T) {
	r := &Reconciler{config: ReconcilerConfig{DryRun: true, TomlConfig: config.TomlConfig{
		GPUHealthRecheck: config.GPUHealthRecheck{Enabled: true},
	}}}
	require.NoError(t, r.initializeGPURecheck())
	require.Nil(t, r.gpuRecheck)
}
