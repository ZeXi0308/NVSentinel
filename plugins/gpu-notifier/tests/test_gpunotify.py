# Copyright (c) 2026, Meshy. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

import httpx

from app.gpunotify.engine import Notifier
from app.gpunotify.slack import GpuSlackSender, SlackFailure
from app.gpunotify.source import KubernetesSource, QUARANTINE, SESSION, STATE, VALIDATION, VALIDATION_EVENTS, observe
from app.gpunotify.store import MemoryStore


def node(uid="node-1", phase="quarantined", active=True):
    return {
        "metadata": {
            "name": uid,
            "uid": uid,
            "labels": {STATE: phase},
            "annotations": {
                **({QUARANTINE: json.dumps([{"checkName": "GPUHealth", "errorCode": ["48"]}])} if active else {})
            },
        },
        "spec": {"unschedulable": active},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }


def pod(node_name="node-1"):
    return {
        "metadata": {
            "name": "worker",
            "namespace": "skypilot",
            "uid": "pod-1",
            "labels": {"skypilot-cluster-name": "training-1"},
            "annotations": {"skypilot-user": "owner@example.com"},
        },
        "spec": {"nodeName": node_name},
        "status": {"phase": "Running"},
    }


class Sender:
    def __init__(self):
        self.posts, self.updates = [], []
        self.fail_post = False
        self.fail_update = False

    def post(self, text, thread_ts=None):
        if self.fail_post:
            raise SlackFailure("temporary_failure")
        self.posts.append((text, thread_ts))
        return str(len(self.posts))

    def update(self, ts, text):
        if self.fail_update:
            raise SlackFailure("update_failure")
        self.updates.append((ts, text))


class Resolver:
    uid = "U123ABC"

    def resolve(self, email):
        return self.uid


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.store, self.sender, self.resolver = MemoryStore(), Sender(), Resolver()
        self.engine = Notifier(self.store, self.sender, self.resolver, "test", "C123")

    def tick(self, phase="quarantined", active=True, pods=None, now=1000):
        self.engine.tick([observe(node(phase=phase, active=active), pods or [], now)], now)

    def parents(self):
        return [p for p in self.sender.posts if p[1] is None]

    def mentions(self):
        return [p for p in self.sender.posts if "<@U123ABC>" in p[0]]

    def test_repeated_events_and_process_restart_reuse_thread(self):
        self.tick()
        self.engine = Notifier(self.store, self.sender, self.resolver, "test", "C123")
        self.tick(phase="draining", now=1030)
        self.tick(phase="draining", now=1060)
        self.assertEqual(len(self.parents()), 1)
        self.assertEqual({ts for _, ts in self.sender.posts if ts}, {"1"})
        self.assertIn("Draining workloads", self.sender.updates[-1][1])
        self.assertEqual([e["phase"] for e in self.store.get("node:node-1")["history"]], ["quarantined", "draining"])

    def test_new_isolation_and_new_node_uid_get_new_incidents(self):
        self.tick()
        self.tick(active=False)
        self.tick()
        self.engine.tick([observe(node("node-2"), [], 1000)], 1000)
        self.assertEqual(len(self.parents()), 3)
        self.assertEqual(len(self.store.items("archive:")), 1)

    def test_parent_failure_is_not_recorded_as_sent(self):
        self.sender.fail_post = True
        self.tick()
        self.assertIsNone(self.store.get("node:node-1")["thread"])
        self.sender.fail_post = False
        self.tick(now=1031)
        self.assertEqual(len(self.parents()), 1)

    def test_failed_update_retries_without_repeating_thread_reply(self):
        self.tick()
        self.sender.fail_update = True
        self.tick(phase="draining")
        sent = len(self.sender.posts)
        self.sender.fail_update = False
        self.tick(phase="draining", now=1031)
        self.assertEqual(len(self.sender.posts), sent)
        self.assertEqual(len(self.sender.updates), 1)

    def test_closed_unsent_cycle_is_delivered_after_next_cycle_begins(self):
        self.sender.fail_post = True
        self.tick()
        self.tick(active=False, now=1030)
        self.sender.fail_post = False
        self.tick(now=1200)
        self.assertEqual(len(self.parents()), 2)
        self.assertFalse(self.store.items("archive:")[0][1]["pending"])

    def test_owner_once_then_daily_and_stop_after_task_ends(self):
        self.tick(pods=[pod()])
        self.tick(pods=[pod()], now=1030)
        self.assertEqual(len(self.mentions()), 1)
        self.tick(pods=[pod()], now=87401)
        self.assertEqual(len(self.mentions()), 2)
        self.tick(now=174000)
        self.assertEqual(len(self.mentions()), 2)

    def test_multi_node_workload_mentions_owner_once(self):
        nodes = [node(), node("node-2")]
        pods = [pod(), pod("node-2")]
        self.engine.tick([observe(n, pods, 1000) for n in nodes], 1000)
        self.assertEqual(len(self.parents()), 2)
        self.assertEqual(len(self.mentions()), 1)
        self.assertIn("node-1, node-2", self.mentions()[0][0])

    def test_recovering_one_node_does_not_repeat_owner_ping_and_retains_thread(self):
        nodes = [node(), node("node-2")]
        pods = [pod(), pod("node-2")]
        self.engine.tick([observe(n, pods, 1000) for n in nodes], 1000)
        thread = self.mentions()[0][1]
        nodes[0] = node(active=False)
        self.engine.tick([observe(n, pods, 1030) for n in nodes], 1030)
        self.assertEqual(len(self.mentions()), 1)
        self.engine.tick([observe(n, pods, 87401) for n in nodes], 87401)
        self.assertEqual(len(self.mentions()), 2)
        self.assertEqual(self.mentions()[-1][1], thread)
        self.assertIn("Affected nodes: node-2", self.mentions()[-1][0])

    def test_new_affected_node_is_not_hidden_by_daily_throttle(self):
        self.tick(pods=[pod()])
        pods = [pod(), pod("node-2")]
        self.engine.tick([observe(n, pods, 1030) for n in (node(), node("node-2"))], 1030)
        self.assertEqual(len(self.mentions()), 2)
        self.assertIn("node-1, node-2", self.mentions()[-1][0])

    def test_pending_delivery_retries_without_new_source_data(self):
        self.sender.fail_post = True
        self.tick(pods=[pod()])
        self.sender.fail_post = False
        self.engine.deliver_pending(1031)
        self.assertEqual(len(self.parents()), 1)
        self.assertEqual(len(self.mentions()), 1)
        # Outage alone does not create another daily owner reminder.
        self.engine.deliver_pending(87401)
        self.assertEqual(len(self.mentions()), 1)

    def test_owner_retry_survives_restart_without_source_and_cancelled_when_task_ends(self):
        self.tick()  # Parent exists before the owner delivery fails.
        self.sender.fail_post = True
        self.tick(pods=[pod()], now=1030)
        self.assertTrue(self.store.items("owner:")[0][1]["pending"])
        self.sender.fail_post = False
        self.engine = Notifier(self.store, self.sender, self.resolver, "test", "C123")
        self.engine.deliver_pending(1061)
        self.assertEqual(len(self.mentions()), 1)
        self.sender.fail_post = True
        self.tick(pods=[pod()], now=90000)
        self.tick(now=90030)  # Fresh evidence says the task ended.
        self.sender.fail_post = False
        self.engine.deliver_pending(90100)
        self.assertEqual(len(self.mentions()), 1)

    def test_24_hour_escalation_includes_waiting_workloads_once(self):
        self.engine.infra_ids = ("UINFRA",)
        self.tick(pods=[pod()])
        self.tick(pods=[pod()], now=87401)
        self.tick(pods=[pod()], now=87431)
        infra = [text for text, _ in self.sender.posts if "<@UINFRA>" in text]
        self.assertEqual(len(infra), 1)
        self.assertIn("coordinate workload completion", infra[0])

    def test_missing_infra_configuration_keeps_escalation_pending(self):
        self.tick(phase="remediation-failed")
        doc = self.store.get("node:node-1")
        self.assertFalse(doc["infra_sent"])
        self.assertTrue(doc["infra_pending"])
        self.engine.infra_ids = ("UINFRA",)
        self.engine.deliver_pending(1030)
        self.assertTrue(self.store.get("node:node-1")["infra_sent"])
        self.assertTrue(any("<@UINFRA>" in text for text, _ in self.sender.posts))

    def test_unresolved_owner_is_retried_and_not_marked_notified(self):
        self.resolver.uid = None
        self.tick(pods=[pod()])
        state = self.store.items("owner:")[0][1]
        self.assertNotIn("sent_at", state)
        self.resolver.uid = "U123ABC"
        self.tick(pods=[pod()], now=1301)
        self.assertEqual(len(self.mentions()), 1)

    def test_missing_node_requires_complete_snapshot(self):
        self.tick()
        self.engine.tick([], 1030, complete=False)
        self.assertFalse(self.store.get("node:node-1")["closed"])
        self.engine.tick([], 1060)
        self.assertEqual(self.store.get("node:node-1")["phase"], "missing")

    def test_manual_cordon_and_dry_run_do_not_start_incident(self):
        n = node(active=False)
        n["spec"]["unschedulable"] = True
        self.engine.tick([observe(n, [], 1000)], 1000)
        n = node()
        n["metadata"]["annotations"]["quarantineHealthEventDryRun"] = "True"
        self.engine.tick([observe(n, [], 1000)], 1000)
        self.assertEqual(len(self.parents()), 0)

    def test_standalone_validation_does_not_start_fault_but_continues_known_fault(self):
        n = node(active=False)
        n["spec"]["unschedulable"] = True
        n["metadata"]["labels"][VALIDATION] = "validating"
        n["metadata"]["annotations"][SESSION] = '[{"name":"manual-check"}]'
        self.engine.tick([observe(n, [], 1000)], 1000)
        self.assertFalse(self.parents())
        self.tick(now=1030)
        self.engine.tick([observe(n, [], 1060)], 1060)
        self.assertEqual(len(self.parents()), 1)
        self.assertEqual(self.store.get("node:node-1")["phase"], "validating")
        self.assertFalse(self.store.get("node:node-1")["closed"])

    def test_validation_event_evidence_can_recover_fault_at_startup(self):
        n = node()
        n["metadata"]["annotations"][VALIDATION_EVENTS] = n["metadata"]["annotations"].pop(QUARANTINE)
        n["metadata"]["labels"][VALIDATION] = "validation-pending"
        self.engine.tick([observe(n, [], 1000)], 1000)
        self.assertEqual(len(self.parents()), 1)
        self.assertIn("GPUHealth", self.parents()[0][0])

    def test_not_ready_does_not_close_incident(self):
        self.tick()
        n = node(active=False)
        n["status"]["conditions"][0]["status"] = "False"
        self.engine.tick([observe(n, [], 1000)], 1000)
        self.assertFalse(self.store.get("node:node-1")["closed"])

    def test_old_not_ready_pod_is_excluded_from_owner_action(self):
        p = pod()
        p["status"]["conditions"] = [{"type": "Ready", "status": "False", "lastTransitionTime": "1970-01-01T00:00:01Z"}]
        self.tick(pods=[p])
        self.assertFalse(self.mentions())

    def test_terminal_pods_and_login_pods_are_excluded(self):
        p = pod()
        p["status"]["phase"] = "Succeeded"
        login = pod()
        login["metadata"]["namespace"] = "skypilot-login"
        self.tick(pods=[p, login])
        self.assertFalse(self.mentions())

    def test_no_owner_ping_after_drain(self):
        self.tick(phase="drain-succeeded", pods=[pod()])
        self.assertFalse(self.mentions())

    def test_contact_support_notifies_infra_not_training_owner(self):
        n = node()
        n["metadata"]["annotations"][QUARANTINE] = json.dumps([{"recommendedAction": 5}])
        self.engine.infra_ids = ("UINFRA",)
        self.engine.tick([observe(n, [pod()], 1000)], 1000)
        self.assertTrue(any("<@UINFRA>" in text for text, _ in self.sender.posts))
        self.assertFalse(self.mentions())

    def test_contact_support_does_not_hide_drain_or_owner_action(self):
        n = node(phase="draining")
        n["metadata"]["annotations"][QUARANTINE] = json.dumps(
            [
                {"recommendedAction": 5, "checkName": "InfoROM"},
                {"recommendedAction": 24, "checkName": "Xid", "errorCode": ["48"]},
            ]
        )
        self.engine.infra_ids = ("UINFRA",)
        self.engine.tick([observe(n, [pod()], 1000)], 1000)
        self.assertEqual(self.store.get("node:node-1")["phase"], "draining")
        self.assertTrue(self.mentions())
        self.assertTrue(any("<@UINFRA>" in text for text, _ in self.sender.posts))

    def test_malformed_annotation_is_not_treated_as_recovery(self):
        n = node()
        n["metadata"]["annotations"][QUARANTINE] = "broken"
        with self.assertRaises(ValueError):
            observe(n, [], 1000)

    def test_reboot_request_is_not_reported_as_completed_reboot(self):
        self.tick(phase="remediation-succeeded")
        self.assertIn("Maintenance request submitted", self.parents()[0][0])

    def test_untrusted_fault_text_cannot_ping_channel(self):
        n = node()
        n["metadata"]["annotations"][QUARANTINE] = json.dumps([{"checkName": "<!channel>"}])
        self.engine.tick([observe(n, [], 1000)], 1000)
        self.assertNotIn("<!channel>", self.parents()[0][0])


class SlackTransportTests(unittest.TestCase):
    def test_rate_limit_preserves_retry_after(self):
        sender = GpuSlackSender("C123", "fake-token")
        with patch("app.gpunotify.slack.httpx.post", return_value=httpx.Response(429, headers={"Retry-After": "120"})):
            with self.assertRaises(SlackFailure) as caught:
                sender.post("test")
        self.assertEqual(caught.exception.retry_after, 120)

    def test_reply_and_update_target_same_channel_and_parent(self):
        sender = GpuSlackSender("C123", "fake-token")
        with (
            patch(
                "app.gpunotify.slack.httpx.post", return_value=httpx.Response(200, json={"ok": True, "ts": "123.456"})
            ) as request,
            patch("app.gpunotify.slack.time.sleep"),
        ):
            sender.post("progress", "123.456")
            self.assertEqual(request.call_args.kwargs["json"]["thread_ts"], "123.456")
            sender.update("123.456", "done")
            self.assertTrue(request.call_args.args[0].endswith("chat.update"))
            self.assertEqual(request.call_args.kwargs["json"]["channel"], "C123")

    def test_slack_ok_false_is_a_failure(self):
        sender = GpuSlackSender("C123", "fake-token")
        with patch(
            "app.gpunotify.slack.httpx.post",
            return_value=httpx.Response(200, json={"ok": False, "error": "not_in_channel"}),
        ):
            with self.assertRaises(SlackFailure):
                sender.post("test")


class KubernetesSourceTests(unittest.TestCase):
    def test_list_pagination_and_rotated_service_account_token(self):
        with tempfile.TemporaryDirectory() as directory:
            token = Path(directory) / "token"
            token.write_text("token-one")
            source = KubernetesSource.__new__(KubernetesSource)
            source.directory = Path(directory)
            requests = []

            def handle(request):
                requests.append(request)
                if len(requests) == 1:
                    token.write_text("token-two")
                    return httpx.Response(200, json={"items": [{"id": 1}], "metadata": {"continue": "next"}})
                return httpx.Response(200, json={"items": [{"id": 2}], "metadata": {}})

            source.client = httpx.Client(base_url="https://kubernetes.test", transport=httpx.MockTransport(handle))
            self.assertEqual(source.items("/api/v1/nodes"), [{"id": 1}, {"id": 2}])
            self.assertEqual(requests[1].url.params["continue"], "next")
            self.assertEqual(requests[1].headers["Authorization"], "Bearer token-two")
            source.client.close()

    def test_list_failure_does_not_return_partial_nodes(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "token").write_text("fake")
            source = KubernetesSource.__new__(KubernetesSource)
            source.directory = Path(directory)
            source.client = httpx.Client(
                base_url="https://kubernetes.test", transport=httpx.MockTransport(lambda request: httpx.Response(403))
            )
            with self.assertRaises(httpx.HTTPStatusError):
                source.items("/api/v1/nodes")
            source.client.close()


if __name__ == "__main__":
    unittest.main()
