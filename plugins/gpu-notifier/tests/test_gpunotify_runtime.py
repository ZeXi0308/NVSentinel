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

import argparse
import json
import threading
import unittest
from concurrent.futures import Future
from unittest.mock import patch

from app.gpunotify.__main__ import configured_infra_ids, run
from app.gpunotify.engine import Notifier
from app.gpunotify.runtime import NotificationRuntime, ObservedSender, initial_metrics
from app.gpunotify.slack import SlackFailure
from app.gpunotify.source import QUARANTINE, STATE, observe
from app.gpunotify.store import MemoryStore


def node(uid="good"):
    return {
        "metadata": {
            "uid": uid,
            "name": uid,
            "labels": {STATE: "quarantined"},
            "annotations": {QUARANTINE: json.dumps([{"checkName": "GPUHealth"}])},
        },
        "spec": {"unschedulable": True},
    }


class ControlledExecutor:
    def __init__(self):
        self.calls = []

    def submit(self, function, *args):
        future = Future()
        self.calls.append((function, args, future))
        return future

    def finish(self, index, value=None, error=None):
        future = self.calls[index][2]
        if error:
            future.set_exception(error)
        else:
            future.set_result(value)

    def shutdown(self, **kwargs):
        pass


class Source:
    def snapshot(self):
        return [], []


class RecordingNotifier:
    def __init__(self):
        self.ingests, self.deliveries, self.thread_ids = [], [], []
        self.next_error = None

    def ingest(self, observations, now, **kwargs):
        self.ingests.append((observations, now, kwargs))
        self.thread_ids.append(threading.get_ident())

    def deliver_pending(self, now):
        self.deliveries.append(now)
        self.thread_ids.append(threading.get_ident())
        if self.next_error:
            exc, self.next_error = self.next_error, None
            raise exc

    def pending_count(self):
        return 0


class RuntimeTests(unittest.TestCase):
    def make_runtime(self):
        self.executor, self.nodes, self.breaker = ControlledExecutor(), RecordingNotifier(), RecordingNotifier()
        self.metrics = initial_metrics()
        return NotificationRuntime(
            Source(), MemoryStore(), self.nodes, self.breaker, self.metrics, executor=self.executor
        )

    def test_malformed_node_does_not_block_valid_node_or_mark_bad_node_missing(self):
        runtime = self.make_runtime()
        bad = node("bad")
        bad["metadata"]["annotations"][QUARANTINE] = "bad json"
        runtime.step(100)
        self.executor.finish(0, ([bad, node()], []))
        self.executor.finish(1, {"trip": True})
        runtime.step(101)
        observations, _, kwargs = self.nodes.ingests[0]
        self.assertEqual([item["uid"] for item in observations], ["good"])
        self.assertEqual(kwargs, {"complete": True, "seen_uids": {"bad", "good"}})
        self.assertEqual(self.metrics["node_observation_errors_total"], 1)
        self.assertEqual(self.metrics["last_poll_success_timestamp_seconds"], 101)
        self.assertEqual(len(self.breaker.ingests), 1)
        self.assertEqual(self.nodes.deliveries, [100, 101])

    def test_unreadable_node_identity_disables_disappearance_detection(self):
        runtime = self.make_runtime()
        runtime.step(100)
        self.executor.finish(0, ([{}, node()], []))
        runtime.step(101)
        self.assertFalse(self.nodes.ingests[0][2]["complete"])
        self.assertEqual(self.nodes.ingests[0][2]["seen_uids"], {"good"})

    def test_source_outage_keeps_retrying_delivery_without_reusing_old_observations(self):
        runtime = self.make_runtime()
        runtime.step(100)
        self.executor.finish(0, error=TimeoutError())
        self.executor.finish(1, {"trip": True})
        runtime.step(101)
        self.assertEqual(self.nodes.ingests, [])
        self.assertEqual(self.nodes.deliveries, [100, 101])
        self.assertEqual(self.breaker.ingests[0][0], {"trip": True})
        self.assertEqual(self.metrics["poll_errors_total"], 1)
        self.assertEqual(self.metrics["last_poll_success_timestamp_seconds"], 0)

    def test_persisted_failed_message_is_sent_after_restart_during_source_outage(self):
        class Sender:
            def __init__(self, broken=False):
                self.broken, self.posts = broken, []

            def post(self, text, thread_ts=None):
                if self.broken:
                    raise SlackFailure("slack_transport_error")
                self.posts.append((text, thread_ts))
                return "delivered-thread"

        store = MemoryStore()
        first = Notifier(store, Sender(broken=True), None, "xinjiang", "test")
        first.tick([observe(node(), [], 100)], 100)
        self.assertIsNone(store.get("node:good")["thread"])
        sender, executor = Sender(), ControlledExecutor()
        restarted = Notifier(store, sender, None, "xinjiang", "test")
        runtime = NotificationRuntime(
            Source(), store, restarted, RecordingNotifier(), initial_metrics(), executor=executor
        )
        runtime.step(101)
        executor.finish(0, error=TimeoutError())
        runtime.step(131)
        self.assertEqual(len(sender.posts), 1)
        self.assertEqual(store.get("node:good")["thread"], "delivered-thread")
        self.assertFalse(store.get("node:good")["closed"])

    def test_blocked_node_read_leaves_worker_for_breaker_and_main_thread_delivery(self):
        started, release = threading.Event(), threading.Event()

        class SlowSource:
            def snapshot(self):
                started.set()
                if not release.wait(3):
                    raise TimeoutError()
                return [], []

        nodes, breaker = RecordingNotifier(), RecordingNotifier()
        runtime = NotificationRuntime(SlowSource(), MemoryStore(), nodes, breaker, initial_metrics())
        try:
            with patch("app.gpunotify.runtime.fetch_breaker", return_value={"trip": True}):
                runtime.step(100)
                self.assertTrue(started.wait(1))
                # This succeeds while the other future remains blocked.
                runtime.polls["breaker"]["future"].result(timeout=1)
                self.assertFalse(runtime.polls["nodes"]["future"].done())
                runtime.step(101)
            self.assertEqual(breaker.ingests[0][0], {"trip": True})
            self.assertEqual(nodes.deliveries, [100, 101])
            self.assertEqual(set(nodes.thread_ids + breaker.thread_ids), {threading.get_ident()})
        finally:
            release.set()
            runtime.close()

    def test_rate_limit_pauses_both_senders_but_keeps_ingesting(self):
        runtime = self.make_runtime()
        self.breaker.next_error = SlackFailure("ratelimited", 12)
        runtime.step(100)
        self.executor.finish(0, ([node()], []))
        self.executor.finish(1, {"trip": True})
        runtime.step(101)
        runtime.step(105)
        self.executor.finish(2, {"trip": False})
        runtime.step(106)
        self.assertEqual(len(self.breaker.ingests), 2)
        self.assertEqual(len(self.nodes.ingests), 1)
        self.assertEqual(self.nodes.deliveries, [])
        runtime.step(113)
        self.assertEqual(self.nodes.deliveries, [113])

    def test_rate_limit_delay_starts_when_slack_rejects_after_slow_delivery(self):
        runtime = self.make_runtime()
        self.breaker.next_error = SlackFailure("ratelimited", 12)
        with patch("app.gpunotify.runtime.time.monotonic", side_effect=[1000, 1010]):
            runtime.step(100)
        self.assertEqual(runtime.delivery_after, 122)
        runtime.step(115)
        self.assertEqual(self.nodes.deliveries, [])
        runtime.step(123)
        self.assertEqual(self.nodes.deliveries, [123])

    def test_failed_breaker_read_does_not_block_node_ingestion(self):
        runtime = self.make_runtime()
        runtime.step(100)
        self.executor.finish(0, ([node()], []))
        self.executor.finish(1, error=PermissionError())
        runtime.step(101)
        self.assertEqual(len(self.nodes.ingests), 1)
        self.assertEqual(self.breaker.ingests, [])
        self.assertEqual(self.metrics["breaker_poll_errors_total"], 1)

    def test_absent_breaker_config_is_unknown_and_does_not_advance_coverage(self):
        runtime = self.make_runtime()
        runtime.step(100)
        self.executor.finish(1, None)
        runtime.step(101)
        self.assertEqual(self.breaker.ingests, [])
        self.assertEqual(self.metrics["last_breaker_poll_success_timestamp_seconds"], 0)
        self.assertEqual(self.metrics["breaker_poll_errors_total"], 1)


class ConfigurationTests(unittest.TestCase):
    def test_infra_ids_are_required_and_validated_before_network_calls(self):
        for raw in ("", "@infra", "U123ABC bad", "<@U123ABC>"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                configured_infra_ids(raw)
        self.assertEqual(configured_infra_ids("U123ABC W456DEF U123ABC"), ("U123ABC", "W456DEF"))
        with (
            patch.dict("os.environ", {"GPU_NOTIFY_INFRA_IDS": ""}),
            patch("app.gpunotify.__main__.sender_for") as sender,
        ):
            with self.assertRaises(ValueError):
                run(argparse.Namespace(send=True, channel="test"))
            sender.assert_not_called()

    def test_delivery_failure_counter_includes_errors_handled_by_notifiers(self):
        class BrokenSender:
            def post(self, *args, **kwargs):
                raise SlackFailure("not_in_channel")

        metrics = initial_metrics()
        with self.assertRaises(SlackFailure):
            ObservedSender(BrokenSender(), metrics).post("test")
        self.assertEqual(metrics["delivery_errors_total"], 1)


if __name__ == "__main__":
    unittest.main()
