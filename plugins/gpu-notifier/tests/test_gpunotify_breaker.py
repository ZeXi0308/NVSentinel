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

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import httpx

from app.gpunotify.breaker import BreakerNotifier, STATE_KEY, TRIP_PREFIX, fetch_breaker
from app.gpunotify.slack import SlackFailure
from app.gpunotify.store import MemoryStore


def configmap(status="TRIPPED", uid="configmap-1", revision="1"):
    return {"metadata": {"uid": uid, "resourceVersion": revision}, "data": {"status": status, "cursor": "resume"}}


class Sender:
    def __init__(self):
        self.posts = []
        self.updates = []
        self.post_error = None
        self.update_error = None

    def post(self, text, thread_ts=None):
        if self.post_error:
            raise self.post_error
        self.posts.append((text, thread_ts))
        return str(len(self.posts))

    def update(self, ts, text):
        if self.update_error:
            raise self.update_error
        self.updates.append((ts, text))


class CircuitBreakerTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.sender = Sender()
        self.notifier = BreakerNotifier(self.store, self.sender, "xinjiang", ["U123ABC"])

    def trips(self):
        return [doc for _, doc in self.notifier._records()]

    def test_closed_at_startup_does_not_create_an_alert(self):
        self.notifier.ingest(configmap("CLOSED"), 1)
        self.notifier.deliver_pending(1)
        self.assertEqual(self.sender.posts, [])
        self.assertEqual(self.notifier.pending_count(), 0)

    def test_trip_mentions_infra_once_across_restart_and_metadata_updates(self):
        self.notifier.ingest(configmap(), 1)
        self.notifier.deliver_pending(1)
        restarted = BreakerNotifier(self.store, self.sender, "xinjiang", ["U123ABC"])
        restarted.ingest(configmap(revision="9"), 2)
        restarted.deliver_pending(2)
        self.assertEqual(len(self.sender.posts), 1)
        self.assertIn("<@U123ABC>", self.sender.posts[0][0])
        self.assertIn("recovery events", self.sender.posts[0][0])
        self.assertEqual(restarted.pending_count(), 0)

    def test_reset_updates_original_thread_and_next_trip_gets_new_thread(self):
        self.notifier.ingest(configmap(), 1)
        self.notifier.deliver_pending(1)
        self.notifier.ingest(configmap("CLOSED", revision="2"), 2)
        self.notifier.deliver_pending(2)
        self.notifier.deliver_pending(3)
        self.assertEqual(len(self.sender.posts), 2)
        self.assertEqual(self.sender.posts[1][1], "1")
        self.assertIn("Confirm fault-quarantine event processing resumed", self.sender.posts[1][0])
        self.assertEqual(self.sender.updates[0][0], "1")
        self.notifier.ingest(configmap(revision="3"), 4)
        self.notifier.deliver_pending(4)
        self.assertEqual(self.sender.posts[2][1], None)
        self.assertEqual(len(self.trips()), 2)

    def test_recreated_configmap_has_separate_trip_without_false_reset(self):
        self.notifier.ingest(configmap(), 1)
        self.notifier.deliver_pending(1)
        self.notifier.ingest(configmap(uid="configmap-2"), 2)
        self.notifier.deliver_pending(2)
        self.assertEqual(len(self.trips()), 2)
        self.assertIn("ConfigMap replaced", self.sender.posts[1][0])
        self.assertNotIn("CLOSED", self.sender.posts[1][0])
        self.assertIsNone(self.sender.posts[2][1])

    def test_absent_or_invalid_snapshot_does_not_clear_known_trip(self):
        self.notifier.ingest(configmap(), 1)
        self.notifier.ingest(None, 2)
        with self.assertRaises(ValueError):
            self.notifier.ingest(configmap("UNKNOWN"), 3)
        self.assertEqual(self.store.get(STATE_KEY)["status"], "TRIPPED")
        self.assertFalse(self.trips()[0]["closed"])

    def test_delivery_retries_without_new_source_observation(self):
        self.sender.post_error = SlackFailure("temporary_error")
        self.notifier.ingest(configmap(), 1)
        self.notifier.deliver_pending(1)
        self.assertIsNone(self.trips()[0]["thread"])
        self.assertEqual(self.notifier.pending_count(), 1)
        self.sender.post_error = None
        self.notifier.deliver_pending(30)
        self.assertEqual(len(self.sender.posts), 0)
        self.notifier.deliver_pending(31)
        self.assertEqual(len(self.sender.posts), 1)

    def test_unconfigured_infra_stays_pending_until_real_mention(self):
        empty = BreakerNotifier(self.store, self.sender, "xinjiang")
        empty.ingest(configmap(), 1)
        empty.deliver_pending(1)
        empty.deliver_pending(2)
        self.assertFalse(self.trips()[0]["infra_sent"])
        self.assertEqual(empty.pending_count(), 1)
        self.assertEqual(len(self.sender.posts), 1)
        self.notifier.deliver_pending(3)
        self.assertIn("<@U123ABC>", self.sender.posts[1][0])
        self.assertEqual(self.sender.posts[1][1], "1")
        self.assertTrue(self.trips()[0]["infra_sent"])
        self.assertEqual(self.notifier.pending_count(), 0)

    def test_reset_before_first_delivery_announces_current_state_without_stale_page(self):
        self.notifier.ingest(configmap(), 1)
        self.notifier.ingest(configmap("CLOSED"), 2)
        self.notifier.deliver_pending(2)
        self.assertEqual(len(self.sender.posts), 1)
        self.assertIn("CLOSED after trip", self.sender.posts[0][0])
        self.assertNotIn("<@", self.sender.posts[0][0])
        self.assertEqual(self.notifier.pending_count(), 0)

    def test_failed_parent_update_does_not_repeat_reset_reply(self):
        self.notifier.ingest(configmap(), 1)
        self.notifier.deliver_pending(1)
        self.notifier.ingest(configmap("CLOSED"), 2)
        self.sender.update_error = SlackFailure("temporary_error")
        self.notifier.deliver_pending(2)
        self.assertEqual(len(self.sender.posts), 2)
        self.sender.update_error = None
        self.notifier.deliver_pending(32)
        self.assertEqual(len(self.sender.posts), 2)
        self.assertEqual(len(self.sender.updates), 1)

    def test_rate_limit_propagates_after_saving_retry(self):
        self.notifier.ingest(configmap(), 1)
        self.sender.post_error = SlackFailure("ratelimited", 120)
        with self.assertRaises(SlackFailure):
            self.notifier.deliver_pending(1)
        self.assertEqual(self.trips()[0]["retry_at"], 121)

    def test_cluster_text_cannot_inject_mentions(self):
        notifier = BreakerNotifier(self.store, self.sender, "<!channel>\ncluster")
        notifier.ingest(configmap(), 1)
        notifier.deliver_pending(1)
        text = self.sender.posts[0][0]
        self.assertNotIn("<!channel>", text)
        self.assertNotIn("\n", text)

    def test_transition_write_failure_does_not_duplicate_archived_delivery(self):
        self.notifier.ingest(configmap(), 1)
        self.notifier.deliver_pending(1)
        self.notifier.ingest(configmap("CLOSED"), 2)
        original_put = self.store.put

        def fail_current(key, value):
            if key == STATE_KEY:
                raise RuntimeError("simulated write failure")
            original_put(key, value)

        self.store.put = fail_current
        with self.assertRaises(RuntimeError):
            self.notifier.ingest(configmap(revision="3"), 3)
        self.store.put = original_put
        self.assertEqual(len(self.trips()), 1)
        self.notifier.deliver_pending(3)
        self.assertEqual(len(self.sender.posts), 2)
        self.notifier.ingest(configmap(revision="3"), 4)
        self.notifier.deliver_pending(4)
        self.assertEqual(len(self.trips()), 2)
        self.assertEqual(len(self.sender.posts), 3)

    def test_acknowledged_snapshots_and_deliveries_do_not_write_every_poll(self):
        self.notifier.ingest(configmap(), 1)
        self.notifier.deliver_pending(1)

        def unexpected_put(key, value):
            self.fail("unchanged acknowledged state was written again")

        self.store.put = unexpected_put
        self.notifier.ingest(configmap(), 2)
        self.notifier.deliver_pending(2)

    def test_unconfigured_infra_waits_without_writing_each_flush(self):
        notifier = BreakerNotifier(self.store, self.sender, "xinjiang")
        notifier.ingest(configmap(), 1)
        notifier.deliver_pending(1)

        def unexpected_put(key, value):
            self.fail("unchanged pending infra state was written again")

        self.store.put = unexpected_put
        notifier.deliver_pending(2)
        self.assertEqual(notifier.pending_count(), 1)


class CircuitBreakerSourceTests(unittest.TestCase):
    def test_read_uses_current_token_timeout_and_only_named_configmap(self):
        requests = []

        def handle(request):
            requests.append(request)
            return httpx.Response(200, json=configmap())

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "token").write_text("first-token")
            with httpx.Client(base_url="https://kubernetes.test", transport=httpx.MockTransport(handle)) as client:
                source = SimpleNamespace(client=client, directory=path)
                self.assertEqual(fetch_breaker(source)["data"]["status"], "TRIPPED")
                (path / "token").write_text("rotated-token")
                fetch_breaker(source)
        self.assertEqual(requests[0].url.path, "/api/v1/namespaces/nvsentinel/configmaps/circuit-breaker")
        self.assertEqual(requests[1].headers["Authorization"], "Bearer rotated-token")
        self.assertEqual(requests[1].extensions["timeout"]["read"], 5)

    def test_not_found_is_unknown_and_permission_failure_raises(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "token").write_text("test-token")
            for status in (404, 403):
                with httpx.Client(
                    base_url="https://kubernetes.test",
                    transport=httpx.MockTransport(lambda request: httpx.Response(status)),
                ) as client:
                    source = SimpleNamespace(client=client, directory=path)
                    if status == 404:
                        self.assertIsNone(fetch_breaker(source))
                    else:
                        with self.assertRaises(httpx.HTTPStatusError):
                            fetch_breaker(source)


if __name__ == "__main__":
    unittest.main()
