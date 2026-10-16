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

import unittest

from app.gpunotify.replay import NODE_KEY, replay_scope, replay_stage
from app.gpunotify.slack import SlackFailure
from app.gpunotify.store import MemoryStore


class Sender:
    def __init__(self):
        self.posts = []
        self.updates = []
        self.fail = False

    def post(self, text, thread_ts=None):
        if self.fail:
            raise SlackFailure("test_unavailable", retry_after=30)
        self.posts.append((text, thread_ts))
        return f"test.{len(self.posts)}"

    def update(self, ts, text):
        self.updates.append((ts, text))


class ReplayTests(unittest.TestCase):
    def stage(self, store, sender, stage, now=1000):
        return replay_stage(store, sender, session="restart-test", channel="C123", stage=stage, clock=lambda: now)

    def test_start_and_resume_keep_one_parent_and_incident(self):
        store, first, second = MemoryStore(), Sender(), Sender()
        started = self.stage(store, first, "start")
        self.assertEqual(started["phase"], "draining")
        self.assertFalse(started["completed"])
        resumed = self.stage(store, second, "resume", 1001)
        self.assertEqual(resumed["thread_ts"], started["thread_ts"])
        self.assertEqual(resumed["incident"], started["incident"])
        self.assertTrue(resumed["completed"])
        self.assertEqual(resumed["phase"], "released")
        self.assertEqual(sum(thread is None for _, thread in first.posts), 1)
        self.assertTrue(all(thread == started["thread_ts"] for _, thread in second.posts))
        self.assertEqual(len(second.posts), 3)
        self.assertTrue(all(text.startswith("[SIMULATION]") for text, _ in first.posts + second.posts))
        self.assertFalse(store.items("owner:"))

    def test_repeated_start_and_resume_do_not_rewind_or_duplicate(self):
        store, sender = MemoryStore(), Sender()
        self.stage(store, sender, "start")
        before = (len(sender.posts), len(sender.updates))
        self.stage(store, sender, "start")
        self.assertEqual((len(sender.posts), len(sender.updates)), before)
        self.stage(store, sender, "resume")
        before = (len(sender.posts), len(sender.updates))
        self.stage(store, sender, "resume")
        self.stage(store, sender, "start")
        self.assertEqual((len(sender.posts), len(sender.updates)), before)
        self.assertEqual(store.get(NODE_KEY)["phase"], "released")

    def test_failed_send_keeps_phase_until_retry_is_delivered(self):
        store, sender = MemoryStore(), Sender()
        sender.fail = True
        with self.assertRaises(RuntimeError):
            self.stage(store, sender, "start")
        self.assertEqual(store.get("replay")["next_phase"], 0)
        self.assertTrue(store.get(NODE_KEY)["pending"])
        with self.assertRaises(ValueError):
            self.stage(store, sender, "resume")
        sender.fail = False
        self.stage(store, sender, "start", 1031)
        self.assertEqual(store.get("replay")["next_phase"], 2)
        self.assertEqual(sum(thread is None for _, thread in sender.posts), 1)

    def test_resume_without_start_does_not_create_an_incident(self):
        store, sender = MemoryStore(), Sender()
        with self.assertRaises(ValueError):
            self.stage(store, sender, "resume")
        self.assertEqual(store.data, {})
        self.assertEqual(sender.posts, [])

    def test_preview_and_slack_scopes_are_distinct_from_each_other_and_live(self):
        self.assertEqual(replay_scope("probe-1", "C123", False), "simulation:probe-1:preview:C123")
        self.assertEqual(replay_scope("probe-1", "C123", True), "simulation:probe-1:slack:C123")
        for invalid in ("", "a:b", "../test", "hello world", "x" * 65):
            with self.subTest(session=invalid), self.assertRaises(ValueError):
                replay_scope(invalid, "C123", False)


if __name__ == "__main__":
    unittest.main()
