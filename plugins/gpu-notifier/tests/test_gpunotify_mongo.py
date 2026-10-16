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

"""Opt-in integration checks against a disposable MongoDB database.

Set GPU_NOTIFY_TEST_MONGO_URI and optionally GPU_NOTIFY_TEST_MONGO_DATABASE.
Only uniquely scoped documents in gpu_notification_state are inserted/deleted;
no Kubernetes or Slack requests are made.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
import uuid

from app.gpunotify.replay import NODE_KEY, replay_scope, replay_stage
from app.gpunotify.slack import SlackFailure
from app.gpunotify.store import MongoStore


@unittest.skipUnless(os.environ.get("GPU_NOTIFY_TEST_MONGO_URI"), "requires explicit test MongoDB URI")
class MongoReplayTests(unittest.TestCase):
    def setUp(self):
        from pymongo import MongoClient

        self.uri = os.environ["GPU_NOTIFY_TEST_MONGO_URI"]
        self.database = os.environ.get("GPU_NOTIFY_TEST_MONGO_DATABASE", "gpu_notifier_test")
        self.session = "integration-" + uuid.uuid4().hex
        self.channel = "CREPLAYTEST"
        self.scope = replay_scope(self.session, self.channel, False)
        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=10000)
        self.collection = self.client[self.database]["gpu_notification_state"]

    def tearDown(self):
        self.collection.delete_many({"scope": self.scope})
        self.collection.delete_one({"_id": self.scope + ":lease"})
        self.client.close()

    def process(self, stage):
        env = dict(os.environ, GPU_NOTIFY_MONGO_URI=self.uri, GPU_NOTIFY_MONGO_DATABASE=self.database)
        # Preview cannot accidentally use supplied bot credentials.
        env.pop("GPU_SLACK_BOT_TOKEN", None)
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "app.gpunotify",
                "replay",
                "--stage",
                stage,
                "--session",
                self.session,
                "--channel",
                self.channel,
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        # No URI or environment is included in failure output.
        self.assertEqual(result.returncode, 0, result.stderr)
        return [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]

    def test_two_cli_processes_resume_parent_from_real_mongo(self):
        first = self.process("start")
        before = first[-1]
        self.assertEqual(sum(item["operation"] == "post" for item in first), 1)
        self.assertEqual(before["phase"], "draining")
        persisted = self.collection.find_one({"_id": self.scope + ":" + NODE_KEY})["value"]
        self.assertEqual(persisted["thread"], before["thread_ts"])

        second = self.process("resume")
        after = second[-1]
        self.assertEqual(after["incident"], before["incident"])
        self.assertEqual(after["thread_ts"], before["thread_ts"])
        self.assertEqual(after["phase"], "released")
        self.assertTrue(after["completed"])
        self.assertFalse(any(item["operation"] == "post" for item in second))
        replies = [item for item in second if item["operation"] == "reply"]
        self.assertEqual(len(replies), 3)
        self.assertTrue(all(item["thread_ts"] == before["thread_ts"] for item in replies))
        self.assertEqual(len(self.process("resume")), 1)  # Checkpoint only, no duplicate send.

    def test_failed_delivery_is_retried_after_store_reopens(self):
        class FailingSender:
            def post(self, text, thread_ts=None):
                raise SlackFailure("test_unavailable", retry_after=30)

        from app.gpunotify.__main__ import PreviewSender

        first = MongoStore(self.uri, self.database, self.scope)
        try:
            with self.assertRaises(RuntimeError):
                replay_stage(
                    first,
                    FailingSender(),
                    session=self.session,
                    channel=self.channel,
                    stage="start",
                    clock=lambda: 1000,
                )
            old_incident = first.get(NODE_KEY)["incident"]
            self.assertEqual(first.get("replay")["next_phase"], 0)
        finally:
            first.close()

        second = MongoStore(self.uri, self.database, self.scope)
        try:
            result = replay_stage(
                second, PreviewSender(), session=self.session, channel=self.channel, stage="start", clock=lambda: 1031
            )
            self.assertEqual(result["incident"], old_incident)
            self.assertEqual(result["phase"], "draining")
            self.assertEqual(second.get(NODE_KEY)["pending"], [])
        finally:
            second.close()


if __name__ == "__main__":
    unittest.main()
