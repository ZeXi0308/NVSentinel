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
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import httpx

from app.gpunotify.slack import GpuSlackSender, OwnerResolver


class OwnerResolverTests(unittest.TestCase):
    def test_success_uses_full_email_bearer_token_and_bounded_timeout(self) -> None:
        resolver = OwnerResolver("fake-token")
        with patch(
            "app.gpunotify.slack.httpx.get",
            return_value=httpx.Response(200, json={"ok": True, "user": {"id": "U123ABC"}}),
        ) as request:
            self.assertEqual(resolver.resolve("owner+training@example.com"), "U123ABC")
        self.assertEqual(request.call_args.kwargs["params"], {"email": "owner+training@example.com"})
        self.assertEqual(request.call_args.kwargs["headers"], {"Authorization": "Bearer fake-token"})
        self.assertEqual(request.call_args.kwargs["timeout"], 3.0)
        self.assertFalse(request.call_args.kwargs["trust_env"])

    def test_same_local_part_at_different_domains_does_not_share_identity(self) -> None:
        resolver = OwnerResolver("fake-token")
        with patch(
            "app.gpunotify.slack.httpx.get",
            side_effect=[
                httpx.Response(200, json={"ok": True, "user": {"id": "U111ABC"}}),
                httpx.Response(200, json={"ok": True, "user": {"id": "U222ABC"}}),
            ],
        ) as request:
            self.assertEqual(resolver.resolve("owner@example.com"), "U111ABC")
            self.assertEqual(resolver.resolve("owner@another.example"), "U222ABC")
        self.assertEqual(
            [call.kwargs["params"]["email"] for call in request.call_args_list],
            ["owner@example.com", "owner@another.example"],
        )

    def test_lookup_misses_and_errors_remain_unresolved(self) -> None:
        responses = [
            httpx.Response(200, json={"ok": False, "error": "users_not_found"}),
            httpx.Response(200, json={"ok": False, "error": "missing_scope"}),
            httpx.Response(429, headers={"Retry-After": "30"}),
            httpx.Response(503),
            httpx.Response(200, content=b"not JSON"),
            httpx.Response(200, json=[]),
            httpx.Response(200, json={"ok": True, "user": None}),
            httpx.Response(200, json={"ok": True, "user": {"id": "<!channel>"}}),
        ]
        for response in responses:
            with self.subTest(response=response), patch("app.gpunotify.slack.httpx.get", return_value=response):
                self.assertIsNone(OwnerResolver("fake-token").resolve("owner@example.com"))
        with patch("app.gpunotify.slack.httpx.get", side_effect=httpx.ReadTimeout("test timeout")):
            self.assertIsNone(OwnerResolver("fake-token").resolve("owner@example.com"))

    def test_sender_builds_its_own_headers(self) -> None:
        with patch(
            "app.gpunotify.slack.httpx.post", return_value=httpx.Response(200, json={"ok": True, "ts": "123.456"})
        ) as request:
            GpuSlackSender("C123", "fake-token").post("test")
        self.assertEqual(request.call_args.kwargs["headers"]["Authorization"], "Bearer fake-token")
        self.assertEqual(request.call_args.kwargs["json"]["channel"], "C123")


class StandalonePackageTests(unittest.TestCase):
    def test_demo_runs_outside_checkout_without_clusterboard_imports(self) -> None:
        plugin = Path(__file__).resolve().parents[1]
        env = dict(os.environ, PYTHONPATH=str(plugin))
        env.pop("GPU_SLACK_BOT_TOKEN", None)
        script = (
            "import sys; import app; from app.gpunotify.__main__ import main; "
            "sys.argv = ['gpu-notifier', 'demo']; main(); "
            "assert not any(name.startswith('app.copnotify') for name in sys.modules); "
            "print('PACKAGE=' + app.__file__)"
        )
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [sys.executable, "-c", script], cwd=directory, env=env, capture_output=True, text=True, timeout=15
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        messages = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
        self.assertEqual(sum(item["operation"] == "post" for item in messages), 1)
        self.assertEqual(sum(item["operation"] == "reply" for item in messages), 4)
        self.assertTrue(all("[SIMULATION]" in item["text"] for item in messages))
        self.assertIn("PACKAGE=" + str(plugin / "app/__init__.py"), result.stdout)


if __name__ == "__main__":
    unittest.main()
