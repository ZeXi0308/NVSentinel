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

"""python -m app.gpunotify demo | replay --stage start|resume --session ID | run --send"""

from __future__ import annotations

from typing import Any

import argparse
import hashlib
import json
import logging
import os
import re
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx

from app.gpunotify.breaker import BreakerNotifier
from app.gpunotify.engine import Notifier
from app.gpunotify.replay import NoOwners, replay_scope, replay_stage
from app.gpunotify.runtime import NotificationRuntime, ObservedSender, initial_metrics
from app.gpunotify.slack import GpuSlackSender, OwnerResolver
from app.gpunotify.source import KubernetesSource
from app.gpunotify.store import MemoryStore, MongoStore

log = logging.getLogger(__name__)
DEFAULT_CHANNEL = "C0C995HP9HT"


class CertificateWatcher:
    """Exit on mounted PEM rotation so the next process reloads PyMongo TLS."""

    def __init__(self, path: str | None) -> None:
        self.path = Path(path) if path else None
        self.next_check = 0
        self.fingerprint = self.read_fingerprint()

    def read_fingerprint(self) -> bytes | None:
        if self.path is None:
            return None
        try:
            return hashlib.sha256(self.path.read_bytes()).digest()
        except OSError as exc:
            log.warning("mongo_client_certificate_read_failed error_type=%s", type(exc).__name__)
            return None

    def changed(self, now: float) -> bool:
        if self.path is None or now < self.next_check:
            return False
        self.next_check = now + 30
        fingerprint = self.read_fingerprint()
        if fingerprint is None:
            return False
        if self.fingerprint is None:
            self.fingerprint = fingerprint
            return False
        return fingerprint != self.fingerprint


class PreviewSender:
    def __init__(self) -> None:
        self.count = 0

    def post(self, text: str, thread_ts: str | None = None) -> str:
        self.count += 1
        print(
            json.dumps(
                {"operation": "reply" if thread_ts else "post", "text": text, "thread_ts": thread_ts},
                ensure_ascii=False,
            )
        )
        return f"preview.{self.count}"

    def update(self, ts: str, text: str) -> None:
        print(json.dumps({"operation": "update", "ts": ts, "text": text}, ensure_ascii=False))


def sender_for(args: argparse.Namespace) -> PreviewSender | GpuSlackSender:
    if not args.send:
        return PreviewSender()
    token = os.environ["GPU_SLACK_BOT_TOKEN"]
    # This new app may be installed at enterprise scope; do not hardcode the old Cop bot's team ID.
    result = httpx.post(
        "https://slack.com/api/auth.test", headers={"Authorization": "Bearer " + token}, timeout=10, trust_env=False
    ).json()
    if not result.get("ok") or not result.get("bot_id"):
        raise RuntimeError("Slack bot authentication failed")
    expected_team = os.environ.get("GPU_SLACK_EXPECTED_TEAM_ID")
    if expected_team and result.get("team_id") != expected_team:
        raise RuntimeError("Slack workspace mismatch")
    return GpuSlackSender(args.channel, token)


def demo(args: argparse.Namespace) -> None:
    sender = sender_for(args)
    # Demo never resolves or mentions real owners and never accesses Kubernetes/Mongo.
    resolver = NoOwners()
    notifier = Notifier(MemoryStore(), sender, resolver, "demo", args.channel, simulated=True)
    sample = {
        "uid": "demo-node-uid",
        "node": "demo-gpu-node",
        "active": True,
        "phase": "quarantined",
        "reason": "Synthetic GPU fault",
        "tasks": {},
    }
    for phase in ("quarantined", "draining", "remediation-succeeded", "validating", "released"):
        sample.update(phase=phase, active=phase != "released")
        notifier.tick([sample], time.time())


def replay(args: argparse.Namespace) -> None:
    if not args.stage or not args.session:
        raise ValueError("replay requires --stage start|resume and --session")
    scope = replay_scope(args.session, args.channel, args.send)
    store = MongoStore(os.environ["GPU_NOTIFY_MONGO_URI"], os.environ["GPU_NOTIFY_MONGO_DATABASE"], scope)
    try:
        result = replay_stage(store, sender_for(args), session=args.session, channel=args.channel, stage=args.stage)
        print(json.dumps(result, ensure_ascii=False))
    finally:
        store.close()


def configured_infra_ids(raw: str) -> tuple[str, ...]:
    ids = tuple(dict.fromkeys(raw.split()))
    if not ids or any(not re.fullmatch(r"[UW][A-Z0-9]+", value) for value in ids):
        raise ValueError("GPU_NOTIFY_INFRA_IDS must contain Slack user IDs for infra notification")
    return ids


def run(args: argparse.Namespace) -> None:
    if not args.send:
        raise ValueError("live mode requires --send; use demo for an offline preview")
    # Validate before authentication or starting either polling loop.
    infra_ids = configured_infra_ids(os.environ.get("GPU_NOTIFY_INFRA_IDS", ""))
    metrics = initial_metrics()
    sender = ObservedSender(sender_for(args), metrics)
    cluster = os.environ["GPU_NOTIFY_CLUSTER"]
    certificate = CertificateWatcher(os.environ.get("GPU_NOTIFY_MONGO_CERT_FILE"))
    store = MongoStore(
        os.environ["GPU_NOTIFY_MONGO_URI"], os.environ["GPU_NOTIFY_MONGO_DATABASE"], cluster + ":" + args.channel
    )
    resolver = OwnerResolver(os.environ["GPU_SLACK_BOT_TOKEN"])
    notifier = Notifier(store, sender, resolver, cluster, args.channel, infra_ids)
    breaker = BreakerNotifier(store, sender, cluster, infra_ids)
    source = KubernetesSource()
    runtime = NotificationRuntime(
        source,
        store,
        notifier,
        breaker,
        metrics,
        namespace=os.environ.get("GPU_NOTIFY_BREAKER_NAMESPACE", "nvsentinel"),
        not_ready_seconds=int(os.environ.get("GPU_NOTIFY_NOT_READY_SECONDS", "300")),
    )
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopped.set())

    class Health(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path not in ("/healthz", "/metrics"):
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            body = (
                "ok\n"
                if self.path == "/healthz"
                else "".join(f"gpu_notify_{key} {value}\n" for key, value in metrics.items())
            )
            self.wfile.write(body.encode())

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("GPU_NOTIFY_PORT", "8080"))), Health)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        while not stopped.is_set():
            if certificate.changed(time.monotonic()):
                log.info("mongo_client_certificate_rotated restarting_worker")
                break
            try:
                runtime.step(time.time())
            except Exception as exc:
                metrics["poll_errors_total"] += 1
                # Never log connection strings, HTTP headers, or exception bodies containing credentials.
                log.error("runtime_step_failed error_type=%s", type(exc).__name__)
            stopped.wait(1)
    finally:
        server.shutdown()
        runtime.close()
        source.client.close()
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("demo", "replay", "run"))
    parser.add_argument("--send", action="store_true", help="Actually send Slack messages")
    parser.add_argument("--channel", default=DEFAULT_CHANNEL)
    parser.add_argument("--stage", choices=("start", "resume"), help="Mongo replay stage; run in separate processes")
    parser.add_argument("--session", help="Stable Mongo replay session ID; reuse for resume")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        if args.mode != "replay" and (args.stage or args.session):
            raise ValueError("--stage and --session apply only to replay")
        {"demo": demo, "replay": replay, "run": run}[args.mode](args)
    except Exception as exc:
        log.error("startup_or_demo_failed error_type=%s", type(exc).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
