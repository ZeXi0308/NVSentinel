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

"""Poll independent sources and flush durable delivery work on the main thread."""

from __future__ import annotations

from typing import Any, Callable

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from app.gpunotify.breaker import fetch_breaker
from app.gpunotify.slack import SlackFailure
from app.gpunotify.source import observe

log = logging.getLogger(__name__)


def initial_metrics() -> dict:
    return {
        "last_poll_success_timestamp_seconds": 0,
        "poll_errors_total": 0,
        "node_observation_errors_total": 0,
        "last_breaker_poll_success_timestamp_seconds": 0,
        "breaker_poll_errors_total": 0,
        "delivery_errors_total": 0,
        "pending_deliveries": 0,
        "unresolved_owners": 0,
        "infra_configuration_missing": 0,
    }


class ObservedSender:
    """Count failed Slack calls even when a notifier handles its own retry."""

    def __init__(self, sender: Any, metrics: dict[str, float]) -> None:
        self.sender, self.metrics = sender, metrics

    def _call(self, method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return method(*args, **kwargs)
        except SlackFailure:
            self.metrics["delivery_errors_total"] += 1
            raise

    def post(self, text: str, thread_ts: str | None = None) -> str:
        return self._call(self.sender.post, text, thread_ts=thread_ts)

    def update(self, ts: str, text: str) -> None:
        return self._call(self.sender.update, ts, text)


class NotificationRuntime:
    """Workers only read Kubernetes; persistence and Slack stay serialized here."""

    def __init__(
        self,
        source: Any,
        store: Any,
        notifier: Any,
        breaker: Any,
        metrics: dict[str, float],
        *,
        namespace: str = "nvsentinel",
        not_ready_seconds: int = 300,
        executor: Any = None,
    ) -> None:
        self.source, self.store = source, store
        self.notifier, self.breaker = notifier, breaker
        self.metrics = metrics
        self.namespace, self.not_ready_seconds = namespace, not_ready_seconds
        # At most one pending read per source: a slow node inventory occupies
        # only one worker, leaving the other available for the breaker.
        self.executor = executor or ThreadPoolExecutor(max_workers=2, thread_name_prefix="gpu-notify-read")
        self.polls = {
            "nodes": {"future": None, "due": 0, "interval": 30},
            "breaker": {"future": None, "due": 0, "interval": 5},
        }
        self.delivery_after = 0

    def _ingest_nodes(self, snapshot: Any, now: float) -> None:
        nodes, pods = snapshot
        seen, observations = set(), []
        complete = True
        for node in nodes:
            try:
                uid = node["metadata"]["uid"]
                if not isinstance(uid, str) or not uid:
                    raise ValueError("invalid Node UID")
                seen.add(uid)
            except (KeyError, TypeError, ValueError):
                # If even the identity is unreadable, skip disappearance
                # detection for this inventory, while processing valid nodes.
                complete = False
                self.metrics["node_observation_errors_total"] += 1
                log.error("node_observation_failed error_type=InvalidNodeIdentity")
                continue
            try:
                observations.append(observe(node, pods, now, self.not_ready_seconds))
            except Exception as exc:
                self.metrics["node_observation_errors_total"] += 1
                log.error("node_observation_failed error_type=%s", type(exc).__name__)
        self.notifier.ingest(observations, now, complete=complete, seen_uids=seen)

    def _consume(self, name: str, future: Any, now: float) -> None:
        try:
            snapshot = future.result()
            if name == "breaker" and snapshot is None:
                self.metrics["breaker_poll_errors_total"] += 1
                log.warning("breaker_configmap_missing")
                return
            self.store.guard()
            if name == "nodes":
                self._ingest_nodes(snapshot, now)
            else:
                self.breaker.ingest(snapshot, now)
            metric = (
                "last_poll_success_timestamp_seconds"
                if name == "nodes"
                else "last_breaker_poll_success_timestamp_seconds"
            )
            self.metrics[metric] = now
        except Exception as exc:
            metric = "poll_errors_total" if name == "nodes" else "breaker_poll_errors_total"
            self.metrics[metric] += 1
            # A failed or incomplete fetch leaves prior observations intact.
            # Its delivery outbox is still flushed below.
            log.error("source_poll_failed source=%s error_type=%s", name, type(exc).__name__)

    def _deliver(self, now: float) -> None:
        if now < self.delivery_after:
            return
        started = time.monotonic()
        # Give a newly observed trip priority over routine node updates.
        for notifier in (self.breaker, self.notifier):
            try:
                self.store.guard()
                notifier.deliver_pending(now)
            except SlackFailure as exc:
                if str(exc) == "ratelimited":
                    # Both notifiers share one channel. Keep collecting source
                    # changes while all Slack sends honor Retry-After.
                    self.delivery_after = now + (time.monotonic() - started) + max(1, exc.retry_after)
                    return
                log.error("delivery_failed error_type=%s", type(exc).__name__)
            except Exception as exc:
                self.metrics["delivery_errors_total"] += 1
                log.error("delivery_failed error_type=%s", type(exc).__name__)

    def _update_delivery_metrics(self) -> None:
        try:
            docs = self.store.items("node:") + self.store.items("archive:") + self.store.items("owner:")
            self.metrics["pending_deliveries"] = (
                sum(bool(d.get("pending") or d.get("infra_pending") or d.get("last_error")) for _, d in docs)
                + self.breaker.pending_count()
            )
            self.metrics["unresolved_owners"] = sum(d.get("last_error") == "owner_unresolved" for _, d in docs)
        except Exception as exc:
            log.error("delivery_metrics_failed error_type=%s", type(exc).__name__)

    def step(self, now: float) -> None:
        for name, poll in self.polls.items():
            future = poll["future"]
            if future is not None and future.done():
                poll["future"] = None
                self._consume(name, future, now)
            if poll["future"] is None and now >= poll["due"]:
                poll["future"] = (
                    self.executor.submit(self.source.snapshot)
                    if name == "nodes"
                    else self.executor.submit(fetch_breaker, self.source, self.namespace)
                )
                poll["due"] = now + poll["interval"]
        self._deliver(now)
        self._update_delivery_metrics()

    def close(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)
