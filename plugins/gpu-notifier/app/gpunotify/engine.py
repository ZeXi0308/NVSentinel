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

"""Durable incident threads and workload-scoped owner reminders."""

from __future__ import annotations

from typing import Any

import hashlib
import html
import json
import logging
import re
import uuid
from datetime import datetime, timezone

from app.gpunotify.slack import SlackFailure
from app.gpunotify.source import PHASES

log = logging.getLogger(__name__)


def escaped(value: Any) -> str:
    return html.escape(str(value), quote=False).replace("\n", " ")


def identity(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


class Notifier:
    def __init__(
        self,
        store: Any,
        sender: Any,
        resolver: Any,
        cluster: str,
        channel: str,
        infra_ids: tuple[str, ...] = (),
        simulated: bool = False,
    ) -> None:
        self.store, self.sender, self.resolver = store, sender, resolver
        self.cluster, self.channel = cluster, channel
        self.infra_ids = tuple(infra_ids)
        if any(not re.fullmatch(r"[UW][A-Z0-9]+", uid) for uid in self.infra_ids):
            raise ValueError("infra IDs must be Slack user IDs")
        self.prefix = "[SIMULATION] " if simulated else ""

    def summary(self, incident: dict, phase: str | None = None, reason: str | None = None) -> str:
        return (
            f"{self.prefix}GPU incident | {escaped(self.cluster)}/{escaped(incident['node'])} | "
            f"{escaped(PHASES.get(phase or incident['phase'], phase or incident['phase']))} | "
            f"{escaped((incident['reason'] if reason is None else reason) or 'Fault details unavailable')} | "
            f"incident={incident['incident'][:8]}"
        )

    def tick(self, observations: list[dict], now: float, *, complete: bool = True) -> None:
        self.ingest(observations, now, complete=complete)
        self.deliver_pending(now)

    def ingest(
        self, observations: list[dict], now: float, *, complete: bool = True, seen_uids: set[str] | None = None
    ) -> None:
        """Record fresh evidence independently of Slack delivery.

        seen_uids includes nodes whose observation could not be parsed. A failed
        snapshot must not call this method; its existing outbox can still drain.
        """
        seen = set(seen_uids or ())
        fresh = set()
        for observation in observations:
            uid = observation["uid"]
            seen.add(uid)
            fresh.add(uid)
            key = "node:" + uid
            doc = self.store.get(key)
            fault_origin = observation.get("fault_origin", observation["active"])
            if not doc or (doc.get("closed") and fault_origin):
                if not fault_origin:
                    continue  # manual cordon / ordinary healthy nodes do not create incidents
                if doc:
                    self.store.put("archive:" + doc["incident"], doc)
                doc = {
                    "uid": uid,
                    "node": observation["node"],
                    "incident": str(uuid.uuid4()),
                    "started": now,
                    "thread": None,
                    "phase": "",
                    "reason": "",
                    "pending": [],
                    "tasks": {},
                    "closed": False,
                    "retry_at": 0,
                    "failures": 0,
                    "infra_sent": False,
                }
            if doc.get("closed"):
                continue
            self.record(key, doc, observation, now)
        if complete:
            for key, doc in self.store.items("node:"):
                if doc["uid"] not in seen and not doc["closed"]:
                    self.record(key, doc, {"phase": "missing", "reason": "", "tasks": {}}, now)
                    fresh.add(doc["uid"])
        self.prepare_owners(now, fresh)

    def deliver_pending(self, now: float) -> None:
        """Flush durable work even when the next Kubernetes read fails."""
        records = self.store.items("node:") + self.store.items("archive:")
        for key, doc in records:
            if now < doc.get("retry_at", 0):
                continue
            needs_escalation = self.needs_infra(doc, now) and not doc["infra_sent"]
            escalation_work = needs_escalation and (self.infra_ids or not doc.get("infra_pending"))
            if (
                doc["thread"]
                and not doc["pending"]
                and doc.get("parent_text") == self.summary(doc)
                and not escalation_work
                and not doc.get("last_error")
            ):
                continue
            try:
                self.deliver(key, doc, now)
            except SlackFailure as exc:
                self.failed(key, doc, exc, now)
                if str(exc) == "ratelimited":
                    raise  # worker respects channel-wide Retry-After
        self.deliver_owners(now)

    def record(self, key: str, doc: dict, observation: dict, now: float) -> None:
        reason = observation.get("reason") or doc["reason"]
        changed = (doc["phase"], doc["reason"]) != (observation["phase"], reason)
        doc.update(
            phase=observation["phase"],
            reason=reason,
            tasks=observation["tasks"],
            manual_required=observation.get("manual_required", False),
            observed_at=now,
            closed=observation["phase"] in ("released", "missing"),
        )
        if changed:
            event = {"phase": doc["phase"], "reason": reason, "at": now}
            doc["pending"].append(event)
            doc.setdefault("history", []).append(event)
        self.store.put(key, doc)

    def deliver(self, key: str, doc: dict, now: float) -> None:
        if not doc["thread"]:
            self.store.guard()
            doc["thread"] = self.sender.post(self.summary(doc))
            doc["parent_text"] = self.summary(doc)
            if len(doc["pending"]) == 1:
                doc["pending"].clear()  # The main message already announces this first observation.
            # A successful send is checkpointed before any following network operation.
            self.store.put(key, doc)
        while doc["pending"]:
            event = doc["pending"][0]
            self.store.guard()
            self.sender.post(self.summary(doc, event["phase"], event.get("reason")), doc["thread"])
            doc["pending"].pop(0)
            self.store.put(key, doc)
        desired = self.summary(doc)
        if doc.get("parent_text") != desired:
            self.store.guard()
            self.sender.update(doc["thread"], desired)
            doc["parent_text"] = desired
            self.store.put(key, doc)
        doc["infra_pending"] = bool(self.needs_infra(doc, now) and not doc["infra_sent"])
        if doc["infra_pending"] and self.infra_ids:
            mentions = " ".join(f"<@{uid}>" for uid in self.infra_ids)
            action = (
                "Isolation has lasted at least 24h; coordinate workload completion and recovery with the owner."
                if not doc["closed"] and doc["tasks"] and now - doc["started"] >= 86400
                else "Please investigate the blocked GPU recovery."
            )
            self.store.guard()
            self.sender.post(
                f"{self.prefix}{mentions} | {escaped(self.cluster)}/{escaped(doc['node'])} | " f"{action}",
                doc["thread"],
            )
            doc["infra_sent"] = True
            doc["infra_pending"] = False
            self.store.put(key, doc)
        doc.update(failures=0, retry_at=0, last_error=None)
        self.store.put(key, doc)

    @staticmethod
    def needs_infra(doc: dict, now: float) -> bool:
        return (
            doc.get("manual_required")
            or doc["phase"] in ("manual", "missing", "drain-failed", "remediation-failed", "validation-failed")
            or (not doc["closed"] and now - doc["started"] >= 86400)
        )

    def failed(self, key: str, doc: dict, exc: SlackFailure, now: float) -> None:
        count = doc.get("failures", 0) + 1
        doc.update(
            failures=count,
            last_error=str(exc),
            retry_at=now + max(exc.retry_after, min(900, 30 * 2 ** min(count - 1, 5))),
        )
        self.store.put(key, doc)
        log.warning("delivery_failed key=%s error=%s", key, type(exc).__name__)

    def prepare_owners(self, now: float, fresh_uids: set[str]) -> None:
        """Create/update owner intents from fresh observations, never from an outage."""
        grouped = {}
        for _, doc in self.store.items("node:"):
            if doc["closed"]:
                continue
            for task_key, task in doc["tasks"].items():
                entry = grouped.setdefault(task_key, {"task": task, "incidents": []})
                entry["incidents"].append(doc)
        for task_key, entry in grouped.items():
            key = "owner:" + identity(task_key)
            state = self.store.get(key) or {}
            task = entry["task"]
            docs = sorted(entry["incidents"], key=lambda d: (d["started"], d["uid"]))
            signature = sorted(d["incident"] for d in docs)
            # Wait for fresh evidence from every affected node before scheduling
            # new work; already persisted deliveries can proceed independently.
            if any(d["uid"] not in fresh_uids for d in docs):
                continue
            if (
                not state.get("pending")
                and not set(signature) - set(state.get("signature", []))
                and now - state.get("sent_at", 0) < 86400
            ):
                continue
            old = state.get("pending") or {}
            related = set(signature) & set(state.get("signature", []))
            related_pending = set(signature) & set(old.get("signature", []))
            # Keep the original task thread when one of its nodes recovers.
            thread = state.get("thread") if related else None
            thread = thread or (old.get("thread") if related_pending else None)
            parent_uid = old.get("parent_uid") if related_pending else docs[0]["uid"]
            state["pending"] = {
                "task": task,
                "signature": signature,
                "uids": [d["uid"] for d in docs],
                "nodes": [d["node"] for d in docs],
                "parent_uid": parent_uid,
                "parent_incident": old.get("parent_incident") if related_pending else docs[0]["incident"],
                "thread": thread,
                "observed_at": now,
            }
            self.store.put(key, state)
        active_keys = {"owner:" + identity(k) for k in grouped}
        for key, state in self.store.items("owner:"):
            if key in active_keys:
                continue
            pending = state.get("pending")
            if pending and not set(pending["uids"]).issubset(fresh_uids):
                continue  # An unreadable node is not proof that its workload ended.
            if pending or state.get("last_error"):
                state.update(pending=None, last_error=None, retry_at=0)
                self.store.put(key, state)

    def deliver_owners(self, now: float) -> None:
        for key, state in self.store.items("owner:"):
            pending = state.get("pending")
            if not pending or now < state.get("retry_at", 0):
                continue
            thread = pending.get("thread")
            if not thread:
                parent = self.store.get("node:" + pending["parent_uid"])
                if parent and parent["incident"] != pending["parent_incident"]:
                    parent = self.store.get("archive:" + pending["parent_incident"])
                if not parent or not parent.get("thread"):
                    continue
                thread = pending["thread"] = parent["thread"]
            task, signature = pending["task"], pending["signature"]
            email = task["email"]
            uid = self.resolver.resolve(email) if email else None
            if not uid or not re.fullmatch(r"[UW][A-Z0-9]+", uid):
                # Keep retrying lookup; a fallback message does not count as an owner notification.
                if state.get("unresolved_signature") != signature:
                    try:
                        self.store.guard()
                        self.sender.post(
                            f"{self.prefix}Owner unresolved | {escaped(task['workload'])} | "
                            "Infra: identify and invite the owner to this channel.",
                            thread,
                        )
                        state["unresolved_signature"] = signature
                    except SlackFailure as exc:
                        self.failed(key, state, exc, now)
                        if str(exc) == "ratelimited":
                            raise
                        continue
                state.update(retry_at=now + 300, last_error="owner_unresolved")
                self.store.put(key, state)
                continue
            nodes = ", ".join(pending["nodes"])
            observed = datetime.fromtimestamp(pending["observed_at"], timezone.utc).isoformat(timespec="seconds")
            text = (
                f"{self.prefix}<@{uid}> | {escaped(self.cluster)} | {escaped(task['workload'])} | "
                f"Affected nodes: {escaped(nodes)} | Save checkpoints and work, then end the workload "
                "when safe; please agree a completion time in this thread. No automatic deletion is scheduled. "
                f"Observed at {observed}."
            )
            try:
                self.store.guard()
                self.sender.post(text, thread)
                state.update(
                    signature=signature,
                    sent_at=now,
                    retry_at=0,
                    failures=0,
                    last_error=None,
                    pending=None,
                    thread=thread,
                )
                self.store.put(key, state)
            except SlackFailure as exc:
                self.failed(key, state, exc, now)
                if str(exc) == "ratelimited":
                    raise
