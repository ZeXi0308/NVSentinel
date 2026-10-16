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

"""Read-only circuit-breaker observations and durable Slack trip notifications."""

from __future__ import annotations

from typing import Any

import html
import logging
import re
import uuid

from app.gpunotify.slack import SlackFailure

log = logging.getLogger(__name__)
STATE_KEY = "breaker:state"
TRIP_PREFIX = "breaker-trip:"


def fetch_breaker(source: Any, namespace: str = "nvsentinel") -> dict | None:
    """The NVSentinel v1.27 ConfigMap stores data.status=CLOSED or TRIPPED."""
    if not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", namespace):
        raise ValueError("invalid circuit-breaker namespace")
    response = source.client.get(
        f"/api/v1/namespaces/{namespace}/configmaps/circuit-breaker",
        headers={"Authorization": "Bearer " + (source.directory / "token").read_text().strip()},
        timeout=5,
    )
    if response.status_code == 404:
        return None  # An absent ConfigMap does not prove that a trip was reset.
    response.raise_for_status()
    return response.json()


class BreakerNotifier:
    """Each observed trip has its own persisted thread, independent of Node reads."""

    def __init__(
        self, store: Any, sender: Any, cluster: str, infra_ids: tuple[str, ...] = (), simulated: bool = False
    ) -> None:
        self.store, self.sender = store, sender
        self.cluster = html.escape(str(cluster), quote=False).replace("\n", " ")
        self.infra_ids = tuple(infra_ids)
        if any(not re.fullmatch(r"[UW][A-Z0-9]+", uid) for uid in self.infra_ids):
            raise ValueError("infra IDs must be Slack user IDs")
        self.prefix = "[SIMULATION] " if simulated else ""

    def ingest(self, snapshot: Any, now: float) -> None:
        if snapshot is None:
            return
        metadata = snapshot.get("metadata") or {}
        uid = metadata.get("uid")
        status = (snapshot.get("data") or {}).get("status")
        if not uid or status not in ("TRIPPED", "CLOSED"):
            raise ValueError("invalid circuit-breaker ConfigMap status or identity")
        current = self.store.get(STATE_KEY) or {}
        changed = (current.get("uid"), current.get("status")) != (uid, status)
        if not changed and current.get("resource_version") == metadata.get("resourceVersion"):
            return
        if status == "TRIPPED" and (changed or not current.get("trip")):
            if current.get("trip"):
                if not current["closed"]:
                    current.update(closed=True, closed_at=now, close_kind="replaced", infra_pending=False)
                self.store.put(TRIP_PREFIX + current["trip"], current)
            current = {
                "trip": str(uuid.uuid4()),
                "configmap_uid": uid,
                "started": now,
                "closed": False,
                "thread": None,
                "parent_text": None,
                "close_sent": False,
                "infra_sent": False,
                "infra_pending": True,
                "retry_at": 0,
                "failures": 0,
            }
        elif status == "CLOSED" and current.get("trip") and not current["closed"]:
            current.update(closed=True, closed_at=now, close_kind="reset", infra_pending=False)
        current.update(uid=uid, status=status, resource_version=metadata.get("resourceVersion"), observed_at=now)
        # The observed transition and its notification state share one Mongo document.
        self.store.put(STATE_KEY, current)

    def summary(self, doc: dict) -> str:
        if not doc["closed"]:
            state = (
                "TRIPPED | Fault-quarantine event processing is halted, including new "
                "quarantines and recovery events; infra must investigate and reset"
            )
        elif doc.get("close_kind") == "reset":
            state = "ConfigMap CLOSED after trip | Confirm fault-quarantine event processing resumed"
        else:
            state = "ConfigMap replaced after trip | Follow the new circuit-breaker state"
        return f"{self.prefix}Circuit breaker | {self.cluster} | {state} | trip={doc['trip'][:8]}"

    def deliver_pending(self, now: float) -> None:
        for key, doc in sorted(self._records(), key=lambda item: item[1]["started"]):
            if now < doc.get("retry_at", 0):
                continue
            needs_mention = not doc["closed"] and not doc["infra_sent"] and bool(self.infra_ids)
            if (
                doc["thread"]
                and not doc.get("retry_at")
                and not needs_mention
                and (not doc["closed"] or doc["close_sent"])
                and doc["parent_text"] == self._parent_text(doc)
            ):
                continue
            try:
                self._deliver(key, doc)
            except SlackFailure as exc:
                failures = doc.get("failures", 0) + 1
                doc.update(
                    failures=failures,
                    last_error=str(exc),
                    retry_at=now + max(exc.retry_after, min(900, 30 * 2 ** min(failures - 1, 5))),
                )
                self.store.put(key, doc)
                log.warning("breaker_delivery_failed error=%s", type(exc).__name__)
                if str(exc) == "ratelimited":
                    raise

    def _deliver(self, key: str, doc: dict) -> None:
        mentions = " ".join(f"<@{uid}>" for uid in self.infra_ids)
        if not doc["thread"]:
            text = self.summary(doc)
            if not doc["closed"]:
                text += " | " + mentions if mentions else " | Infra escalation pending: configure GPU_NOTIFY_INFRA_IDS"
            self.store.guard()
            doc["thread"] = self.sender.post(text)
            doc["parent_text"] = text
            doc["infra_sent"] = bool(mentions) and not doc["closed"]
            doc["infra_pending"] = not doc["closed"] and not doc["infra_sent"]
            # The first message already describes a reset observed before delivery.
            doc["close_sent"] = doc["closed"]
            self.store.put(key, doc)
        if doc["closed"] and not doc["close_sent"]:
            self.store.guard()
            self.sender.post(self.summary(doc), doc["thread"])
            doc["close_sent"] = True
            self.store.put(key, doc)
        if not doc["closed"] and not doc["infra_sent"] and mentions:
            self.store.guard()
            self.sender.post(
                f"{self.prefix}{mentions} | {self.cluster} | Circuit breaker TRIPPED; "
                "please investigate and reset fault-quarantine.",
                doc["thread"],
            )
            doc.update(infra_sent=True, infra_pending=False)
            self.store.put(key, doc)
        desired = self._parent_text(doc)
        if doc["parent_text"] != desired:
            self.store.guard()
            self.sender.update(doc["thread"], desired)
            doc["parent_text"] = desired
            self.store.put(key, doc)
        doc.update(retry_at=0, failures=0, last_error="infra_unconfigured" if doc["infra_pending"] else None)
        self.store.put(key, doc)

    def _parent_text(self, doc: dict) -> str:
        text = self.summary(doc)
        if not doc["closed"]:
            mentions = " ".join(f"<@{uid}>" for uid in self.infra_ids)
            if doc["infra_sent"] and mentions:
                text += " | " + mentions
            elif not doc["infra_sent"]:
                text += " | Infra escalation pending: configure GPU_NOTIFY_INFRA_IDS"
        return text

    def pending_count(self) -> int:
        return sum(
            not doc["thread"]
            or doc.get("infra_pending", False)
            or (doc["closed"] and not doc["close_sent"])
            or bool(doc.get("retry_at"))
            for _, doc in self._records()
        )

    def _records(self) -> list[tuple[str, dict]]:
        current = self.store.get(STATE_KEY) or {}
        records = [(key, doc) for key, doc in self.store.items(TRIP_PREFIX) if doc["trip"] != current.get("trip")]
        if current.get("trip"):
            records.append((STATE_KEY, current))
        return records
