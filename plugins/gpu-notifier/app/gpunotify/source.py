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

"""Translate observed Kubernetes snapshots; never infer diagnostic success."""

from __future__ import annotations

from typing import Any

import json
import ssl
from datetime import datetime
from pathlib import Path

import httpx

STATE = "dgxc.nvidia.com/nvsentinel-state"
VALIDATION = "nvsentinel.nvidia.com/validation-state"
QUARANTINE = "quarantineHealthEvent"
VALIDATION_EVENTS = "quarantineValidationHealthEvent"
SESSION = "nvsentinel.nvidia.com/validation-session"
PHASES = {
    "quarantined": "Node quarantined",
    "draining": "Draining workloads",
    "drain-succeeded": "Drain reported complete",
    "drain-failed": "Drain failed; infra investigation needed",
    "remediating": "Remediation requested",
    "remediation-succeeded": "Maintenance request submitted; awaiting recovery",
    "remediation-failed": "Remediation failed; infra investigation needed",
    "validation-pending": "Waiting for GPU validation",
    "validating": "GPU validation running",
    "validation-failed": "GPU validation failed; infra investigation needed",
    "manual": "Manual repair required",
    "released": "Node Ready and uncordoned; diagnostic result not confirmed by this notifier",
    "held": "Still unschedulable; waiting for recovery",
    "unready": "Node not Ready; waiting for recovery",
    "missing": "Node removed; infra investigation needed",
}


def timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def waiting_pod(pod: dict, now: float, not_ready_seconds: int) -> bool:
    """Match the full-drain abnormal-Pod exceptions used by the pilot."""
    status, meta = pod.get("status", {}), pod["metadata"]
    if status.get("phase") in ("Succeeded", "Failed"):
        return False
    if meta.get("deletionTimestamp"):
        grace = pod.get("spec", {}).get("terminationGracePeriodSeconds", 30)
        if now > timestamp(meta["deletionTimestamp"]) + grace:
            return False
    for condition in status.get("conditions", []):
        if condition.get("type") == "Ready" and condition.get("status") == "False":
            changed = condition.get("lastTransitionTime")
            if changed and now > timestamp(changed) + not_ready_seconds:
                return False
    return True


def health_events(annotations: dict, key: str) -> list[dict]:
    events = json.loads(annotations.get(key, "[]"))
    if isinstance(events, dict):  # legacy single HealthEvent
        events = [events]
    if not isinstance(events, list) or any(not isinstance(e, dict) for e in events):
        raise ValueError("invalid health event annotation")
    return events


def observe(node: dict, pods: list[dict], now: float, not_ready_seconds: int = 300) -> dict:
    meta = node["metadata"]
    annotations, labels = meta.get("annotations", {}), meta.get("labels", {})
    # Bad annotation data must fail this node's observation, not imply recovery.
    events = health_events(annotations, QUARANTINE)
    validation_events = health_events(annotations, VALIDATION_EVENTS)
    dry_run = annotations.get("quarantineHealthEventDryRun", "").lower() == "true"
    fault_origin = not dry_run and bool(events or validation_events) and bool(node.get("spec", {}).get("unschedulable"))
    # Validation can continue a known fault, but a standalone check is not a new fault.
    active = not dry_run and bool(events or validation_events or annotations.get(SESSION) or labels.get(VALIDATION))
    phase = labels.get(VALIDATION) or labels.get(STATE) or "quarantined"
    manual_required = any(e.get("recommendedAction") in (5, "CONTACT_SUPPORT") for e in events)
    if (
        manual_required
        and phase == "quarantined"
        and all(e.get("recommendedAction") in (5, "CONTACT_SUPPORT") for e in events)
    ):
        phase = "manual"
    if not active:
        ready = any(
            c.get("type") == "Ready" and c.get("status") == "True" for c in node.get("status", {}).get("conditions", [])
        )
        phase = "held" if node.get("spec", {}).get("unschedulable") else ("released" if ready else "unready")
    tasks: dict[str, dict] = {}
    # Only request owner action while the controller is still waiting on drain.
    if active and phase in ("quarantined", "draining"):
        for pod in pods:
            pm = pod["metadata"]
            pl, pa = pm.get("labels", {}), pm.get("annotations", {})
            if pod.get("spec", {}).get("nodeName") != meta["name"]:
                continue
            if pm.get("namespace") == "skypilot-login" or not pl.get("skypilot-cluster-name"):
                continue
            if not waiting_pod(pod, now, not_ready_seconds):
                continue
            workload = pl["skypilot-cluster-name"]
            email = str(pa.get("skypilot-user") or pl.get("skypilot-user") or "").strip().lower()
            # Never guess an email from a login/name; leave ownership unresolved.
            if "@" not in email:
                email = ""
            key = json.dumps([pm["namespace"], workload, email])
            tasks[key] = {"workload": workload, "namespace": pm["namespace"], "email": email}
    reasons = sorted(
        {
            str(e.get("checkName", "Unknown check"))
            + (" [" + ", ".join(map(str, e["errorCode"])) + "]" if e.get("errorCode") else "")
            for e in events or validation_events
        }
    )
    return {
        "uid": meta["uid"],
        "node": meta["name"],
        "active": active,
        "fault_origin": fault_origin,
        "manual_required": manual_required,
        "phase": phase,
        "reason": "; ".join(reasons)[:1500],
        "tasks": tasks,
        "resource_version": meta.get("resourceVersion", ""),
    }


class KubernetesSource:
    """In-cluster GET/LIST only. Complete both lists before processing a tick."""

    def __init__(
        self,
        server: str = "https://kubernetes.default.svc",
        service_account: str = "/var/run/secrets/kubernetes.io/serviceaccount",
    ) -> None:
        self.directory = Path(service_account)
        self.client = httpx.Client(
            base_url=server,
            timeout=20,
            trust_env=False,
            verify=ssl.create_default_context(cafile=str(self.directory / "ca.crt")),
        )

    def items(self, path: str, **params: Any) -> list[dict]:
        result = []
        while True:
            response = self.client.get(
                path,
                params={"limit": 500, **params},
                headers={"Authorization": "Bearer " + (self.directory / "token").read_text().strip()},
            )
            response.raise_for_status()
            data = response.json()
            result.extend(data["items"])
            continuation = data.get("metadata", {}).get("continue")
            if not continuation:
                return result
            params["continue"] = continuation

    def snapshot(self) -> tuple[list[dict], list[dict]]:
        return self.items("/api/v1/nodes"), self.items("/api/v1/pods", labelSelector="skypilot-cluster-name")
