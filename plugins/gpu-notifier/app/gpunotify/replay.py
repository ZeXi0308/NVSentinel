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

"""A synthetic incident split across processes to verify Mongo-backed delivery."""

from __future__ import annotations

from typing import Any, Callable

import re
import time

from app.gpunotify.engine import Notifier

PHASES = ("quarantined", "draining", "remediation-succeeded", "validating", "released")
NODE_KEY = "node:replay-node-uid"


def replay_scope(session: str, channel: str, send: bool) -> str:
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", session):
        raise ValueError("replay requires --session with 1-64 letters, digits, underscores or hyphens")
    # Preview timestamps must never be reused as real Slack message timestamps.
    return f"simulation:{session}:{'slack' if send else 'preview'}:{channel}"


class NoOwners:
    def resolve(self, email: str) -> str | None:
        raise AssertionError("the synthetic replay must not look up or notify owners")


def replay_stage(
    store: Any, sender: Any, *, session: str, channel: str, stage: str, clock: Callable[[], float] = time.time
) -> dict:
    """Run start/resume with a stable session and checkpoint each delivered phase.

    `start` ends at draining; `resume` uses the saved parent through release.
    Repeating a completed stage performs no delivery. If a delivery failed,
    rerunning the same stage resumes it after the persisted retry deadline.
    """
    if stage not in ("start", "resume"):
        raise ValueError("replay stage must be start or resume")
    replay_scope(session, channel, False)  # Validate even when called without the CLI.
    state = store.get("replay")
    if state is None:
        if stage != "start":
            raise ValueError("run replay --stage start with the same session and channel first")
        state = {"session": session, "channel": channel, "next_phase": 0}
        store.put("replay", state)
    elif state.get("session") != session or state.get("channel") != channel:
        raise ValueError("replay session does not match the persisted configuration")
    if stage == "resume" and state["next_phase"] < 2:
        raise ValueError("finish the start stage before resuming")

    notifier = Notifier(store, sender, NoOwners(), "simulation-" + session, channel, simulated=True)
    stop = 2 if stage == "start" else len(PHASES)
    while state["next_phase"] < stop:
        phase = PHASES[state["next_phase"]]
        sample = {
            "uid": "replay-node-uid",
            "node": "synthetic-gpu-node",
            "active": phase != "released",
            "phase": phase,
            "reason": "Synthetic GPU fault; Mongo persistence test",
            "tasks": {},
        }
        notifier.tick([sample], clock())
        doc = store.get(NODE_KEY)
        if not doc or not doc.get("thread") or doc["pending"] or doc.get("last_error"):
            raise RuntimeError("replay delivery is pending; rerun the same stage after the retry delay")
        if doc.get("parent_text") != notifier.summary(doc):
            raise RuntimeError("replay parent update is pending; rerun the same stage after the retry delay")
        state["next_phase"] += 1
        store.put("replay", state)

    doc = store.get(NODE_KEY)
    if not doc:
        raise RuntimeError("the replay incident is missing from Mongo")
    return {
        "operation": "replay_checkpoint",
        "session": session,
        "stage": stage,
        "incident": doc["incident"],
        "thread_ts": doc["thread"],
        "phase": doc["phase"],
        "completed": state["next_phase"] == len(PHASES),
    }
