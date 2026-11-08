# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
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

"""One fresh memory-health observation for a controller-issued Pod annotation."""

import json
import time
import logging as log
from dataclasses import dataclass

from gpu_health_monitor.dcgm_watcher.types import HealthDetails, HealthStatus

MEMORY_WATCH = "DCGM_HEALTH_WATCH_MEM"
ECC_ERROR = "DCGM_FR_VOLATILE_DBE_DETECTED"


@dataclass(frozen=True)
class Request:
    id: str
    node_uid: str

    def metadata(self) -> dict[str, str]:
        return {"gpuRecheckRequest": self.id, "gpuRecheckNodeUID": self.node_uid}


class FaultRecheck:
    def __init__(self, path: str | None) -> None:
        self.path = path
        self.observed: Request | None = None
        self.completed: Request | None = None
        self.observed_at = 0.0

    def read(self) -> Request | None:
        if not self.path:
            return None
        try:
            with open(self.path) as stream:
                raw = stream.read(4097)
            if not raw.strip():
                return None
            if len(raw) > 4096:
                raise ValueError("request exceeds 4096 bytes")
            data = json.loads(raw)
            if not isinstance(data, dict) or data.get("errorCodes") != [ECC_ERROR]:
                raise ValueError("only volatile double-bit ECC rechecks are supported")
            for key in ("id", "nodeUID"):
                if not isinstance(data.get(key), str) or not 0 < len(data[key]) <= 128:
                    raise ValueError(f"invalid {key}")
            return Request(data["id"], data["nodeUID"])
        except (OSError, ValueError) as exc:
            log.warning("Cannot read GPU fault recheck request: %s", exc)
            return None

    def for_snapshot(self, details: dict[str, HealthDetails], gpu_ids: list[int]) -> Request | None:
        current = self.read()
        previous, self.observed = self.observed, current
        if current != previous:
            self.observed_at = time.monotonic()
        # Callbacks can queue behind a slow publish; their arrival order does not
        # prove freshness. Require a hardware check started after this request.
        if current is None or current != previous or current == self.completed:
            return None
        memory = details.get(MEMORY_WATCH)
        if not memory or not memory.is_complete or not gpu_ids or memory.observed_at <= self.observed_at:
            return None
        if memory.status not in (HealthStatus.PASS, HealthStatus.WARN, HealthStatus.FAIL):
            return None
        if memory.evaluated_gpu_ids is not None and not set(gpu_ids) <= memory.evaluated_gpu_ids:
            return None
        return current

    def complete(self, request: Request | None) -> None:
        if request is not None:
            self.completed = request
