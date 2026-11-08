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

"""A requested recheck must observe the fault again, not replay a cached event."""

import json
import time
from threading import Event

import pytest

from gpu_health_monitor.dcgm_watcher.types import ErrorDetails, HealthDetails, HealthStatus
from gpu_health_monitor.platform_connector.platform_connector import (
    PlatformConnectorConfig,
    PlatformConnectorEventProcessor,
)
from gpu_health_monitor.protos import health_event_pb2 as pb

CODE = "DCGM_FR_VOLATILE_DBE_DETECTED"
WATCH = "DCGM_HEALTH_WATCH_MEM"


@pytest.fixture
def recheck_processor(tmp_path, monkeypatch):
    for key in list(__import__("os").environ):
        if key.startswith("HEALTH_PUBLISH_"):
            monkeypatch.delenv(key)
    metadata = tmp_path / "gpu.json"
    metadata.write_text(json.dumps({"gpus": [{"gpu_id": 0, "uuid": "GPU-test", "pci_address": "0000:18:00.0"}]}))
    state = tmp_path / "state"
    state.write_text("test-boot")
    request = tmp_path / "request"
    request.write_text("")
    config = PlatformConnectorConfig(
        socket_path="unused",
        node_name="gpu-20",
        dcgm_errors_info_dict={CODE: "COMPONENT_RESET"},
        state_file_path=str(state),
        metadata_path=str(metadata),
        processing_strategy=pb.EXECUTE_REMEDIATION,
        recheck_request_path=str(request),
    )
    processor = PlatformConnectorEventProcessor(config, Event())
    sent = []

    def publish(events, **kwargs):
        sent.extend(pb.HealthEvent.FromString(e.SerializeToString()) for e in events)
        return True

    monkeypatch.setattr(processor, "send_health_event_with_retries", publish)
    yield processor, request, sent
    processor.close()


def poll(processor, failing=True, complete=True, observed_at=None):
    processor.health_event_occurred(
        {
            WATCH: HealthDetails(
                HealthStatus.FAIL if failing else HealthStatus.PASS,
                {0: [ErrorDetails(CODE, "Detected one volatile DBE")]} if failing else {},
                is_complete=complete,
                observed_at=time.monotonic() if observed_at is None else observed_at,
            )
        },
        [0],
    )


def request_recheck(path, request_id="first"):
    path.write_text(json.dumps({"id": request_id, "nodeUID": "node-uid", "errorCodes": [CODE]}))


def test_persistent_fault_is_confirmed_once_after_request(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 1
    request_recheck(path)
    poll(processor)  # Read the request; this poll may have started before it.
    assert len([e for e in sent if not e.isHealthy]) == 1
    poll(processor)
    faults = [e for e in sent if not e.isHealthy]
    assert len(faults) == 2
    assert faults[-1].metadata["gpuRecheckRequest"] == "first"
    assert faults[-1].metadata["gpuRecheckNodeUID"] == "node-uid"
    assert [(e.entityType, e.entityValue) for e in faults[-1].entitiesImpacted] == [
        ("GPU", "0"),
        ("PCI", "0000:18:00.0"),
        ("GPU_UUID", "GPU-test"),
    ]
    assert faults[-1].generatedTimestamp.ToNanoseconds() > faults[0].generatedTimestamp.ToNanoseconds()
    for _ in range(3):
        poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2


def test_recovered_fault_is_not_replayed(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor, failing=False)
    poll(processor, failing=False)
    assert len([e for e in sent if not e.isHealthy]) == 1
    assert sent[-1].isHealthy


def test_incomplete_check_waits_for_complete_observation(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor)
    poll(processor, complete=False)
    assert len([e for e in sent if not e.isHealthy]) == 1
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2


def test_failed_delivery_retries_same_request(recheck_processor, monkeypatch):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor)
    publish = processor.send_health_event_with_retries
    monkeypatch.setattr(processor, "send_health_event_with_retries", lambda *args, **kwargs: False)
    poll(processor)
    monkeypatch.setattr(processor, "send_health_event_with_retries", publish)
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2


def test_removed_request_cancels_pending_recheck(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor)
    path.write_text("")
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 1


def test_queued_old_samples_cannot_confirm_fault(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    queued_at = time.monotonic()
    request_recheck(path)
    poll(processor, observed_at=queued_at)
    poll(processor, observed_at=queued_at)
    poll(processor, observed_at=queued_at)
    assert len([e for e in sent if not e.isHealthy]) == 1
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2


@pytest.mark.parametrize("raw", ["{}", "{", "[]", "x" * 4097, '{"id":"a","nodeUID":"n","errorCodes":["other"]}'])
def test_invalid_request_does_not_republish(recheck_processor, raw):
    processor, path, sent = recheck_processor
    poll(processor)
    path.write_text(raw)
    poll(processor)
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 1


def test_store_only_is_not_promoted_by_recheck(recheck_processor):
    processor, path, sent = recheck_processor
    processor._processing_strategy = pb.STORE_ONLY
    poll(processor)
    request_recheck(path)
    poll(processor)
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 1
    assert all(e.processingStrategy == pb.STORE_ONLY for e in sent)


def test_healthy_confirmation_leaves_future_fault_detection_enabled(recheck_processor):
    processor, path, sent = recheck_processor
    request_recheck(path)
    poll(processor, failing=False)
    poll(processor, failing=False)
    poll(processor)
    faults = [e for e in sent if not e.isHealthy]
    assert len(faults) == 1
    assert "gpuRecheckRequest" not in faults[0].metadata


def test_missing_timestamp_is_not_fresh_evidence(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor)
    poll(processor, observed_at=0)
    assert len([e for e in sent if not e.isHealthy]) == 1


def test_partial_gpu_coverage_cannot_complete_request(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor)
    processor.health_event_occurred(
        {
            WATCH: HealthDetails(
                HealthStatus.FAIL,
                {0: [ErrorDetails(CODE, "DBE")]},
                evaluated_gpu_ids={0},
                observed_at=time.monotonic(),
            )
        },
        [0, 1],
    )
    assert len([e for e in sent if not e.isHealthy]) == 1
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2


def test_multiple_gpus_confirmed_in_one_request(recheck_processor):
    processor, path, sent = recheck_processor

    def both():
        processor.health_event_occurred(
            {
                WATCH: HealthDetails(
                    HealthStatus.FAIL,
                    {0: [ErrorDetails(CODE, "DBE")], 1: [ErrorDetails(CODE, "DBE")]},
                    observed_at=time.monotonic(),
                )
            },
            [0, 1],
        )

    both()
    request_recheck(path)
    both()
    both()
    both()
    faults = [e for e in sent if not e.isHealthy]
    assert len(faults) == 4
    assert len([e for e in faults if e.metadata.get("gpuRecheckRequest") == "first"]) == 2
