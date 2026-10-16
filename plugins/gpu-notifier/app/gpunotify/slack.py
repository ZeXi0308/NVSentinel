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

"""Slack transport and owner lookup for the independent notification plugin."""

from __future__ import annotations

import time
import re

import httpx


class OwnerResolver:
    """Look up the complete email; the engine persists unresolved-owner retries."""

    def __init__(self, token: str) -> None:
        self.token = token

    def resolve(self, email: str) -> str | None:
        try:
            response = httpx.get(
                "https://slack.com/api/users.lookupByEmail",
                headers={"Authorization": "Bearer " + self.token},
                params={"email": email},
                timeout=3.0,
                trust_env=False,
            )
            if response.status_code >= 400:
                return None
            body = response.json()
            if not isinstance(body, dict) or not body.get("ok"):
                return None
            user = body.get("user")
            uid = user.get("id") if isinstance(user, dict) else None
            return uid if isinstance(uid, str) and re.fullmatch(r"[UW][A-Z0-9]+", uid) else None
        except (httpx.HTTPError, ValueError):
            return None


class SlackFailure(RuntimeError):
    def __init__(self, code: str, retry_after: float = 30) -> None:
        super().__init__(code)
        self.retry_after = retry_after


class GpuSlackSender:
    def __init__(self, channel_id: str, token: str) -> None:
        self._channel_id, self._token = channel_id, token
        self.next_send = 0.0

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": "Bearer " + self._token,
            "Content-Type": "application/json; charset=utf-8",
        }

    def _request(self, method: str, payload: dict) -> dict:
        # Slack normally permits roughly one message/second/channel.
        time.sleep(max(0, self.next_send - time.monotonic()))
        self.next_send = time.monotonic() + 1.1
        try:
            response = httpx.post(
                "https://slack.com/api/" + method, headers=self._headers(), json=payload, timeout=10, trust_env=False
            )
        except httpx.HTTPError:
            raise SlackFailure("slack_transport_error") from None
        if response.status_code == 429:
            raise SlackFailure("ratelimited", max(1, float(response.headers.get("Retry-After", 30))))
        if response.status_code >= 400:
            raise SlackFailure("slack_http_" + str(response.status_code))
        try:
            body = response.json()
        except ValueError:
            raise SlackFailure("slack_invalid_response") from None
        if not body.get("ok"):
            raise SlackFailure(str(body.get("error", "unknown_error")))
        return body

    def _post(self, payload: dict) -> dict:
        return self._request("chat.postMessage", payload)

    def post(self, text: str, thread_ts: str | None = None) -> str:
        payload = {"channel": self._channel_id, "text": text, "unfurl_links": False, "unfurl_media": False}
        if thread_ts:
            payload["thread_ts"] = thread_ts
        return str(self._post(payload)["ts"])

    def update(self, ts: str, text: str) -> None:
        self._request("chat.update", {"channel": self._channel_id, "ts": ts, "text": text})
