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

"""Pilot state lives in a dedicated collection in the existing Mongo database."""

from __future__ import annotations

from typing import Any

import copy
import time
import uuid


class MemoryStore:
    """Tests/replay only; live runs always require Mongo."""

    def __init__(self) -> None:
        self.data = {}

    def guard(self) -> None:
        pass

    def get(self, key: str) -> dict | None:
        return copy.deepcopy(self.data.get(key))

    def put(self, key: str, value: Any) -> None:
        self.data[key] = copy.deepcopy(value)

    def items(self, prefix: str) -> list[tuple[str, dict]]:
        return [(k, copy.deepcopy(v)) for k, v in self.data.items() if k.startswith(prefix)]


class MongoStore:
    def __init__(self, uri: str, database: str, scope: str) -> None:
        from pymongo import MongoClient
        from pymongo.write_concern import WriteConcern

        self.client = MongoClient(uri, serverSelectionTimeoutMS=10000, socketTimeoutMS=10000)
        self.collection = self.client[database].get_collection(
            "gpu_notification_state", write_concern=WriteConcern(w="majority")
        )
        self.scope, self.owner = scope, str(uuid.uuid4())
        self.lease_id = scope + ":lease"
        self.guard()

    def guard(self) -> None:
        from pymongo.errors import DuplicateKeyError

        now = time.time()
        try:
            result = self.collection.update_one(
                {"_id": self.lease_id, "$or": [{"owner": self.owner}, {"expires": {"$lt": now}}]},
                {"$set": {"owner": self.owner, "expires": now + 300}},
                upsert=True,
            )
        except DuplicateKeyError:
            raise RuntimeError("another GPU notifier holds this cluster/channel lease") from None
        if not result.acknowledged:
            raise RuntimeError("notification lease write not acknowledged")

    def get(self, key: str) -> dict | None:
        doc = self.collection.find_one({"_id": self.scope + ":" + key})
        return doc["value"] if doc else None

    def put(self, key: str, value: Any) -> None:
        self.guard()
        self.collection.replace_one(
            {"_id": self.scope + ":" + key},
            {"_id": self.scope + ":" + key, "scope": self.scope, "key": key, "value": value},
            upsert=True,
        )

    def items(self, prefix: str) -> list[tuple[str, dict]]:
        return [
            (d["key"], d["value"]) for d in self.collection.find({"scope": self.scope, "key": {"$regex": "^" + prefix}})
        ]

    def close(self) -> None:
        self.collection.delete_one({"_id": self.lease_id, "owner": self.owner})
        self.client.close()
