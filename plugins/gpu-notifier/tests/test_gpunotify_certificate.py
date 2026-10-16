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

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.gpunotify.__main__ import CertificateWatcher


class CertificateWatcherTests(unittest.TestCase):
    def test_content_rotation_detected_on_next_poll(self):
        with tempfile.TemporaryDirectory() as directory:
            pem = Path(directory) / "client.pem"
            pem.write_bytes(b"test-certificate-one")
            watcher = CertificateWatcher(str(pem))
            self.assertFalse(watcher.changed(100))
            pem.write_bytes(b"test-certificate-two")
            self.assertFalse(watcher.changed(129))
            self.assertTrue(watcher.changed(130))

    def test_unchanged_contents_and_disabled_watcher_do_not_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            pem = Path(directory) / "client.pem"
            pem.write_bytes(b"test-certificate")
            watcher = CertificateWatcher(str(pem))
            pem.write_bytes(b"test-certificate")
            self.assertFalse(watcher.changed(100))
            self.assertFalse(watcher.changed(130))
        with patch.object(Path, "read_bytes", side_effect=AssertionError("must not read")):
            self.assertFalse(CertificateWatcher(None).changed(100))

    def test_transient_read_error_retries_and_preserves_original_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            pem = Path(directory) / "client.pem"
            pem.write_bytes(b"old-certificate")
            watcher = CertificateWatcher(str(pem))
            with patch.object(Path, "read_bytes", side_effect=PermissionError("sensitive-path")):
                with self.assertLogs("app.gpunotify.__main__", level="WARNING") as logs:
                    self.assertFalse(watcher.changed(100))
                self.assertNotIn("sensitive-path", "".join(logs.output))
            pem.write_bytes(b"new-certificate")
            self.assertTrue(watcher.changed(130))


if __name__ == "__main__":
    unittest.main()
