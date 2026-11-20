"""Unit tests for Hermex upload persistence until session end. No network."""
from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path.home() / ".hermes" / "hermes-agent"))
from tui_gateway.upload_persist import (  # noqa: E402
    SESSION_PATHS_KEY,
    discard_session_uploaded_images,
    remember_session_uploaded_images,
    safe_upload_path,
    sweep_orphaned_cache_uploads,
)


class UploadPersistTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory(prefix="upload-persist-")
        self.cache = Path(self.td.name) / "cache"
        self.other = Path(self.td.name) / "other"
        self.cache.mkdir()
        self.other.mkdir()
        self.photo = self.cache / "dashboard_photo.jpg"
        self.photo.write_bytes(b"\xff\xd8\xff\xd9hello")
        self.session = {SESSION_PATHS_KEY: []}

    def tearDown(self):
        self.td.cleanup()

    def test_remember_does_not_delete(self):
        session = {}
        with unittest.mock.patch(
            "tui_gateway.upload_persist.upload_roots", lambda: [self.cache.resolve()]
        ):
            remember_session_uploaded_images(
                session, None, f"see image_url: {self.photo}"
            )
        self.assertTrue(self.photo.is_file())
        self.assertEqual(session[SESSION_PATHS_KEY], [str(self.photo.resolve())])

    def test_discard_at_session_end_deletes(self):
        session = {}
        with unittest.mock.patch(
            "tui_gateway.upload_persist.upload_roots", lambda: [self.cache.resolve()]
        ):
            remember_session_uploaded_images(session, [str(self.photo)], "")
            discard_session_uploaded_images(session)
        self.assertFalse(self.photo.exists())
        self.assertNotIn(SESSION_PATHS_KEY, session)

    def test_safe_path_skips_symlink_and_outside(self):
        outside = self.other / "outside.jpg"
        outside.write_bytes(b"x")
        link = self.cache / "link.jpg"
        os.symlink(outside, link)
        roots = [self.cache.resolve()]
        self.assertIsNone(safe_upload_path(str(outside), roots))
        self.assertIsNone(safe_upload_path(str(link), roots))
        self.assertEqual(safe_upload_path(str(self.photo), roots), self.photo.resolve())

    def test_sweep_deletes_only_old_cache_files(self):
        old = self.cache / "old.jpg"
        new = self.cache / "new.jpg"
        old.write_bytes(b"old")
        new.write_bytes(b"new")
        now = time.time()
        os.utime(old, (now - 25 * 3600, now - 25 * 3600))
        os.utime(new, (now, now))
        outside = self.other / "old-out.jpg"
        outside.write_bytes(b"out")
        os.utime(outside, (now - 25 * 3600, now - 25 * 3600))
        n = sweep_orphaned_cache_uploads(now=now, max_age_s=24 * 3600, root=self.cache)
        self.assertEqual(n, 1)
        self.assertFalse(old.exists())
        self.assertTrue(new.is_file())
        self.assertTrue(outside.is_file())


# import mock for patch
import unittest.mock  # noqa: E402

if __name__ == "__main__":
    unittest.main()
