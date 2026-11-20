"""Unit tests for image=latest / look saves / session replay. Mocks only: no Echo or Dell."""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

_tmp = tempfile.TemporaryDirectory(prefix="latest-img-test-")
_key = Path(_tmp.name) / "echo.key"
_ssh = Path(_tmp.name) / "ssh.key"
_key.write_text("dummy-not-a-real-key\n")
_ssh.write_text("dummy-not-a-real-ssh-key\n")
os.chmod(_key, 0o600)
os.chmod(_ssh, 0o600)
os.environ["ECHO_HOST"] = "192.0.2.248"
os.environ["BRIDGE_BIND"] = "192.0.2.230"
os.environ["HERMES_API_URL"] = "http://192.0.2.230:8642"
os.environ["HERMES_PEER_DELL_URL"] = "http://192.0.2.148:8642"
os.environ["HERMES_PEER_IMAC_URL"] = "http://imac.local:8642"
os.environ["HERMES_PEER_MACMINI_URL"] = "http://192.0.2.215:8642"
os.environ["ECHO_KEY_FILE"] = str(_key)
os.environ["ECHO_SSH_KEY"] = str(_ssh)
os.environ["DELL_MEDIA_URL"] = "http://192.0.2.148:8091"
os.environ["DELL_LLM_API_KEY"] = "test-key-not-used-on-the-network"

import bridge  # noqa: E402


def _jpeg(path: Path, extra=b""):
    path.write_bytes(b"\xff\xd8\xff\xd9" + extra)
    os.chmod(path, 0o600)
    return path


def _make_db(path: Path, session_id="api-test1", source="api_server"):
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT, started_at REAL)")
    con.execute(
        """CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            role TEXT,
            content TEXT,
            tool_call_id TEXT,
            tool_calls TEXT,
            tool_name TEXT,
            timestamp REAL,
            active INTEGER DEFAULT 1
        )"""
    )
    now = time.time()
    con.execute(
        "INSERT INTO sessions (id, source, started_at) VALUES (?,?,?)",
        (session_id, source, now),
    )
    return con, now


def _add_pending_video(con, session_id, ts, call_id="call-video"):
    calls = json.dumps(
        [
            {
                "id": call_id,
                "call_id": call_id,
                "type": "function",
                "function": {
                    "name": "mcp__echo__generate_video",
                    "arguments": '{"image":"latest","seconds":5}',
                },
            }
        ]
    )
    con.execute(
        "INSERT INTO messages (session_id, role, tool_calls, timestamp, active) "
        "VALUES (?,?,?,?,1)",
        (session_id, "assistant", calls, ts),
    )


def _add_picture_result(con, session_id, ts, text):
    con.execute(
        "INSERT INTO messages (session_id, role, content, tool_name, tool_call_id, timestamp, active) "
        "VALUES (?,?,?,?,?,?,1)",
        (session_id, "tool", text, "mcp__echo__generate_picture", "pic-%s" % ts, ts),
    )


def _add_look_result(con, session_id, ts, text):
    con.execute(
        "INSERT INTO messages (session_id, role, content, tool_name, tool_call_id, timestamp, active) "
        "VALUES (?,?,?,?,?,?,1)",
        (session_id, "tool", text, "mcp__echo__look", "look-%s" % ts, ts),
    )


def _add_user(con, session_id, ts, text):
    con.execute(
        "INSERT INTO messages (session_id, role, content, timestamp, active) VALUES (?,?,?,?,1)",
        (session_id, "user", text, ts),
    )


class LatestImageResolverTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory(prefix="latest-sess-")
        root = Path(self.td.name)
        self.generated = root / "generated"
        self.look = root / "look"
        self.uploads = root / "uploads"
        self.generated.mkdir()
        self.look.mkdir()
        self.uploads.mkdir()
        self.pic = _jpeg(self.generated / "halloween.jpg")
        self.snap = _jpeg(self.look / "me.jpg")
        self.photo = _jpeg(self.uploads / "dashboard_photo.jpg")
        self.db = root / "state.db"
        self.patches = [
            patch.object(bridge, "CACHE_GENERATED", self.generated),
            patch.object(bridge, "CACHE_LOOK", self.look),
            patch.object(bridge, "_UPLOAD_ROOTS", (self.uploads,)),
            patch.object(bridge, "_IMAGES_DIR", self.uploads),
            patch.object(bridge, "_ATTACH_DIR", self.uploads),
            patch.object(bridge, "_state_db", lambda: self.db),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.td.cleanup()

    def _load(self, build):
        con, now, = None, None
        con, now = _make_db(self.db)
        sid = "api-test1"
        build(con, now, sid)
        _add_pending_video(con, sid, now)
        con.commit()
        con.close()

    def test_latest_prefers_look_over_earlier_generated(self):
        def build(con, now, sid):
            _add_picture_result(con, sid, now - 20, f"SAVED:{self.pic}")
            _add_look_result(
                con, sid, now - 10,
                f"a person with eyes closed\n\npicture sent to the Echo for 20 seconds\nSAVED:{self.snap}",
            )
        self._load(build)
        self.assertEqual(bridge._resolve_video_image("latest"), self.snap)
        self.assertEqual(bridge._resolve_video_image("this photo"), self.snap)
        self.assertEqual(bridge._resolve_video_image("that photo"), self.snap)
        self.assertEqual(bridge._resolve_video_image("last"), self.snap)
        self.assertEqual(bridge._resolve_video_image("look"), self.snap)
        self.assertEqual(bridge._resolve_video_image("generated"), self.pic)

    def test_later_upload_beats_generated_and_look(self):
        def build(con, now, sid):
            _add_picture_result(con, sid, now - 30, f"SAVED:{self.pic}")
            _add_look_result(con, sid, now - 20, f"SAVED:{self.snap}")
            _add_user(con, sid, now - 10, f"turn this into a video\nimage_url: {self.photo}")
        self._load(build)
        self.assertEqual(bridge._resolve_video_image("latest"), self.photo)
        self.assertEqual(bridge._resolve_video_image("upload"), self.photo)
        self.assertEqual(bridge._resolve_video_image("look"), self.snap)
        self.assertEqual(bridge._resolve_video_image("generated"), self.pic)

    def test_upload_alias_uses_earlier_session_upload_not_only_this_turn(self):
        def build(con, now, sid):
            _add_user(con, sid, now - 30, f"image_url: {self.photo}")
            _add_user(con, sid, now - 5, "turn it into a video")
        self._load(build)
        self.assertEqual(bridge._resolve_video_image("upload"), self.photo)
        self.assertEqual(bridge._resolve_video_image("latest"), self.photo)

    def test_latest_fails_when_none(self):
        def build(con, now, sid):
            _add_user(con, sid, now - 5, "turn it into a video")
        self._load(build)
        with self.assertRaisesRegex(RuntimeError, "no image is available in this session"):
            bridge._resolve_video_image("latest")
        with self.assertRaisesRegex(RuntimeError, "no photo was uploaded in this session"):
            bridge._resolve_video_image("upload")
        with self.assertRaisesRegex(RuntimeError, "no camera photo was taken in this session"):
            bridge._resolve_video_image("look")

    def test_latest_fails_when_event_has_two_paths(self):
        other = _jpeg(self.look / "other.jpg")
        def build(con, now, sid):
            _add_look_result(con, sid, now - 10, f"SAVED:{self.snap}\nSAVED:{other}")
        self._load(build)
        with self.assertRaisesRegex(RuntimeError, "more than one path"):
            bridge._resolve_video_image("latest")

    def test_text_is_prompt_only_and_missing_image_fails(self):
        def build(con, now, sid):
            pass
        self._load(build)
        self.assertIsNone(bridge._resolve_video_image("text"))
        self.assertIsNone(bridge._resolve_video_image("TEXT"))
        with self.assertRaisesRegex(RuntimeError, r'image is required; pass image="text"'):
            bridge._resolve_video_image("")
        with self.assertRaisesRegex(RuntimeError, r'image is required; pass image="text"'):
            bridge._resolve_video_image("   ")

    def test_explicit_look_path_is_accepted(self):
        def build(con, now, sid):
            pass
        self._load(build)
        self.assertEqual(bridge._resolve_video_image(str(self.snap)), self.snap)

    def test_look_result_without_saved_is_ignored(self):
        def build(con, now, sid):
            _add_picture_result(con, sid, now - 20, f"SAVED:{self.pic}")
            _add_look_result(con, sid, now - 10, "a person. The photo is served from memory.")
        self._load(build)
        self.assertEqual(bridge._resolve_video_image("latest"), self.pic)
        with self.assertRaisesRegex(RuntimeError, "no camera photo was taken"):
            bridge._resolve_video_image("look")


class LookSnapshotSaveTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory(prefix="look-save-")
        self.look = Path(self.td.name) / "look"
        self.look.mkdir()
        self.patch = patch.object(bridge, "CACHE_LOOK", self.look)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        self.td.cleanup()

    def test_save_look_snapshot_writes_jpeg(self):
        jpeg = b"\xff\xd8\xff\xd9" + b"abcd"
        path = Path(bridge._save_look_snapshot(jpeg))
        self.assertTrue(path.is_file())
        self.assertEqual(path.read_bytes(), jpeg)
        self.assertEqual(path.parent, self.look)
        self.assertTrue(path.name.endswith(".jpg"))

    def test_save_look_snapshot_rejects_non_jpeg(self):
        with self.assertRaisesRegex(RuntimeError, "camera did not return a JPEG"):
            bridge._save_look_snapshot(b"not-a-jpeg")

    async def test_echo_look_returns_saved_path(self):
        jpeg = b"\xff\xd8\xff\xd9" + b"frame"

        @asynccontextmanager
        async def fake_lan(_jpeg):
            yield ("slot", "http://127.0.0.1/look.jpg")

        with patch.object(bridge, "_vision_is_on", lambda: False), \
             patch.object(bridge, "_turn_origin", lambda _n: "echo"), \
             patch.object(bridge, "_capture_still", AsyncMock(return_value=jpeg)), \
             patch.object(bridge, "_lan_jpeg", fake_lan), \
             patch.object(bridge, "_show_picture_checked", AsyncMock(return_value="picture sent to the Echo")), \
             patch.object(bridge, "_wait_echo_fetched", AsyncMock()), \
             patch.object(bridge, "_describe_jpeg", lambda *_a, **_k: "a person with eyes closed"):
            out = await bridge.look()
        self.assertIn("SAVED:", out)
        self.assertIn("a person with eyes closed", out)
        saved = Path(out.split("SAVED:", 1)[1].strip())
        self.assertTrue(saved.is_file())
        self.assertEqual(saved.read_bytes(), jpeg)

    async def test_chat_look_does_not_write(self):
        jpeg = b"\xff\xd8\xff\xd9" + b"frame"
        with patch.object(bridge, "_vision_is_on", lambda: False), \
             patch.object(bridge, "_turn_origin", lambda _n: "chat"), \
             patch.object(bridge, "_capture_still", AsyncMock(return_value=jpeg)), \
             patch.object(bridge, "_describe_jpeg", lambda *_a, **_k: "a person"):
            out = await bridge.look()
        self.assertEqual(out, "a person")
        self.assertNotIn("SAVED:", out)
        self.assertEqual(list(self.look.iterdir()), [])


class SessionReplayMemoryTests(unittest.TestCase):
    def setUp(self):
        self.by = {}
        self.p1 = patch.object(bridge, "_last_video_by_session", self.by)
        self.p2 = patch.object(bridge, "_last_video", None)
        self.p1.start()
        self.p2.start()

    def tearDown(self):
        self.p1.stop()
        self.p2.stop()

    def test_remembers_per_session(self):
        with patch.object(bridge, "_pending_sessions", lambda *a, **k: [("sess-a", "api_server")]):
            bridge._remember_last_video("generate_video", {"url": "http://x/a.mp4", "title": "A"})
        with patch.object(bridge, "_pending_sessions", lambda *a, **k: [("sess-b", "api_server")]):
            bridge._remember_last_video("generate_video", {"url": "http://x/b.mp4", "title": "B"})
            self.assertEqual(bridge._last_video_in_session()["url"], "http://x/b.mp4")
        with patch.object(bridge, "_pending_sessions", lambda *a, **k: [("sess-a", "api_server")]):
            self.assertEqual(bridge._last_video_in_session()["url"], "http://x/a.mp4")

    def test_fails_when_this_session_has_none(self):
        with patch.object(bridge, "_pending_sessions", lambda *a, **k: [("sess-z", "api_server")]):
            with self.assertRaisesRegex(RuntimeError, "no last video in this conversation"):
                bridge._last_video_in_session()

    def test_fails_when_session_is_ambiguous(self):
        with patch.object(bridge, "_pending_sessions", lambda *a, **k: [("a", "api_server"), ("b", "api_server")]):
            with self.assertRaisesRegex(RuntimeError, "cannot tell which conversation"):
                bridge._last_video_in_session()

    def test_replay_last_video_plays_session_clip(self):
        rec = {"url": "http://192.0.2.148:8091/clip.mp4", "title": "clip"}
        played = []

        async def fake_play(url, title):
            played.append((url, title))
            return "playing on the Echo"

        async def run():
            with patch.object(bridge, "_pending_sessions", lambda *a, **k: [("sess-a", "api_server")]), \
                 patch.object(bridge, "_last_video_check", lambda url: None), \
                 patch.object(bridge, "_play_video_confirmed", fake_play):
                bridge._remember_last_video("generate_video", rec)
                out = await bridge.replay_last_video()
                alias = await bridge.replay_video()
            return out, alias

        out, alias = asyncio.run(run())
        self.assertEqual(out, "playing on the Echo")
        self.assertEqual(alias, "playing on the Echo")
        self.assertEqual(played, [(rec["url"], "clip"), (rec["url"], "clip")])


if __name__ == "__main__":
    unittest.main()
