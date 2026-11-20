"""Generic long jobs: immediate acknowledgement, the job registry (origin, ETA, result), completion delivery
(Echo speech or the chat outbox drained by the echo-job-notify Hermes plugin), Echo speech, and the local-file
video relay. Nothing touches the real Dell, Echo, Hermes or ~/.hermes."""
from __future__ import annotations

import asyncio
import importlib.util
import json
import re
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image

_tmp = tempfile.TemporaryDirectory(prefix="long-jobs-test-")
_key = Path(_tmp.name) / "echo.key"
_ssh = Path(_tmp.name) / "ssh.key"
_key.write_text("dummy-not-a-real-key\n")
_ssh.write_text("dummy-not-a-real-ssh-key\n")
os.environ.setdefault("ECHO_HOST", "192.0.2.248")
os.environ.setdefault("BRIDGE_BIND", "192.0.2.230")
os.environ.setdefault("HERMES_API_URL", "http://192.0.2.230:8642")
os.environ.setdefault("HERMES_PEER_DELL_URL", "http://192.0.2.148:8642")
os.environ.setdefault("HERMES_PEER_IMAC_URL", "http://imac.local:8642")
os.environ.setdefault("HERMES_PEER_MACMINI_URL", "http://192.0.2.215:8642")
os.environ.setdefault("ECHO_KEY_FILE", str(_key))
os.environ.setdefault("ECHO_SSH_KEY", str(_ssh))
os.environ.setdefault("DELL_MEDIA_URL", "http://192.0.2.148:8091")
os.environ.setdefault("DELL_LLM_API_KEY", "test-key-not-used-on-the-network")

import bridge  # noqa: E402
import long_jobs  # noqa: E402
import video_relay  # noqa: E402

CHAT_ORIGIN = {"kind": "chat", "session_id": "20261116_130000_abcdef", "source": "gui", "deliverable": True}
ECHO_ORIGIN = {"kind": "echo", "session_id": "api-0123456789abcdef", "source": "api_server", "deliverable": True}
LOCAL_ORIGIN = {"kind": "chat", "session_id": "local-1", "source": "oneshot", "deliverable": False}
TUI_ORIGIN = {"kind": "chat", "session_id": "local-2", "source": "local", "deliverable": True,
              "session_key": "agent:main:local:dm:local-2"}
URL = "http://192.0.2.230:8645/say/-Tok.wav"
PLUGIN = Path.home() / ".hermes" / "plugins" / "echo-job-notify" / "__init__.py"
MP4 = Path(_tmp.name) / "clip.mp4"
subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
                "testsrc=size=128x72:rate=30:duration=3", "-c:v", "libx264", "-pix_fmt", "yuv420p",
                "-movflags", "+faststart", str(MP4)], check=True)


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory(prefix="long-jobs-dir-")
        self.logs = []
        self.patches = [
            patch.object(long_jobs, "JOBS_DIR", Path(self.dir.name) / "long-jobs"),
            patch.object(bridge, "_log", lambda *a: self.logs.append(" ".join(map(str, a)))),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        self.dir.cleanup()

    def log(self, *a):
        self.logs.append(" ".join(map(str, a)))

    def file(self, name, data=b"x"):
        p = Path(self.dir.name) / name
        p.write_bytes(data)
        return str(p)


class Echo:
    """Records what would be said on the Echo instead of reaching the device."""
    def __init__(self):
        self.calls = []

    async def __call__(self, text, seconds=20):
        self.calls.append(text)
        return "said on the Echo"


class LongJobBase(Base):
    def setUp(self):
        super().setUp()
        self.echo = Echo()
        self.outbox = Path(self.dir.name) / "outbox"

        async def no_service(*a, **k):
            raise AssertionError("long-job code must speak through _speak_on_echo")
        for p in (patch.object(bridge, "_speak_on_echo", self.echo), patch.object(bridge, "call", no_service),
                  patch.object(bridge, "CHAT_OUTBOX", self.outbox)):
            p.start()
            self.patches.append(p)

    def origin(self, o):
        p = patch.object(bridge, "_job_origin", lambda name: dict(o))
        p.start()
        self.patches.append(p)

    def outbox_items(self):
        return [json.loads(f.read_text()) for f in sorted((self.outbox / "pending").glob("*.json"))]

    def send(self):
        asyncio.run(long_jobs.send_notices(self.log, bridge._long_job_notify))


class RegistryTests(Base):
    def test_new_finish_and_describe(self):
        rec = long_jobs.new_job("splat", "3D scene", ECHO_ORIGIN, 70)
        self.assertEqual(long_jobs.load(rec["job_id"])["status"], "running")
        self.assertIn("expected in about 70 min", long_jobs.describe(rec))
        png, mp4 = self.file("a.png"), str(MP4)
        done = long_jobs.finish(rec["job_id"], "A hut.", [png, mp4], echo_video=mp4)
        self.assertEqual((done["status"], done["media"], done["echo_video"]), ("done", [png, mp4], mp4))
        with self.assertRaisesRegex(RuntimeError, "already done"):
            long_jobs.fail(rec["job_id"], "late")

    def test_bad_inputs_fail_loudly(self):
        with self.assertRaisesRegex(ValueError, "origin must be"):
            long_jobs.new_job("x", "thing", {"kind": "radio"}, 5)
        with self.assertRaisesRegex(ValueError, "eta_min"):
            long_jobs.new_job("x", "thing", ECHO_ORIGIN, 0)
        rec = long_jobs.new_job("x", "thing", ECHO_ORIGIN, 5)
        with self.assertRaisesRegex(RuntimeError, "missing or not an absolute"):
            long_jobs.finish(rec["job_id"], media=["/nope/a.png"])
        with self.assertRaisesRegex(ValueError, "echo_video must be one of"):
            long_jobs.finish(rec["job_id"], media=[str(MP4)], echo_video="/other.mp4")
        with self.assertRaisesRegex(ValueError, "not a long-job id"):
            long_jobs.load("../etc")

    def test_cli_finish_and_fail(self):
        a = long_jobs.new_job("splat", "3D scene", CHAT_ORIGIN, 70)
        b = long_jobs.new_job("splat", "3D scene", CHAT_ORIGIN, 70)
        env = dict(os.environ, HOME=self.dir.name)
        jd = Path(self.dir.name) / ".hermes" / "long-jobs"
        jd.mkdir(parents=True)
        for r in (a, b):
            (jd / f"{r['job_id']}.json").write_text(json.dumps(r))
        here = Path(__file__).parent
        r1 = subprocess.run(["python3", str(here / "long_jobs.py"), "finish", a["job_id"], "--summary", "A hut.",
                             "--media", str(MP4), "--echo-video", str(MP4)], env=env, capture_output=True, text=True)
        self.assertEqual(r1.returncode, 0, r1.stderr)
        self.assertIn("is done", r1.stdout)
        r2 = subprocess.run(["python3", str(here / "long_jobs.py"), "fail", b["job_id"], "--error", "out of memory"],
                            env=env, capture_output=True, text=True)
        self.assertIn("FAILED: out of memory", r2.stdout)
        self.assertEqual(json.loads((jd / f"{a['job_id']}.json").read_text())["echo_video"], str(MP4))

    def test_notifier_lock_is_exclusive(self):
        import fcntl
        long_jobs.JOBS_DIR.mkdir(parents=True)
        fd = os.open(str(long_jobs.JOBS_DIR / ".notifier.lock"), os.O_RDWR | os.O_CREAT)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            with patch.object(long_jobs, "_notifier_lock_fd", None):
                self.assertFalse(long_jobs._notifier_lock_acquire())
        finally:
            os.close(fd)
        with patch.object(long_jobs, "_notifier_lock_fd", None):
            self.assertTrue(long_jobs._notifier_lock_acquire())
            os.close(long_jobs._notifier_lock_fd)


class AckTests(LongJobBase):
    def test_job_origin_classifies_sessions(self):
        with patch.object(bridge, "_pending_sessions", lambda name: [("api-b105027a515969b0", "api_server")]):
            self.assertEqual(bridge._job_origin("x"), {"kind": "echo", "session_id": "api-b105027a515969b0",
                                                       "source": "api_server", "deliverable": True})
        with patch.object(bridge, "_pending_sessions", lambda name: [("20261115_094539_4b7ffd", "gui")]):
            o = bridge._job_origin("x")
            self.assertEqual((o["kind"], o["deliverable"]), ("chat", True))
        with patch.object(bridge, "_pending_sessions", lambda name: [("local-abc", "local")]), \
                patch.object(bridge, "_stored_session_key", lambda sid: "agent:main:local:dm:" + sid):
            o = bridge._job_origin("x")
            self.assertEqual((o["deliverable"], o["session_key"]), (True, "agent:main:local:dm:local-abc"))
        with patch.object(bridge, "_pending_sessions", lambda name: [("local-abc", "oneshot")]):
            o = bridge._job_origin("x")
            self.assertEqual((o["kind"], o["deliverable"]), ("chat", False))
        with patch.object(bridge, "_pending_sessions", lambda name: []):
            with self.assertRaisesRegex(RuntimeError, "cannot tell whether this request came from"):
                bridge._job_origin("x")

    def test_acknowledge_echo_speaks_at_once(self):
        out = asyncio.run(bridge._acknowledge(ECHO_ORIGIN, "Your 3D scene", 70))
        self.assertEqual(self.echo.calls,
                         ["Working on it. Your 3D scene will be ready in about 70 minutes. I'll tell you when it's done."])
        self.assertIn('Reply with just "On it." and call no other tool.', out)

    def test_acknowledge_chat_and_undeliverable_chat(self):
        out = asyncio.run(bridge._acknowledge(CHAT_ORIGIN, "Your video", 2))
        self.assertIn("Working on it. Your video will be ready in about 2 minutes; I'll send it here", out)
        out = asyncio.run(bridge._acknowledge(LOCAL_ORIGIN, "Your video", 1))
        self.assertIn("cannot receive a later message", out)
        self.assertEqual(self.echo.calls, [])

    def test_generate_video_returns_at_once_with_a_recorded_job(self):
        followed = []

        def fake_media(method, path, body=None, *a):
            return {"job_id": "dell-job-1"}

        async def fake_follow(dell_kind, dell_job_id, job, on_echo):
            followed.append((dell_kind, dell_job_id, job["job_id"]))
        with patch.object(bridge, "_media", fake_media), patch.object(bridge, "_follow_media_job", fake_follow):
            self.origin(CHAT_ORIGIN)
            out = asyncio.run(bridge.generate_video("a cat surfing", image="text"))
            self.assertIn("Your video will be ready in about 3 minutes; I'll send it here", out)
        recs = long_jobs.all_records()
        self.assertEqual([(r["kind"], r["label"], r["origin"]["kind"]) for r in recs],
                         [("videos", "video", "chat")])
        self.assertEqual({f[0] for f in followed}, {"videos"})
        self.assertEqual(self.echo.calls, [])

    def test_generate_video_refuses_a_missing_image(self):
        self.origin(CHAT_ORIGIN)

        def no_media(*a, **k):
            raise AssertionError("Dell must not be called when image is missing")

        with patch.object(bridge, "_media", no_media):
            with self.assertRaises(TypeError):  # image has no default: it is a required argument
                asyncio.run(bridge.generate_video("a cat surfing"))
            with self.assertRaisesRegex(RuntimeError, r'image is required; pass image="text"'):
                asyncio.run(bridge.generate_video(prompt="a cat surfing", image=""))
            with self.assertRaisesRegex(RuntimeError, r'image is required; pass image="text"'):
                asyncio.run(bridge.generate_video(prompt="a cat surfing", image="  "))

    def test_generate_video_schema_requires_image(self):
        tools = asyncio.run(bridge.mcp.list_tools())
        gv = next(t for t in tools if t.name == "generate_video")
        self.assertIn("image", gv.inputSchema.get("required", []))
        self.assertIn("prompt", gv.inputSchema.get("required", []))
        self.assertNotIn("default", gv.inputSchema["properties"]["image"])
        self.assertTrue(gv.inputSchema["properties"]["image"]["description"].startswith(
            "Required, always. The photo you can see in the chat is NOT passed to this tool automatically"))

    def test_missing_image_names_the_attached_photo(self):
        self.origin(CHAT_ORIGIN)
        photo = Path(self.dir.name) / "upload_abc.png"

        def no_media(*a, **k):
            raise AssertionError("Dell must not be called when image is missing")

        with patch.object(bridge, "_media", no_media), \
                patch.object(bridge, "_turn_upload_paths", lambda *a, **k: [photo]):
            with self.assertRaisesRegex(RuntimeError, "this turn has an attached photo at " + re.escape(str(photo))):
                asyncio.run(bridge.generate_video(prompt="the dino waves", image=""))
        with patch.object(bridge, "_media", no_media), \
                patch.object(bridge, "_turn_upload_paths", lambda *a, **k: []):
            with self.assertRaisesRegex(RuntimeError, r'image is required; pass image="text"'):
                asyncio.run(bridge.generate_video(prompt="the dino waves", image=""))

    def test_none_image_only_for_extend_and_restyle(self):
        self.origin(CHAT_ORIGIN)

        def no_media(*a, **k):
            raise AssertionError("Dell must not be called")

        with patch.object(bridge, "_media", no_media):
            with self.assertRaisesRegex(RuntimeError, r'image="none" is only for mode=extend or mode=restyle'):
                asyncio.run(bridge.generate_video(prompt="x", image="none"))
            with self.assertRaisesRegex(RuntimeError, r'mode=extend does not use a first-frame image; pass image="none"'):
                asyncio.run(bridge.generate_video(prompt="x", image="", mode="extend", source="aa" * 16, extend_seconds=2))
            with self.assertRaisesRegex(RuntimeError, r'mode=restyle does not use a first-frame image; pass image="none"'):
                asyncio.run(bridge.generate_video(prompt="x", image="upload", mode="restyle", source="aa" * 16, control="depth"))

    def test_generate_video_text_is_prompt_only(self):
        bodies = []

        def fake_media(method, path, body=None, *a):
            bodies.append(body)
            return {"job_id": "dell-text"}

        async def fake_follow(dell_kind, dell_job_id, job, on_echo):
            pass

        self.origin(CHAT_ORIGIN)
        with patch.object(bridge, "_media", fake_media), patch.object(bridge, "_follow_media_job", fake_follow):
            out = asyncio.run(bridge.generate_video(prompt="a cat surfing", image="text"))
        self.assertEqual(len(bodies), 1)
        self.assertEqual(bodies[0], {"prompt": "a cat surfing"})
        self.assertNotIn("image_base64", bodies[0])
        self.assertNotIn("mode", bodies[0])
        self.assertNotIn("seconds", bodies[0])
        self.assertNotIn("style", bodies[0])
        self.assertIn("Your video will be ready in about 3 minutes; I'll send it here", out)

    def test_generate_video_upload_sends_the_image(self):
        photo = Path(self.dir.name) / "photo.jpg"
        Image.new("RGB", (16, 16), (200, 30, 30)).save(photo, format="JPEG")
        bodies = []

        def fake_media(method, path, body=None, *a):
            bodies.append(body)
            return {"job_id": "dell-i2v"}

        async def fake_follow(dell_kind, dell_job_id, job, on_echo):
            pass

        def resolve(image, tool_name="generate_video"):
            if image != "upload":
                raise AssertionError(f"expected image=upload, got {image!r}")
            return photo

        self.origin(CHAT_ORIGIN)
        with patch.object(bridge, "_media", fake_media), patch.object(bridge, "_follow_media_job", fake_follow),                 patch.object(bridge, "_resolve_video_image", resolve):
            out = asyncio.run(bridge.generate_video(prompt="the dinosaur waves, camera holds, soft rustle", image="upload"))
        self.assertEqual(len(bodies), 1)
        self.assertIn("image_base64", bodies[0])
        self.assertEqual(bodies[0]["prompt"], "the dinosaur waves, camera holds, soft rustle")
        self.assertIn("I'll send it here", out)

    def test_generate_video_text_refused_when_a_photo_is_attached(self):
        self.origin(CHAT_ORIGIN)

        def no_media(*a, **k):
            raise AssertionError("Dell must not be called for refused text-only")

        with patch.object(bridge, "_media", no_media),                 patch.object(bridge, "_turn_upload_paths", lambda *a, **k: [Path("/tmp/photo.jpg")]):
            with self.assertRaisesRegex(RuntimeError, "Text-only render refused"):
                asyncio.run(bridge.generate_video(prompt="a cat surfing", image="text"))

    def test_generate_video_signature_defaults(self):
        import inspect
        sig = inspect.signature(bridge.generate_video)
        self.assertIs(sig.parameters["prompt"].default, inspect.Parameter.empty)
        self.assertIs(sig.parameters["image"].default, inspect.Parameter.empty)
        self.assertIs(sig.parameters["seconds"].default, None)
        self.assertIs(sig.parameters["mode"].default, None)
        self.assertIs(sig.parameters["style"].default, None)
        self.assertIs(sig.parameters["source"].default, None)
        self.assertIs(sig.parameters["end_image"].default, None)
        self.assertIs(sig.parameters["extend_seconds"].default, None)
        self.assertIs(sig.parameters["control"].default, None)
        prompt_desc = sig.parameters["prompt"].annotation.__metadata__[0].description
        image_desc = sig.parameters["image"].annotation.__metadata__[0].description
        self.assertIn("no fixed instruction and no silent default", prompt_desc)
        self.assertIn("Nothing is filled in automatically", image_desc)
        self.assertNotIn("ignored", prompt_desc.lower())
        self.assertNotIn("ignored", image_desc.lower())
        self.assertFalse(hasattr(bridge, "_I2V_FIXED_PROMPT"))

    def test_generate_video_blank_prompt_is_refused(self):
        self.origin(CHAT_ORIGIN)

        def no_media(*a, **k):
            raise AssertionError("Dell must not be called when the prompt is blank")

        with patch.object(bridge, "_media", no_media):
            with self.assertRaisesRegex(RuntimeError, "prompt is required"):
                asyncio.run(bridge.generate_video(prompt="", image="text"))
            with self.assertRaisesRegex(RuntimeError, "prompt is required"):
                asyncio.run(bridge.generate_video(prompt="   ", image="text"))


    def test_generate_video_sends_only_valid_fields_for_each_mode(self):
        photo = Path(self.dir.name) / "photo.jpg"
        Image.new("RGB", (16, 16), (200, 30, 30)).save(photo, format="JPEG")
        end = Path(self.dir.name) / "end.jpg"
        Image.new("RGB", (16, 16), (30, 30, 200)).save(end, format="JPEG")
        bodies = []

        def fake_media(method, path, body=None, *a):
            bodies.append(body)
            return {"job_id": "dell-x"}

        async def fake_follow(*a, **k):
            pass

        def resolve(image, tool_name="generate_video"):
            return {"first": photo, "last": end, "upload": photo, "text": None}[image] if image in ("first", "last", "upload", "text") else photo

        self.origin(CHAT_ORIGIN)
        with patch.object(bridge, "_media", fake_media), patch.object(bridge, "_follow_media_job", fake_follow), \
                patch.object(bridge, "_resolve_video_image", resolve):
            asyncio.run(bridge.generate_video(prompt="a cat waves", image="text", seconds=7, style="anime"))
            asyncio.run(bridge.generate_video(prompt="the dino waves", image="upload"))
            asyncio.run(bridge.generate_video(prompt="walks from here to there", image="first",
                                              mode="first_last", end_image="last", seconds=4))
            asyncio.run(bridge.generate_video(prompt="keep going", image="none", mode="extend",
                                              source="aa" * 16, extend_seconds=3))
            asyncio.run(bridge.generate_video(prompt="as a drawing", image="none", mode="restyle",
                                              source="http://192.0.2.148:8091/media/" + "bb" * 16 + ".mp4",
                                              control="depth"))
        self.assertEqual(bodies[0], {"prompt": "a cat waves", "seconds": 7, "style": "anime"})
        self.assertIn("image_base64", bodies[1])
        self.assertNotIn("mode", bodies[1])
        self.assertNotIn("seconds", bodies[1])
        self.assertEqual(bodies[2]["mode"], "first_last")
        self.assertIn("image_base64", bodies[2])
        self.assertIn("end_image_base64", bodies[2])
        self.assertEqual(bodies[2]["seconds"], 4)
        self.assertEqual(bodies[3], {"prompt": "keep going", "mode": "extend",
                                     "source_video_id": "aa" * 16, "extend_seconds": 3})
        self.assertEqual(bodies[4], {"prompt": "as a drawing", "mode": "restyle",
                                     "source_video_id": "bb" * 16, "control": "depth"})

    def test_generate_video_rejects_fields_invalid_for_the_mode(self):
        def no_media(*a, **k):
            raise AssertionError("Dell must not be called")
        self.origin(CHAT_ORIGIN)
        with patch.object(bridge, "_media", no_media):
            with self.assertRaisesRegex(RuntimeError, "style is not valid for mode=extend"):
                asyncio.run(bridge.generate_video(prompt="x", image="none", mode="extend", source="aa"*16,
                                                  extend_seconds=2, style="anime"))
            with self.assertRaisesRegex(RuntimeError, "seconds is not valid for mode=restyle"):
                asyncio.run(bridge.generate_video(prompt="x", image="none", mode="restyle", source="aa"*16,
                                                  control="canny", seconds=5))
            with self.assertRaisesRegex(RuntimeError, "mode=t2v is text-only"):
                asyncio.run(bridge.generate_video(prompt="x", image="upload", mode="t2v"))
            with self.assertRaisesRegex(RuntimeError, "mode=i2v needs a first-frame image"):
                asyncio.run(bridge.generate_video(prompt="x", image="text", mode="i2v"))
            with self.assertRaisesRegex(RuntimeError, "source is only used for extend and restyle"):
                asyncio.run(bridge.generate_video(prompt="x", image="text", source="aa"*16))

    def test_generate_video_previous_uses_the_session_media_id(self):
        rec = long_jobs.new_job("videos", "video", CHAT_ORIGIN, 3, "Dell job x")
        long_jobs.finish(rec["job_id"], media=[], source_video_id="ab"*16)
        bodies = []

        def fake_media(method, path, body=None, *a):
            bodies.append(body)
            return {"job_id": "dell-ext"}

        async def fake_follow(*a, **k):
            pass

        self.origin(CHAT_ORIGIN)
        with patch.object(bridge, "_media", fake_media), patch.object(bridge, "_follow_media_job", fake_follow):
            asyncio.run(bridge.generate_video(prompt="longer", image="none", mode="extend", source="previous",
                                              extend_seconds=2))
        self.assertEqual(bodies[0]["source_video_id"], "ab"*16)

    def test_generate_video_attached_mp4_is_sent_as_base64_and_oversize_fails(self):
        mp4 = Path(self.dir.name) / "clip.mp4"
        # _mp4_bytes requires upload root or CACHE_GENERATED; patch that check via a real attach dir? mock _is_upload_path
        mp4.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 32)
        bodies = []

        def fake_media(method, path, body=None, *a):
            bodies.append(body)
            return {"job_id": "dell-b64"}

        async def fake_follow(*a, **k):
            pass

        self.origin(CHAT_ORIGIN)
        with patch.object(bridge, "_media", fake_media), patch.object(bridge, "_follow_media_job", fake_follow), \
                patch.object(bridge, "_is_upload_path", return_value=True), \
                patch.object(bridge, "_under_root", return_value=False):
            asyncio.run(bridge.generate_video(prompt="restyle this", image="none", mode="restyle",
                                              source=str(mp4), control="pose"))
        self.assertIn("video_base64", bodies[0])
        self.assertNotIn("source_video_id", bodies[0])
        import base64
        self.assertEqual(base64.b64decode(bodies[0]["video_base64"]), mp4.read_bytes())
        big = Path(self.dir.name) / "big.mp4"
        big.write_bytes(b"\x00\x00\x00\x18ftypmp42" + b"\x00" * (10 * 1024 * 1024))
        with patch.object(bridge, "_media", fake_media), \
                patch.object(bridge, "_is_upload_path", return_value=True):
            with self.assertRaisesRegex(RuntimeError, "larger than 10 MiB"):
                asyncio.run(bridge.generate_video(prompt="x", image="none", mode="extend", source=str(big),
                                                  extend_seconds=1))

    def test_generate_video_queue_position_is_in_the_acknowledgement(self):
        def fake_media(method, path, body=None, *a):
            return {"job_id": "dell-q", "queue_position": 2, "waiting_for": "llm_unload"}

        async def fake_follow(*a, **k):
            pass

        self.origin(CHAT_ORIGIN)
        with patch.object(bridge, "_media", fake_media), patch.object(bridge, "_follow_media_job", fake_follow):
            out = asyncio.run(bridge.generate_video(prompt="a cat", image="text"))
        self.assertIn("queue position 2", out)
        self.assertIn("waiting for llm_unload", out)

    def test_generate_video_429_tells_mo_the_queue_is_full(self):
        def full(method, path, body=None, *a):
            raise bridge.DellMediaError(429, "queue is full (max 10)")

        self.origin(CHAT_ORIGIN)
        with patch.object(bridge, "_media", full):
            with self.assertRaisesRegex(RuntimeError, r"video queue is full \(HTTP 429\)"):
                asyncio.run(bridge.generate_video(prompt="a cat", image="text"))
        self.assertEqual(long_jobs.all_records(), [])

    def test_wait_job_timeout_starts_when_rendering_starts(self):
        polls = iter([
            {"status": "queued", "queue_position": 2},
            {"status": "queued", "queue_position": 1, "waiting_for": "job-ahead"},
            {"status": "running"},
            {"status": "done", "url": "http://192.0.2.148:8091/media/" + "cd"*16 + ".mp4"},
        ])

        def fake_media(method, path, body=None, *a):
            return next(polls)

        ticks = iter([1000, 1005, 1010, 1015])

        with patch.object(bridge, "_media", fake_media):
            done = asyncio.run(bridge._wait_job("videos", "j1", timeout=30, poll_s=0, queue_cap_s=3600,
                                                nowfn=lambda: next(ticks)))
        self.assertEqual(done["status"], "done")

    def test_wait_job_fails_after_the_render_timeout(self):
        def always_running(method, path, body=None, *a):
            return {"status": "running"}

        times = iter([0, 0, 11])

        with patch.object(bridge, "_media", always_running):
            with self.assertRaisesRegex(RuntimeError, "still running after 10s"):
                asyncio.run(bridge._wait_job("videos", "j1", timeout=10, poll_s=0, queue_cap_s=1000,
                                             nowfn=lambda: next(times)))

    def test_follow_media_job_stores_the_gateway_video_id(self):
        rec = long_jobs.new_job("videos", "video", CHAT_ORIGIN, 3)
        url = "http://192.0.2.148:8091/media/" + "ee"*16 + ".mp4"
        saved = Path(self.dir.name) / "out.mp4"
        saved.write_bytes(b"x" * 32)

        async def wait(*a, **k):
            return {"status": "done", "url": url}

        async def no_echo(url):
            raise AssertionError("chat origin must not play on the Echo")

        with patch.object(bridge, "_wait_job", wait), patch.object(bridge, "_save_generated", lambda url: str(saved)):
            asyncio.run(bridge._follow_media_job("videos", "dell-1", rec, no_echo))
        done = long_jobs.load(rec["job_id"])
        self.assertEqual(done["status"], "done")
        self.assertEqual(done["source_video_id"], "ee"*16)


class NoticeTests(LongJobBase):
    def test_echo_done_notice_is_spoken_once_and_offers_the_video(self):
        rec = long_jobs.new_job("splat", "3D scene", ECHO_ORIGIN, 70)
        long_jobs.finish(rec["job_id"], "A hut.", [str(MP4)], echo_video=str(MP4))
        self.send()
        self.send()
        self.assertEqual(self.echo.calls, ["Your 3D scene is ready. Ask me to show it."])
        self.assertEqual(long_jobs.load(rec["job_id"])["notified"]["how"], "spoken on the Echo")

    def test_running_jobs_get_no_notice(self):
        long_jobs.new_job("splat", "3D scene", ECHO_ORIGIN, 70)
        self.send()
        self.assertEqual(self.echo.calls, [])

    def test_chat_notice_goes_to_the_outbox_with_every_file(self):
        rec = long_jobs.new_job("splat", "3D scene", CHAT_ORIGIN, 70)
        png = self.file("final.png")
        long_jobs.finish(rec["job_id"], "A hut.", [png, str(MP4)], echo_video=str(MP4))
        self.send()
        (item,) = self.outbox_items()
        self.assertEqual((item["route"], item["session_key"]), ("desktop", CHAT_ORIGIN["session_id"]))
        self.assertTrue(item["content"].startswith(bridge.NOTICE_PREFIX))
        self.assertIn(bridge.RELAY_VERBATIM, item["content"])
        self.assertNotIn("Tell Mo in one short sentence", item["content"])
        lines = item["content"].splitlines()
        self.assertIn(f"MEDIA:{png}", lines)
        self.assertIn(f"MEDIA:{MP4}", lines)
        self.assertIn("A hut.", item["content"])
        self.assertEqual(self.echo.calls, [])

    def test_tui_chat_notice_uses_the_gateway_route_and_session_key(self):
        rec = long_jobs.new_job("music", "song", TUI_ORIGIN, 1)
        long_jobs.finish(rec["job_id"], "", [str(MP4)])
        self.send()
        (item,) = self.outbox_items()
        self.assertEqual((item["route"], item["session_key"]), ("gateway", "agent:main:local:dm:local-2"))

    def test_failed_job_is_announced_on_each_route(self):
        a = long_jobs.new_job("splat", "3D scene", ECHO_ORIGIN, 70)
        long_jobs.fail(a["job_id"], "out of memory")
        b = long_jobs.new_job("splat", "3D scene", CHAT_ORIGIN, 70)
        long_jobs.fail(b["job_id"], "out of memory")
        self.send()
        self.assertEqual(self.echo.calls, ["Sorry, your 3D scene failed: out of memory"])
        (item,) = self.outbox_items()
        self.assertIn("out of memory", item["content"])
        self.assertIn(bridge.RELAY_VERBATIM, item["content"])
        self.assertNotIn("FAILED:", item["content"])
        self.assertNotIn("Tell Mo in one short sentence", item["content"])

    def test_undeliverable_origin_is_marked_with_the_reason(self):
        rec = long_jobs.new_job("splat", "3D scene", LOCAL_ORIGIN, 70)
        long_jobs.finish(rec["job_id"], "A hut.")
        self.send()
        self.assertEqual(self.outbox_items(), [])
        self.assertIn("not delivered", long_jobs.load(rec["job_id"])["notified"]["how"])

    def test_a_failed_send_is_retried_next_time(self):
        rec = long_jobs.new_job("splat", "3D scene", ECHO_ORIGIN, 70)
        long_jobs.finish(rec["job_id"], "A hut.")

        async def broken(*a, **k):
            raise RuntimeError("Echo offline")
        with patch.object(bridge, "_speak_on_echo", broken):
            self.send()
        self.assertIsNone(long_jobs.load(rec["job_id"])["notified"])
        self.assertTrue(any("will retry" in l for l in self.logs))
        self.send()
        self.assertEqual(len(self.echo.calls), 1)


class MediaFollowTests(LongJobBase):
    def follow(self, origin, wait, saved=None):
        played = []
        saved = saved or self.file("song.mp3")
        job = long_jobs.new_job("music", "song", origin, 1)

        async def on_echo(url):
            played.append(url)
            return "playing on the Echo speaker"
        with patch.object(bridge, "_wait_job", wait), patch.object(bridge, "_save_generated", lambda url: saved):
            asyncio.run(bridge._follow_media_job("music", "j1", job, on_echo))
        return long_jobs.load(job["job_id"]), played, saved

    async def ok(self, kind, job_id, timeout):
        return {"url": "http://dell/m.mp3"}

    async def boom(self, kind, job_id, timeout):
        raise RuntimeError("out of memory")

    def test_echo_origin_plays_when_done_and_records_it(self):
        rec, played, _ = self.follow(ECHO_ORIGIN, self.ok)
        self.assertEqual(played, ["http://dell/m.mp3"])
        self.assertEqual((rec["status"], rec["notified"]["how"]), ("done", "playing on the Echo speaker"))
        self.send()
        self.assertEqual(self.echo.calls, [])

    def test_chat_origin_file_is_delivered_by_the_notifier(self):
        rec, played, saved = self.follow(CHAT_ORIGIN, self.ok)
        self.assertEqual((rec["status"], rec["media"], rec["notified"]), ("done", [saved], None))
        self.send()
        (item,) = self.outbox_items()
        self.assertIn(f"MEDIA:{saved}", item["content"].splitlines())

    def test_failure_is_recorded_and_announced(self):
        rec, _, _ = self.follow(ECHO_ORIGIN, self.boom)
        self.assertEqual((rec["status"], rec["error"]), ("failed", "out of memory"))
        self.send()
        self.assertEqual(self.echo.calls, ["Sorry, your song failed: out of memory"])


class ToolTests(LongJobBase):
    def test_tools_registered_and_old_splat_tools_retired(self):
        names = {t.name for t in asyncio.run(bridge.mcp.list_tools())}
        self.assertLessEqual({"long_job_status", "show_job_result", "generate_video", "ask_synth", "create_splat",
                             "ask_vision"},
                             names)
        self.assertNotIn("generate_music", names)
        self.assertFalse({"splat_status", "play_splat_preview", "send_splat_files"} & names)

    def test_long_job_status_says_always_check(self):
        doc = bridge.long_job_status.__doc__ or ""
        self.assertIn("every time Mo asks about a job's progress", " ".join(doc.split()))
        self.assertIn("never answer that from memory", " ".join(doc.split()))

    def test_long_job_status(self):
        with self.assertRaisesRegex(RuntimeError, "no long jobs"):
            asyncio.run(bridge.long_job_status())
        rec = long_jobs.new_job("music", "song", ECHO_ORIGIN, 1)
        self.assertIn(f"song job {rec['job_id']}", asyncio.run(bridge.long_job_status()))
        self.assertIn("is running", asyncio.run(bridge.long_job_status("list")))

    def test_show_job_result_chat_returns_media_lines(self):
        rec = long_jobs.new_job("splat", "3D scene", CHAT_ORIGIN, 70)
        png = self.file("final.png")
        long_jobs.finish(rec["job_id"], "A hut.", [png, str(MP4)], echo_video=str(MP4))
        with patch.object(bridge, "_turn_origin", lambda name: "chat"):
            out = asyncio.run(bridge.show_job_result())
        self.assertIn(f"MEDIA:{png}", out.splitlines())
        self.assertIn(f"MEDIA:{MP4}", out.splitlines())

    def test_show_job_result_echo_plays_the_local_video_through_the_relay(self):
        rec = long_jobs.new_job("splat", "3D scene", ECHO_ORIGIN, 70)
        long_jobs.finish(rec["job_id"], "A hut.", [str(MP4)], echo_video=str(MP4))
        seen = {}

        def fake_start(source, **kw):
            seen.update(kw, source=source)
            return "http://192.0.2.230:8644/tok/index.m3u8"

        async def fake_play(url, title):
            seen["played"] = url
            return "playing on the Echo"
        with patch.object(bridge, "_turn_origin", lambda name: "echo"), \
                patch.object(video_relay, "start", fake_start), \
                patch.object(bridge, "_play_video_confirmed", fake_play), \
                patch.object(bridge, "_remember_last_video", lambda *a: seen.setdefault("remembered", a)):
            out = asyncio.run(bridge.show_job_result(rec["job_id"]))
        self.assertEqual((seen["source"], seen["kind"]), (str(MP4), "file"))
        self.assertEqual(seen["remembered"][1]["local_video"], str(MP4))
        self.assertIn("playing on the Echo", out)

    def test_show_job_result_refuses_vision_results_on_the_echo(self):
        rec = long_jobs.new_job("vision", "video annotation", CHAT_ORIGIN, 5)
        long_jobs.finish(rec["job_id"], "A person walks.\nViewer: http://h/v/t/", [str(MP4)], echo_video=str(MP4))
        with patch.object(bridge, "_turn_origin", lambda name: "echo"):
            with self.assertRaisesRegex(RuntimeError, "Vision results are not shown on the Echo"):
                asyncio.run(bridge.show_job_result(rec["job_id"]))

    def test_show_job_result_refuses_unfinished_or_videoless_jobs(self):
        rec = long_jobs.new_job("music", "song", ECHO_ORIGIN, 1)
        with patch.object(bridge, "_turn_origin", lambda name: "echo"):
            with self.assertRaisesRegex(RuntimeError, "is running"):
                asyncio.run(bridge.show_job_result(rec["job_id"]))
            long_jobs.finish(rec["job_id"], "", [self.file("s.mp3")])
            with self.assertRaisesRegex(RuntimeError, "has no video to show on the Echo"):
                asyncio.run(bridge.show_job_result(rec["job_id"]))


class FileRelayTests(unittest.TestCase):
    """kind="file" on the real relay: real ffmpeg remux of a local MP4 into a complete VOD playlist."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="relay-file-test-")
        tmpdir = Path(self.tmp.name)
        self.patches = [
            patch.object(video_relay, "_TMP_DIR", tmpdir),
            patch.object(video_relay, "_OWNER_FILE", tmpdir / "video-relay.owner.json"),
            patch.object(video_relay, "_START_LOCK_FILE", tmpdir / "video-relay.lock"),
        ]
        for p in self.patches:
            p.start()
        import socket
        s = socket.socket()
        s.bind((video_relay.RELAY_BIND, 0))
        self.port = s.getsockname()[1]
        s.close()

    def tearDown(self):
        video_relay.stop()
        for p in reversed(self.patches):
            p.stop()
        self.tmp.cleanup()

    def test_file_relay_serves_complete_vod_playlist_until_stopped(self):
        import urllib.request
        url = video_relay.start(str(MP4), kind="file", title="t", port=self.port)
        with urllib.request.urlopen(url, timeout=5) as r:
            text = r.read().decode()
        self.assertIn("#EXT-X-PLAYLIST-TYPE:VOD", text)
        self.assertIn("#EXT-X-ENDLIST", text)
        seg = [l for l in text.splitlines() if l.endswith(".ts")][0]
        with urllib.request.urlopen(url.rsplit("/", 1)[0] + "/" + seg, timeout=5) as r:
            self.assertEqual(r.read(1), b"\x47")
        time.sleep(1.5)
        self.assertEqual(video_relay.current_url(), url)
        video_relay.stop()
        self.assertIsNone(video_relay.current_url())

    def test_file_relay_refuses_missing_or_relative_file(self):
        for src in ("/nope/preview.mp4", "preview.mp4"):
            with self.assertRaisesRegex(RuntimeError, "relay file is missing"):
                video_relay.start(src, kind="file", port=self.port)


class SpeechTests(Base):
    """The Echo's notify speak option goes to Home Assistant, so speech is synthesized (Wyoming) and played."""

    def fake_wyoming(self, rate=22050, channels=1, seconds=0.5):
        import socket
        import threading
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        got = {}

        def serve():
            conn, _ = srv.accept()
            f = conn.makefile("rb")
            h = json.loads(f.readline())
            got["type"], got["data"] = h["type"], json.loads(f.read(h["data_length"]))
            pcm = b"\0\0" * int(rate * channels * seconds)

            def ev(t, data, payload=b""):
                d = json.dumps(data).encode()
                conn.sendall(json.dumps({"type": t, "data_length": len(d), "payload_length": len(payload)}).encode()
                             + b"\n" + d + payload)
            ev("audio-start", {"rate": rate, "width": 2, "channels": channels})
            ev("audio-chunk", {"rate": rate, "width": 2, "channels": channels}, pcm)
            ev("audio-stop", {})
            conn.close()
            srv.close()
        threading.Thread(target=serve, daemon=True).start()
        return f"127.0.0.1:{srv.getsockname()[1]}", got

    def test_tts_returns_48k_stereo_wav_in_the_echo_voice(self):
        addr, got = self.fake_wyoming()
        with patch.dict("os.environ", {"ECHO_TTS": addr, "ECHO_TTS_VOICE": "en_US-glados-medium"}):
            wav, secs = bridge._wyoming_tts("Working on it.")
        self.assertEqual(got, {"type": "synthesize",
                               "data": {"text": "Working on it.", "voice": {"name": "en_US-glados-medium"}}})
        import io
        import wave
        with wave.open(io.BytesIO(wav)) as w:
            self.assertEqual((w.getframerate(), w.getnchannels(), w.getsampwidth()), (48000, 2, 2))
        self.assertAlmostEqual(secs, 0.5, places=2)

    def test_tts_without_config_fails_loudly(self):
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("ECHO_TTS", None)
            with self.assertRaises(KeyError):
                bridge._wyoming_tts("x")

    def run_speak(self, echo_log_line):
        calls = []

        async def fake_call(service, *a, **kw):
            calls.append((service, kw))
            return "ok"

        async def fake_play(url):
            calls.append(("play", url))
            return "playing on the Echo speaker (speaker=PLAYING)"

        def fake_ssh(remote, timeout=12):
            self.assertIn("-Tok.wav", remote)
            return echo_log_line

        @bridge.asynccontextmanager
        async def fake_wav(wav):
            yield URL
        with patch.object(bridge, "call", fake_call), patch.object(bridge, "_play_audio_confirmed", fake_play), \
                patch.object(bridge, "_echo_ssh", fake_ssh), patch.object(bridge, "_lan_wav", fake_wav), \
                patch.object(bridge, "_wyoming_tts", lambda text: (b"RIFF", 0.0)):
            out = asyncio.run(bridge._speak_on_echo("Working on it.", 8))
        return calls, out

    def test_speak_shows_a_silent_card_plays_the_speech_and_reads_the_echo_log(self):
        calls, out = self.run_speak(f"I [1.0] playing media url={URL} live=false\n"
                                    f"I [2.0] media finished item={URL} by itself=true\n"
                                    "E [3.0] playing media failed err=\"some later item\"\n")
        self.assertEqual(calls[0][0], "notify")
        self.assertEqual((calls[0][1]["speak"], calls[0][1]["chime"]), ("off", "off"))
        self.assertEqual(calls[1], ("play", "http://192.0.2.230:8645/say/-Tok.wav"))
        self.assertIn("said on the Echo", out)

    def test_speak_fails_when_the_echo_refused_the_audio(self):
        with self.assertRaisesRegex(RuntimeError, "(?s)did not play the speech.*cannot play"):
            self.run_speak(f"I [1.0] playing media url={URL} live=false\n"
                           "E [1.0] playing media failed err=\"cannot play 22050 Hz 1 channel 16 bit audio\"\n"
                           f"I [1.0] media finished item={URL} by itself=true\n")
        with self.assertRaisesRegex(RuntimeError, "did not play the speech"):
            self.run_speak("")


class PluginDrainTests(Base):
    def setUp(self):
        super().setUp()
        spec = importlib.util.spec_from_file_location("echo_job_notify", PLUGIN)
        self.plugin = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.plugin)
        p = patch.object(self.plugin, "OUTBOX", Path(self.dir.name) / "outbox")
        p.start()
        self.patches.append(p)
        self.dirs = self.plugin._dirs()

    def put(self, name, key, created_at=None, route="desktop"):
        (self.dirs["pending"] / name).write_text(json.dumps(
            {"route": route, "session_key": key, "content": "hello", "created_at": created_at or time.time()}))

    def test_drain_delivers_retries_and_expires(self):
        self.put("1.json", "live")
        self.put("2.json", "gone")
        self.put("3.json", "gone", created_at=time.time() - 25 * 3600)
        calls = []

        class Ctx:
            def inject_message(self, content, session_key=None):
                calls.append((content, session_key))
                return session_key == "live"
        self.plugin.drain_once(Ctx(), self.dirs, set())
        names = lambda d: sorted(f.name for f in self.dirs[d].glob("*.json"))
        self.assertEqual(names("delivered"), ["1.json"])
        self.assertEqual(names("pending"), ["2.json"])
        self.assertEqual(names("failed"), ["3.json"])
        self.assertEqual(names("claimed"), [])
        self.assertEqual(calls[0], ("hello", "live"))

    def test_routes_per_process(self):
        class M:
            _cli_ref = None
            has_tui_message_injector = True
            has_gateway_message_injector = False
        self.assertEqual(self.plugin._routes(M), {"desktop"})
        M.has_gateway_message_injector = True
        self.assertEqual(self.plugin._routes(M), {"gateway"})
        M.has_tui_message_injector = False
        self.assertEqual(self.plugin._routes(M), {"gateway"})
        M._cli_ref = object()
        self.assertEqual(self.plugin._routes(M), set())

    def test_drain_skips_routes_this_process_cannot_serve(self):
        self.put("1.json", "agent:main:local:dm:x", route="gateway")
        self.put("2.json", "20261116_x", route="desktop")
        calls = []

        class Ctx:
            def inject_message(self, content, session_key=None):
                calls.append(session_key)
                return True
        self.plugin.drain_once(Ctx(), self.dirs, set(), {"desktop"})
        self.assertEqual(calls, ["20261116_x"])
        self.assertEqual(sorted(f.name for f in self.dirs["pending"].glob("*.json")), ["1.json"])



class FakeBotRuns:
    """A stand-in for a Hermes bot's /v1/runs: records posted inputs and stops; GET answers from self.status/output."""
    def __init__(self):
        import http.server, threading
        self.posted, self.stopped, self.auth = [], [], []
        self.status, self.output, self.error, self.refuse = "running", "", "", None
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, obj):
                b = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"] or 0)) or b"{}")
                outer.auth.append(self.headers["Authorization"])
                if self.path.endswith("/stop"):
                    outer.stopped.append(self.path.split("/")[-2])
                    outer.status = "cancelled"
                    return self._send(200, {"status": "stopping"})
                if outer.refuse:
                    return self._send(503, {"error": outer.refuse})
                outer.posted.append(body)
                self._send(202, {"run_id": f"run_{len(outer.posted)}", "status": "started"})

            def do_GET(self):
                self._send(200, {"status": outer.status, "output": outer.output, "error": outer.error})
        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}/p/splat/v1/runs"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()


class CreateSplatTests(LongJobBase):
    """create_splat is a thin handoff: a long job, the 3D bot's run with Mo's words, the relay unit. Nothing else."""
    def setUp(self):
        super().setUp()
        self.runs = FakeBotRuns()
        self.key = Path(self.dir.name) / "splat.env"
        self.key.write_text("DELL_LLM_API_KEY=x\nAPI_SERVER_KEY=splat-key-1\n")
        self.relays = []
        for p in (patch.object(bridge, "SPLAT_RUNS_URL", self.runs.url), patch.object(bridge, "SPLAT_KEY_FILE", self.key),
                  patch.object(bridge, "_start_handoff_relay", lambda jid, rid: self.relays.append((jid, rid)))):
            p.start()
            self.patches.append(p)

    def tearDown(self):
        self.runs.close()
        super().tearDown()

    def test_chat_request_is_handed_over_word_for_word_and_acknowledged(self):
        self.origin(CHAT_ORIGIN)
        out = asyncio.run(bridge.create_splat("make the hut on my last island bigger"))
        rec = long_jobs.all_records()[0]
        self.assertEqual((rec["kind"], rec["label"], rec["status"], rec["eta_min"]), ("splat", "3D scene", "running", 10))
        self.assertEqual(rec["origin"], CHAT_ORIGIN)
        self.assertEqual(self.runs.posted, [{"input": "make the hut on my last island bigger"}])
        self.assertEqual(self.runs.auth, ["Bearer splat-key-1"])
        self.assertEqual(self.relays, [(rec["job_id"], "run_1")])
        self.assertIn(f"3D scene job {rec['job_id']} handed to the 3D bot.", out)
        self.assertIn("Your 3D scene will be ready in about 10 minutes; I'll send it here", out)
        self.assertEqual(self.echo.calls, [])

    def test_echo_request_speaks_the_acknowledgement(self):
        self.origin(ECHO_ORIGIN)
        out = asyncio.run(bridge.create_splat("a lighthouse"))
        self.assertEqual(self.echo.calls, ["Working on it. Your 3D scene will be ready in about 10 minutes. "
                                           "I'll tell you when it's done."])
        self.assertIn('"On it."', out)

    def test_a_refused_handoff_fails_the_job_loudly(self):
        self.origin(CHAT_ORIGIN)
        self.runs.refuse = "busy"
        with self.assertRaisesRegex(RuntimeError, "could not hand the request to the 3D bot: HTTP 503"):
            asyncio.run(bridge.create_splat("a boat"))
        rec = long_jobs.all_records()[0]
        self.assertEqual(rec["status"], "failed")
        self.assertIn("HTTP 503", rec["error"])
        self.assertEqual(self.relays, [])

    def test_a_missing_key_fails_loudly(self):
        self.origin(CHAT_ORIGIN)
        self.key.write_text("OTHER=1\n")
        with self.assertRaisesRegex(RuntimeError, "API_SERVER_KEY is not set"):
            asyncio.run(bridge.create_splat("a boat"))
        self.assertEqual(long_jobs.all_records()[0]["status"], "failed")

    def test_empty_request_is_refused(self):
        self.origin(CHAT_ORIGIN)
        with self.assertRaisesRegex(ValueError, "request is required"):
            asyncio.run(bridge.create_splat("  "))
        self.assertEqual(long_jobs.all_records(), [])

    def test_no_scene_logic_left_in_glados(self):
        for name in ("_splat_service", "_splat_base_job", "SPLAT_SOCK"):
            self.assertFalse(hasattr(bridge, name), name)
        import inspect
        self.assertEqual(list(inspect.signature(bridge.create_splat).parameters), ["request"])



class AskVisionTests(LongJobBase):
    """ask_vision is a thin handoff: a long job, Vision's run with Mo's words, the relay unit. Not on the Echo."""
    def setUp(self):
        super().setUp()
        self.runs = FakeBotRuns()
        self.key = Path(self.dir.name) / "vision.env"
        self.key.write_text("DELL_LLM_API_KEY=x\nAPI_SERVER_KEY=vision-key-1\n")
        self.relays = []
        for p in (patch.object(bridge, "VISION_RUNS_URL", self.runs.url), patch.object(bridge, "VISION_KEY_FILE", self.key),
                  patch.object(bridge, "_start_vision_handoff_relay", lambda jid, rid: self.relays.append((jid, rid)))):
            p.start()
            self.patches.append(p)

    def tearDown(self):
        self.runs.close()
        super().tearDown()

    def test_chat_request_is_handed_over_word_for_word_and_acknowledged(self):
        self.origin(CHAT_ORIGIN)
        req = "Describe this video clip: /home/mohammed/scratch/vision-test/test2s.mp4"
        out = asyncio.run(bridge.ask_vision(req))
        rec = long_jobs.all_records()[0]
        self.assertEqual((rec["kind"], rec["label"], rec["status"], rec["eta_min"]),
                         ("vision", "video annotation", "running", 5))
        self.assertEqual(rec["origin"], CHAT_ORIGIN)
        self.assertEqual(self.runs.posted, [{"input": req}])
        self.assertEqual(self.runs.auth, ["Bearer vision-key-1"])
        self.assertEqual(self.relays, [(rec["job_id"], "run_1")])
        self.assertIn(f"video annotation job {rec['job_id']} handed to Vision.", out)
        self.assertIn("Your video annotation will be ready in about 5 minutes; I'll send it here", out)
        self.assertEqual(self.echo.calls, [])

    def test_echo_request_is_refused_and_does_not_start_a_job(self):
        self.origin(ECHO_ORIGIN)
        with self.assertRaisesRegex(RuntimeError, "Vision is not connected to the Echo"):
            asyncio.run(bridge.ask_vision("describe this clip: /tmp/a.mp4"))
        self.assertEqual(long_jobs.all_records(), [])
        self.assertEqual(self.runs.posted, [])
        self.assertEqual(self.relays, [])
        self.assertEqual(self.echo.calls, [])

    def test_a_refused_handoff_fails_the_job_loudly(self):
        self.origin(CHAT_ORIGIN)
        self.runs.refuse = "busy"
        with self.assertRaisesRegex(RuntimeError, "could not hand the request to Vision: HTTP 503"):
            asyncio.run(bridge.ask_vision("describe /tmp/a.mp4"))
        rec = long_jobs.all_records()[0]
        self.assertEqual(rec["status"], "failed")
        self.assertIn("HTTP 503", rec["error"])
        self.assertEqual(self.relays, [])

    def test_a_missing_key_fails_loudly(self):
        self.origin(CHAT_ORIGIN)
        self.key.write_text("OTHER=1\n")
        with self.assertRaisesRegex(RuntimeError, "API_SERVER_KEY is not set"):
            asyncio.run(bridge.ask_vision("describe /tmp/a.mp4"))
        self.assertEqual(long_jobs.all_records()[0]["status"], "failed")

    def test_empty_request_is_refused(self):
        self.origin(CHAT_ORIGIN)
        with self.assertRaisesRegex(ValueError, "request is required"):
            asyncio.run(bridge.ask_vision("  "))
        self.assertEqual(long_jobs.all_records(), [])

    def test_signature_is_only_request(self):
        import inspect
        self.assertEqual(list(inspect.signature(bridge.ask_vision).parameters), ["request"])


class AskSynthTests(LongJobBase):
    """ask_synth is a chat-only handoff to Synth, same shape as ask_vision, with one
    documented deviation: an Echo turn is spoken (songs are in the chat with Synth now)
    and no job is started, rather than only raising RuntimeError."""

    def setUp(self):
        super().setUp()
        self.runs = FakeBotRuns()
        self.key = Path(self.dir.name) / "synth.env"
        self.key.write_text("DELL_LLM_API_KEY=x\nAPI_SERVER_KEY=synth-key-1\n")
        self.relays = []
        for p in (patch.object(bridge, "SYNTH_RUNS_URL", self.runs.url),
                  patch.object(bridge, "SYNTH_KEY_FILE", self.key),
                  patch.object(bridge, "_start_synth_handoff_relay", lambda jid, rid: self.relays.append((jid, rid)))):
            p.start()
            self.patches.append(p)

    def tearDown(self):
        self.runs.close()
        super().tearDown()

    def test_chat_request_is_handed_over_word_for_word_and_acknowledged(self):
        self.origin(CHAT_ORIGIN)
        req = "a lofi song about rain, plus /tmp/clip.wav"
        out = asyncio.run(bridge.ask_synth(req))
        rec = long_jobs.all_records()[0]
        self.assertEqual((rec["kind"], rec["label"], rec["status"], rec["eta_min"]),
                         ("synth", "audio", "running", 5))
        self.assertEqual(rec["origin"], CHAT_ORIGIN)
        self.assertEqual(self.runs.posted, [{"input": req}])
        self.assertEqual(self.runs.auth, ["Bearer synth-key-1"])
        self.assertEqual(self.relays, [(rec["job_id"], "run_1")])
        self.assertIn(f"audio job {rec['job_id']} handed to Synth.", out)
        self.assertIn("Your audio will be ready in about 5 minutes; I'll send it here", out)
        self.assertEqual(self.echo.calls, [])

    def test_echo_request_is_spoken_and_does_not_start_a_job(self):
        self.origin(ECHO_ORIGIN)
        out = asyncio.run(bridge.ask_synth("a lofi song about rain"))
        self.assertEqual(self.echo.calls, ["Songs are in the chat with Synth now."])
        self.assertIn("Songs are in the chat with Synth now.", out)
        self.assertIn('"On it."', out)
        self.assertEqual(long_jobs.all_records(), [])
        self.assertEqual(self.runs.posted, [])
        self.assertEqual(self.relays, [])

    def test_a_refused_handoff_fails_the_job_loudly(self):
        self.origin(CHAT_ORIGIN)
        self.runs.refuse = "busy"
        with self.assertRaisesRegex(RuntimeError, "could not hand the request to Synth: HTTP 503"):
            asyncio.run(bridge.ask_synth("a song"))
        rec = long_jobs.all_records()[0]
        self.assertEqual(rec["status"], "failed")
        self.assertIn("HTTP 503", rec["error"])
        self.assertEqual(self.relays, [])

    def test_a_missing_key_fails_loudly(self):
        self.origin(CHAT_ORIGIN)
        self.key.write_text("OTHER=1\n")
        with self.assertRaisesRegex(RuntimeError, "API_SERVER_KEY is not set"):
            asyncio.run(bridge.ask_synth("a song"))
        self.assertEqual(long_jobs.all_records()[0]["status"], "failed")

    def test_empty_request_is_refused(self):
        self.origin(CHAT_ORIGIN)
        with self.assertRaisesRegex(ValueError, "request is required"):
            asyncio.run(bridge.ask_synth("  "))
        self.assertEqual(long_jobs.all_records(), [])
        self.assertEqual(self.runs.posted, [])

    def test_signature_is_only_request(self):
        import inspect
        self.assertEqual(list(inspect.signature(bridge.ask_synth).parameters), ["request"])

    def test_soul_directs_song_requests_to_ask_synth(self):
        text = Path("/home/mohammed/.hermes/SOUL.md").read_text()
        self.assertIn("call ask_synth", text)
        self.assertIn("Do not call generate_music (it has been removed)", text)
        self.assertNotIn("To compose or play generated music, call generate_music", text)
        self.assertIn("Long jobs (generate_video, create_splat, ask_vision, ask_synth)", text)
        self.assertIn("a video, song, speech, 3D scene or video-clip annotation", text)


class HandoffRelayTests(LongJobBase):
    """hermes_handoff.follow relays the 3D bot's reply into the long job unchanged."""
    def setUp(self):
        super().setUp()
        import hermes_handoff
        self.h = hermes_handoff
        self.runs = FakeBotRuns()
        self.key = Path(self.dir.name) / "splat.env"
        self.key.write_text("API_SERVER_KEY=splat-key-1\n")
        p = patch.object(hermes_handoff, "POLL_S", 0.05)
        p.start()
        self.patches.append(p)
        self.job = long_jobs.new_job("splat", "3D scene", CHAT_ORIGIN, 10)

    def tearDown(self):
        self.runs.close()
        super().tearDown()

    def follow(self):
        return self.h.follow(self.job["job_id"], "run_1", self.runs.url, self.key, log=lambda *a: None)

    def test_a_finished_scene_is_relayed_with_its_files(self):
        png, mp4, ply = self.file("final.png"), str(MP4), self.file("scene.ply")
        self.runs.status = "completed"
        self.runs.output = f"Hut: A hut. (900 splats, scene 0123456789abcdef)\nViewer: http://v/x/\nMEDIA:{png}\nMEDIA:{mp4}\nMEDIA:{ply}"
        rec = self.follow()
        self.assertEqual((rec["status"], rec["summary"]), ("done", "Hut: A hut. (900 splats, scene 0123456789abcdef)\nViewer: http://v/x/"))
        self.assertEqual((rec["media"], rec["echo_video"]), ([png, mp4, ply], mp4))
        self.send()
        item = self.outbox_items()[0]
        self.assertIn("Viewer: http://v/x/", item["content"])
        self.assertIn(f"MEDIA:{mp4}", item["content"])

    def test_a_completed_reply_without_files_is_relayed_as_written(self):
        self.runs.status = "completed"
        self.runs.output = "Another 3D scene is being made (scene 0123456789abcdef); one at a time."
        rec = self.follow()
        self.assertEqual((rec["status"], rec["summary"]), ("done", self.runs.output))
        self.send()
        item = self.outbox_items()[0]
        self.assertIn(self.runs.output, item["content"])
        self.assertIn(bridge.RELAY_VERBATIM, item["content"])
        self.assertNotIn("Tell Mo in one short sentence", item["content"])
        self.assertNotIn("FAILED:", item["content"])

    def test_a_failed_run_with_a_reply_and_files_keeps_both(self):
        png = self.file("final.png")
        self.runs.status = "failed"
        self.runs.output = (
            f"Depth failed: memory watchdog.\nWant me to retry?\nViewer: http://v/x/\nMEDIA:{png}"
        )
        rec = self.follow()
        self.assertEqual(rec["status"], "failed")
        self.assertEqual(rec["error"], "Depth failed: memory watchdog.\nWant me to retry?\nViewer: http://v/x/")
        self.assertEqual(rec["media"], [png])
        self.send()
        item = self.outbox_items()[0]
        self.assertIn("Depth failed: memory watchdog.", item["content"])
        self.assertIn("Want me to retry?", item["content"])
        self.assertIn("Viewer: http://v/x/", item["content"])
        self.assertIn(f"MEDIA:{png}", item["content"].splitlines())
        self.assertIn(bridge.RELAY_VERBATIM, item["content"])
        self.assertNotIn("Tell Mo in one short sentence", item["content"])
        self.assertNotIn("FAILED:", item["content"])

    def test_a_completed_vision_reply_with_viewer_is_done_even_without_media_files(self):
        self.runs.status = "completed"
        self.runs.output = "A person and a parking lot.\nViewer: http://192.0.2.230:8647/v/abc/"
        rec = self.follow()
        self.assertEqual((rec["status"], rec["summary"], rec["media"]), ("done", self.runs.output, []))
        self.send()
        item = self.outbox_items()[0]
        self.assertIn("A person and a parking lot.", item["content"])
        self.assertIn("Viewer: http://192.0.2.230:8647/v/abc/", item["content"])
        self.assertIn(bridge.RELAY_VERBATIM, item["content"])
        self.assertNotIn("Tell Mo in one short sentence", item["content"])

    def test_a_failed_run_without_reply_says_so(self):
        self.runs.status, self.runs.error = "failed", "model said no"
        self.assertEqual(self.follow()["error"], "the bot's run ended (failed) without a reply: model said no")

    def test_missing_files_fail_loudly(self):
        self.runs.status = "completed"
        self.runs.output = "Hut.\nMEDIA:/nope/final.png"
        self.assertIn("names files that do not exist: /nope/final.png", self.follow()["error"])

    def test_cancelling_the_long_job_stops_the_bots_run(self):
        long_jobs.fail(self.job["job_id"], "Mo cancelled it")
        self.assertEqual(self.follow()["status"], "failed")
        self.assertEqual(self.runs.stopped, ["run_1"])

    def test_an_unreachable_gateway_fails_after_a_minute_of_polls(self):
        self.runs.close()
        rec = self.follow()
        self.assertEqual(rec["status"], "failed")
        self.assertIn("lost track of the bot's run run_1", rec["error"])
        self.runs = FakeBotRuns()


class ChatNoticeViewerTests(unittest.TestCase):
    def test_viewer_and_media_are_in_the_verbatim_reply(self):
        text = bridge._chat_notice("Mo's 3D scene (job x) is ready. Title: a box.\nViewer: http://h/v/t/", ["/a.png"])
        self.assertTrue(text.startswith(bridge.NOTICE_PREFIX + " " + bridge.RELAY_VERBATIM))
        self.assertIn("\nViewer: http://h/v/t/\nMEDIA:/a.png", text)
        self.assertEqual(text.count("Viewer: http://h/v/t/"), 1)
        self.assertNotIn("Tell Mo in one short sentence", text)

    def test_notice_without_media_still_asks_for_a_verbatim_relay(self):
        text = bridge._chat_notice("Want me to retry the failed stages, or finish?")
        self.assertTrue(text.startswith(bridge.NOTICE_PREFIX + " " + bridge.RELAY_VERBATIM))
        self.assertTrue(text.endswith("Want me to retry the failed stages, or finish?"))
        self.assertNotIn("Tell Mo in one short sentence", text)

    def test_existing_media_lines_in_the_body_are_not_duplicated(self):
        text = bridge._chat_notice("Done.\nMEDIA:/a.png", ["/a.png"])
        self.assertEqual(text.count("MEDIA:/a.png"), 1)


class NotifierOwnerTests(unittest.TestCase):
    """Only the hermes-gateway echo MCP may start the long-job notifier."""

    GATEWAY_CGROUP = "0::/user.slice/user-1002.slice/user@1002.service/app.slice/hermes-gateway.service\n"
    DASHBOARD_CGROUP = "0::/user.slice/user-1002.slice/user@1002.service/app.slice/hermes-dashboard.service\n"
    OTHER_CGROUP = "0::/user.slice/user-1002.slice/user@1002.service/app.slice/other.service\n"
    GATEWAY_CMD = b"python3\0-I\0-c\0from hermes_cli.main import main  # dashboard mention in script\n\0gateway\0run\0"
    DASHBOARD_CMD = b"python3\0-I\0-c\0from hermes_cli.main import main\n\0dashboard\0--host\00.0.0.0\0--port\09119\0--no-open\0"
    UV_CMD = b"/home/mohammed/.hermes/tools/uv/uv\0run\0-q\0--script\0/home/mohammed/Code/echo-hermes-bridge/bridge.py\0"
    BRIDGE_CMD = b"/home/mohammed/.cache/uv/environments-v2/bridge/bin/python\0/home/mohammed/Code/echo-hermes-bridge/bridge.py\0"

    def test_markers_prefer_the_systemd_cgroup(self):
        self.assertTrue(long_jobs.process_is_hermes_gateway(self.GATEWAY_CGROUP, self.DASHBOARD_CMD))
        self.assertFalse(long_jobs.process_is_hermes_gateway(self.DASHBOARD_CGROUP, self.GATEWAY_CMD))

    def test_markers_use_argv_after_python_c_not_the_bootstrap_script(self):
        self.assertTrue(long_jobs.process_is_hermes_gateway(self.OTHER_CGROUP, self.GATEWAY_CMD))
        self.assertFalse(long_jobs.process_is_hermes_gateway(self.OTHER_CGROUP, self.DASHBOARD_CMD))
        self.assertIsNone(long_jobs.process_is_hermes_gateway(self.OTHER_CGROUP, self.UV_CMD))

    def test_walk_finds_the_gateway_parent(self):
        procs = {
            10: (self.OTHER_CGROUP, self.BRIDGE_CMD, 20),
            20: (self.OTHER_CGROUP, self.UV_CMD, 30),
            30: (self.GATEWAY_CGROUP, self.GATEWAY_CMD, 1),
        }
        self.assertTrue(long_jobs.launched_by_hermes_gateway(10, _read=procs.__getitem__))

    def test_walk_rejects_a_dashboard_parent(self):
        procs = {
            10: (self.DASHBOARD_CGROUP, self.BRIDGE_CMD, 20),
            20: (self.DASHBOARD_CGROUP, self.UV_CMD, 30),
            30: (self.DASHBOARD_CGROUP, self.DASHBOARD_CMD, 1),
        }
        self.assertFalse(long_jobs.launched_by_hermes_gateway(10, _read=procs.__getitem__))

    def test_walk_defaults_to_false_when_the_parent_is_unknown(self):
        procs = {
            10: (self.OTHER_CGROUP, self.BRIDGE_CMD, 20),
            20: (self.OTHER_CGROUP, self.UV_CMD, 1),
        }
        self.assertFalse(long_jobs.launched_by_hermes_gateway(10, _read=procs.__getitem__))

    def test_lifespan_starts_the_notifier_only_for_the_gateway(self):
        async def ran(should):
            with patch.object(long_jobs, "launched_by_hermes_gateway", return_value=should), \
                    patch.object(long_jobs, "ensure_notifier") as ens, \
                    patch.object(bridge, "_log"):
                async with bridge._echo_lifespan(None):
                    pass
            return ens.called
        self.assertTrue(asyncio.run(ran(True)))
        self.assertFalse(asyncio.run(ran(False)))





class VideoRelayFileOnlyTests(unittest.TestCase):
    def test_network_kinds_are_refused(self):
        for kind in ("live", "vod", ""):
            with self.assertRaisesRegex(RuntimeError, "local files only"):
                video_relay.start("https://example.invalid/x.m3u8", kind=kind)

    def test_no_twitch_left(self):
        import bridge as _b
        self.assertFalse(hasattr(_b, "play_twitch"))
        self.assertFalse([n for n in dir(_b) if "twitch" in n.lower()])
        self.assertFalse(hasattr(video_relay, "_ffmpeg_cmd"))

if __name__ == "__main__":
    unittest.main()
