"""Unit tests for glados_vision. Mocks only: no Echo camera, screen, or speaker."""
from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

# Importing bridge.py reads env paths at module load. Use dummy files, never the real keys.
_tmp = tempfile.TemporaryDirectory(prefix="glados-vision-test-")
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

# Never touch the live watchdog lock under ~/.hermes from tests.
bridge._VISION_WATCHDOG_LOCK_PATH = Path(_tmp.name) / "echo-vision-watchdog.lock"


def run(coro):
    return asyncio.run(coro)


class ParseClassesTests(unittest.TestCase):
    def test_default_and_trim(self):
        self.assertEqual(bridge._parse_vision_classes(None), [])
        self.assertEqual(bridge._parse_vision_classes(""), [])
        self.assertEqual(bridge._parse_vision_classes("   "), [])
        self.assertEqual(bridge._parse_vision_classes(" person, cup ,phone "), ["person", "cup", "cell phone"])
        self.assertEqual(bridge._parse_vision_classes("cup"), ["cup"])
        self.assertEqual(bridge._parse_vision_classes("cell phone"), ["cell phone"])
        self.assertEqual(bridge._parse_vision_classes("Phone"), ["cell phone"])
        self.assertEqual(bridge._parse_vision_classes("O'Brien, left-arm, cup_1, Dr. X"), ["O'Brien", "left-arm", "cup_1", "Dr. X"])
        self.assertEqual(bridge._vision_start_body([]), {})
        self.assertEqual(bridge._vision_start_body(["person", "cup"]), {"classes": ["person", "cup"]})

    def test_rejects_empty_and_bad(self):
        with self.assertRaisesRegex(RuntimeError, "empty name"):
            bridge._parse_vision_classes("person,,cup")
        with self.assertRaisesRegex(RuntimeError, "repeated"):
            bridge._parse_vision_classes("phone, cell phone")
        with self.assertRaisesRegex(RuntimeError, "not allowed"):
            bridge._parse_vision_classes("person, cup!")
        with self.assertRaisesRegex(RuntimeError, "not allowed"):
            bridge._parse_vision_classes("x" * 41)
        with self.assertRaisesRegex(RuntimeError, "repeated"):
            bridge._parse_vision_classes("cup, Cup")
        with self.assertRaisesRegex(RuntimeError, "between 1 and 32"):
            bridge._parse_vision_classes(",".join("c%d" % i for i in range(33)))


class VisionUrlTests(unittest.TestCase):
    def test_stream_url(self):
        self.assertEqual(bridge.VISION, "http://192.0.2.148:8092")
        self.assertEqual(bridge.VISION_STREAM, "http://192.0.2.148:8092/vision.ts")
        self.assertTrue(bridge._is_vision_url(bridge.VISION_STREAM))
        self.assertTrue(bridge._is_vision_url(bridge.VISION_STREAM + "?x=1"))
        self.assertFalse(bridge._is_vision_url("http://192.0.2.148:8092/other.ts"))
        self.assertFalse(bridge._is_vision_url("http://example/vision.ts"))


class GladosVisionToolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bridge._vision_held = False
        self._wd = patch.object(bridge, "_vision_watchdog_ensure", new=AsyncMock())
        self._wd.start()

    def tearDown(self):
        self._wd.stop()
        bridge._vision_held = False
        if bridge._vision_watchdog_task is not None:
            bridge._vision_watchdog_cancel()

    async def test_chat_origin_errors_without_side_effects(self):
        order = []

        def origin(_name):
            return "chat"

        async def cam(*_a, **_k):
            order.append("camera")
            return False

        def req(*_a, **_k):
            order.append("request")
            return {}

        async def play(*_a, **_k):
            order.append("play")
            return "playing"

        def wait(*_a, **_k):
            order.append("rtsp-wait")
            return 0.0

        with patch.object(bridge, "_turn_origin", side_effect=origin), \
             patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_wait_rtsp_listening", side_effect=wait), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "_play_video_confirmed", side_effect=play):
            with self.assertRaisesRegex(RuntimeError, "only on the Echo screen"):
                await bridge.glados_vision("on")
        self.assertEqual(order, [])
        self.assertFalse(bridge._vision_held)

    async def test_on_success_order(self):
        order = []

        def origin(_name):
            return "echo"

        async def cam(wanted=None, timeout=5):
            order.append(("camera", wanted))
            return bool(wanted)

        def req(method, path, body=None, timeout=15):
            order.append((method, path, body))
            if path.endswith("/start"):
                return {"state": "running"}
            return {"state": "idle"}

        async def play(url, title):
            order.append(("play", url, title))
            return "playing on the Echo screen"

        def wait(*_a, **_k):
            order.append("rtsp-wait")
            return 0.12

        with patch.object(bridge, "_turn_origin", side_effect=origin), \
             patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_wait_rtsp_listening", side_effect=wait), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "_play_video_confirmed", side_effect=play):
            out = await bridge.glados_vision("on", "person, cup")
        self.assertIn("playing on the Echo screen", out)
        self.assertIn("person, cup", out)
        self.assertEqual(order[0], ("camera", True))
        self.assertEqual(order[1], "rtsp-wait")
        self.assertEqual(order[2], ("POST", "/v1/vision/start", {"classes": ["person", "cup"]}))
        self.assertEqual(order[3], ("play", bridge.VISION_STREAM, "Terminator vision"))
        self.assertTrue(bridge._vision_held)

    async def test_on_without_classes_is_prompt_free(self):
        order = []

        def origin(_name):
            return "echo"

        async def cam(wanted=None, timeout=5):
            order.append(("camera", wanted))
            return bool(wanted)

        def req(method, path, body=None, timeout=15):
            order.append((method, path, body))
            if path.endswith("/start"):
                return {"state": "running"}
            return {"state": "idle"}

        async def play(url, title):
            order.append(("play", url, title))
            return "playing on the Echo screen"

        with patch.object(bridge, "_turn_origin", side_effect=origin), \
             patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_wait_rtsp_listening", return_value=0.1), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "_play_video_confirmed", side_effect=play):
            out = await bridge.glados_vision("on")
        self.assertIn("playing on the Echo screen", out)
        self.assertIn("watching everything", out)
        self.assertIn(("POST", "/v1/vision/start", {}), order)
        self.assertTrue(bridge._vision_held)

    async def test_phone_alias_sent_as_cell_phone(self):
        order = []

        def origin(_name):
            return "echo"

        async def cam(wanted=None, timeout=5):
            return bool(wanted)

        def req(method, path, body=None, timeout=15):
            order.append((method, path, body))
            if path.endswith("/start"):
                return {"state": "running"}
            return {"state": "idle"}

        async def play(url, title):
            return "playing on the Echo screen"

        with patch.object(bridge, "_turn_origin", side_effect=origin), \
             patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_wait_rtsp_listening", return_value=0.1), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "_play_video_confirmed", side_effect=play):
            out = await bridge.glados_vision("on", "person, phone")
        self.assertIn("person, cell phone", out)
        self.assertIn(("POST", "/v1/vision/start", {"classes": ["person", "cell phone"]}), order)

    async def test_start_failure_undoes_without_play_or_stop_video(self):
        order = []

        def origin(_name):
            return "echo"

        async def cam(wanted=None, timeout=5):
            order.append(("camera", wanted))
            return bool(wanted) if wanted is not None else False

        def req(method, path, body=None, timeout=15):
            order.append((method, path, body))
            if path.endswith("/start"):
                raise RuntimeError("Dell vision 502: camera problem")
            return {"state": "idle"}

        async def play(*_a, **_k):
            order.append("play")
            return "playing"

        async def call(name, **_k):
            order.append(("call", name))
            return "ok"

        with patch.object(bridge, "_turn_origin", side_effect=origin), \
             patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_wait_rtsp_listening", return_value=0.1), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "_play_video_confirmed", side_effect=play), \
             patch.object(bridge, "call", side_effect=call):
            with self.assertRaisesRegex(RuntimeError, "Dell vision 502"):
                await bridge.glados_vision("on")
        self.assertIn(("camera", True), order)
        self.assertIn(("POST", "/v1/vision/start", {}), order)
        self.assertIn(("POST", "/v1/vision/stop", None), order)
        self.assertIn(("camera", False), order)
        self.assertNotIn("play", order)
        self.assertNotIn(("call", "stop_video"), order)
        self.assertFalse(bridge._vision_held)

    async def test_play_failure_undoes_including_stop_video(self):
        order = []

        def origin(_name):
            return "echo"

        async def cam(wanted=None, timeout=5):
            order.append(("camera", wanted))
            return False if wanted is False else True

        def req(method, path, body=None, timeout=15):
            order.append((method, path))
            if path.endswith("/start"):
                return {"state": "running"}
            return {"state": "idle"}

        async def play(*_a, **_k):
            order.append("play")
            raise RuntimeError("not confirmed by the Echo")

        async def call(name, **_k):
            order.append(("call", name))
            return "ok"

        with patch.object(bridge, "_turn_origin", side_effect=origin), \
             patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_wait_rtsp_listening", return_value=0.1), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "_play_video_confirmed", side_effect=play), \
             patch.object(bridge, "call", side_effect=call):
            with self.assertRaisesRegex(RuntimeError, "not confirmed by the Echo"):
                await bridge.glados_vision("on", "person")
        self.assertIn("play", order)
        self.assertIn(("call", "stop_video"), order)
        self.assertIn(("POST", "/v1/vision/stop"), order)
        self.assertIn(("camera", False), order)
        self.assertFalse(bridge._vision_held)

    async def _on_with_switch(self, cam_on):
        order = []

        async def cam(wanted=None, timeout=5):
            order.append(("camera", wanted))
            if wanted is True:
                return await cam_on()
            return False

        def req(method, path, body=None, timeout=15):
            order.append((method, path))
            return {"state": "idle"}

        with patch.object(bridge, "_turn_origin", return_value="echo"), \
             patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_wait_rtsp_listening", return_value=0.1), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "_play_video_confirmed", new=AsyncMock()), \
             patch.object(bridge, "call", new=AsyncMock(return_value="ok")):
            with self.assertRaises(RuntimeError) as ctx:
                await bridge.glados_vision("on")
        return order, str(ctx.exception)

    async def test_switch_on_error_still_switches_camera_off(self):
        async def boom():
            raise RuntimeError("camera_streaming state was not reported by the Echo")
        order, err = await self._on_with_switch(boom)
        self.assertIn("not reported", err)
        self.assertEqual(order[0], ("camera", True))
        self.assertIn(("camera", False), order)
        self.assertFalse(bridge._vision_held)

    async def test_switch_on_unconfirmed_still_switches_camera_off(self):
        async def no():
            return False
        order, err = await self._on_with_switch(no)
        self.assertIn("did not turn on", err)
        self.assertIn(("camera", False), order)
        self.assertFalse(bridge._vision_held)

    async def test_off_still_switches_camera_off_when_earlier_steps_fail(self):
        bridge._vision_held = True
        order = []

        async def cam(wanted=None, timeout=5):
            order.append(("camera", wanted))
            return False

        def req(*_a, **_k):
            raise RuntimeError("Dell down")

        with patch.object(bridge, "_turn_origin", return_value="echo"), \
             patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "call", new=AsyncMock(side_effect=RuntimeError("echo offline"))), \
             patch.object(bridge.video_relay, "stop", side_effect=RuntimeError("relay")):
            with self.assertRaisesRegex(RuntimeError, "stop_video: echo offline.*relay stop.*worker stop: Dell down"):
                await bridge.glados_vision("off")
        self.assertEqual(order, [("camera", False)])
        self.assertFalse(bridge._vision_held)

    async def test_http_errors_are_plain(self):
        for code, body, needle in (
            (400, {"error": "bad classes"}, "Dell vision 400: bad classes"),
            (409, {"error": "already running"}, "Dell vision 409: already running"),
            (503, {"error": "ECHO_CAM_PASS isn't set"}, "Dell vision 503:"),
            (502, {"error": "401"}, "Dell vision 502: 401"),
        ):
            class FakeHTTPError(bridge.urllib.error.HTTPError):
                def __init__(self, payload, status):
                    self._payload = payload.encode()
                    self.code = status

                def read(self):
                    return self._payload

            def opener(req, timeout=15):
                raise FakeHTTPError(bridge.json.dumps(body), code)

            with patch.object(bridge.urllib.request, "urlopen", side_effect=opener):
                with self.assertRaisesRegex(RuntimeError, needle):
                    bridge._vision_request("POST", "/v1/vision/start", {"classes": ["person"]})

    async def test_off_reads_back_all_three(self):
        order = []

        def origin(_name):
            return "echo"

        async def cam(wanted=None, timeout=5):
            order.append(("camera", wanted))
            return False

        def req(method, path, body=None, timeout=15):
            order.append((method, path))
            return {"state": "idle", "was": "running"}

        async def call(name, **_k):
            order.append(("call", name))
            return "ok"

        with patch.object(bridge, "_turn_origin", side_effect=origin), \
             patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "call", side_effect=call), \
             patch.object(bridge.video_relay, "stop"):
            out = await bridge.glados_vision("off")
        self.assertIn("Vision off:", out)
        self.assertIn("Echo video ok", out)
        self.assertIn("worker idle", out)
        self.assertIn("camera streaming off", out)
        self.assertEqual(order[0], ("call", "stop_video"))
        self.assertEqual(order[1], ("POST", "/v1/vision/stop"))
        self.assertEqual(order[2], ("camera", False))
        self.assertFalse(bridge._vision_held)

    async def test_stop_video_hook_cleans_vision_when_held(self):
        bridge._vision_held = True
        order = []

        async def cam(wanted=None, timeout=5):
            order.append(("camera", wanted))
            return False

        def req(method, path, body=None, timeout=15):
            order.append((method, path))
            return {"state": "idle"}

        async def call(name, **_k):
            order.append(("call", name))
            return "ok"

        with patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "call", side_effect=call), \
             patch.object(bridge.video_relay, "stop"):
            out = await bridge.stop_video()
        self.assertIn("Terminator vision stopped", out)
        self.assertIn(("call", "stop_video"), order)
        self.assertIn(("POST", "/v1/vision/stop"), order)
        self.assertIn(("camera", False), order)
        self.assertFalse(bridge._vision_held)

    async def test_other_video_releases_vision_but_vision_url_does_not(self):
        released = []

        async def release(**_k):
            released.append(True)

        class _Echo:
            async def __aenter__(self):
                raise RuntimeError("echo-entered")

            async def __aexit__(self, *_a):
                return False

        def echo_cm():
            return _Echo()

        with patch.object(bridge, "_vision_release", side_effect=release), \
             patch.object(bridge, "echo", side_effect=echo_cm), \
             patch.object(bridge.video_relay, "is_relay_url", return_value=False), \
             patch.object(bridge.video_relay, "stop"):
            bridge._vision_held = True
            with self.assertRaisesRegex(RuntimeError, "echo-entered"):
                await bridge._play_video_confirmed("http://192.0.2.148:8091/media/clip.mp4", "clip")
            self.assertEqual(released, [True])

            released.clear()
            bridge._vision_held = True
            with self.assertRaisesRegex(RuntimeError, "echo-entered"):
                await bridge._play_video_confirmed(bridge.VISION_STREAM, bridge.VISION_TITLE)
            self.assertEqual(released, [])


class RtspReadyWaitTests(unittest.TestCase):
    def test_succeeds_after_refusals(self):
        calls = []

        class Sock:
            def close(self):
                calls.append("close")

        def connect(addr, timeout=1):
            calls.append(("connect", addr, timeout))
            if sum(1 for c in calls if c[0] == "connect") < 3:
                raise ConnectionRefusedError(111, "Connection refused")
            return Sock()

        sleeps = []
        waited = bridge._wait_rtsp_listening(
            "192.0.2.248", 8554, timeout=2, connect=connect, sleep=lambda s: sleeps.append(s),
        )
        self.assertGreaterEqual(sum(1 for c in calls if isinstance(c, tuple) and c[0] == "connect"), 3)
        self.assertIn("close", calls)
        self.assertTrue(sleeps)
        self.assertGreaterEqual(waited, 0)

    def test_times_out_and_fails_loudly(self):
        def connect(addr, timeout=1):
            raise ConnectionRefusedError(111, "Connection refused")

        with self.assertRaisesRegex(
            RuntimeError,
            r"RTSP port 192.0.2.248:8554 did not open within 0s.*Connection refused.*password may not be set",
        ):
            # timeout 0: one attempt then fail (no real network)
            bridge._wait_rtsp_listening(
                "192.0.2.248", 8554, timeout=0, connect=connect, sleep=lambda s: None,
            )


class RtspReadyInOnFlowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bridge._vision_held = False
        self._wd = patch.object(bridge, "_vision_watchdog_ensure", new=AsyncMock())
        self._wd.start()

    def tearDown(self):
        self._wd.stop()
        bridge._vision_held = False
        if bridge._vision_watchdog_task is not None:
            bridge._vision_watchdog_cancel()

    async def test_wait_failure_cleans_up_and_does_not_start(self):
        order = []

        def origin(_name):
            return "echo"

        async def cam(wanted=None, timeout=5):
            order.append(("camera", wanted))
            return False if wanted is False else True

        def wait(*_a, **_k):
            order.append("rtsp-wait")
            raise RuntimeError(
                "Echo camera RTSP port 192.0.2.248:8554 did not open within 15s after "
                "camera_streaming was turned on (Connection refused). "
                "The stream password may not be set, or the camera stream failed to start."
            )

        def req(method, path, body=None, timeout=15):
            order.append((method, path, body))
            return {"state": "idle"}

        async def play(*_a, **_k):
            order.append("play")
            return "playing"

        async def call(name, **_k):
            order.append(("call", name))
            return "ok"

        with patch.object(bridge, "_turn_origin", side_effect=origin), \
             patch.object(bridge, "_camera_streaming_state", side_effect=cam), \
             patch.object(bridge, "_wait_rtsp_listening", side_effect=wait), \
             patch.object(bridge, "_vision_request", side_effect=req), \
             patch.object(bridge, "_play_video_confirmed", side_effect=play), \
             patch.object(bridge, "call", side_effect=call):
            with self.assertRaisesRegex(RuntimeError, "did not open within 15s"):
                await bridge.glados_vision("on")
        self.assertEqual(order[0], ("camera", True))
        self.assertEqual(order[1], "rtsp-wait")
        self.assertIn(("POST", "/v1/vision/stop", None), order)
        self.assertIn(("camera", False), order)
        self.assertFalse(any(
            isinstance(item, tuple) and item[:2] == ("POST", "/v1/vision/start")
            for item in order
        ))
        self.assertNotIn("play", order)
        self.assertFalse(bridge._vision_held)




class VisionSnapshotTests(unittest.IsolatedAsyncioTestCase):
    def test_start_body_omits_relations(self):
        self.assertEqual(bridge._vision_start_body([]), {})
        self.assertNotIn("relations", bridge._vision_start_body([]))
        body = bridge._vision_start_body(["person"])
        self.assertEqual(body, {"classes": ["person"]})
        self.assertNotIn("relations", body)

    def test_parse_snapshot_labels(self):
        self.assertEqual(bridge._parse_snapshot_labels("person, cup, person"), ["person", "cup"])
        self.assertEqual(bridge._parse_snapshot_labels(""), [])
        self.assertEqual(bridge._parse_snapshot_labels(None), [])

    def test_http_errors_are_spoken(self):
        self.assertIn(
            "not running",
            str(bridge._vision_snapshot_http_error(409, '{"error":"not running"}')),
        )
        self.assertEqual(
            str(bridge._vision_snapshot_http_error(422, '{"error":"none"}')),
            "I can't see any objects right now",
        )
        self.assertIn("format", str(bridge._vision_snapshot_http_error(400, "bad")))
        self.assertIn("timed out", str(bridge._vision_snapshot_http_error(504, "x")))

    def test_vision_is_on_from_held_or_status(self):
        with patch.object(bridge, "_vision_held", True):
            self.assertTrue(bridge._vision_is_on())
        with patch.object(bridge, "_vision_held", False), \
             patch.object(bridge, "_vision_request", lambda *a, **k: {"state": "running"}):
            self.assertTrue(bridge._vision_is_on())
        with patch.object(bridge, "_vision_held", False), \
             patch.object(bridge, "_vision_request", lambda *a, **k: {"state": "idle"}):
            self.assertFalse(bridge._vision_is_on())
        with patch.object(bridge, "_vision_held", False), \
             patch.object(bridge, "_vision_request", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down"))):
            self.assertFalse(bridge._vision_is_on())

    async def test_look_during_vision_saves_and_shows_labels(self):
        jpeg = b"\xff\xd8\xff\xd9" + b"cutout"
        shown = []
        captured = []

        async def fake_still():
            captured.append("still")
            return b"\xff\xd8nope"

        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def fake_lan(_jpeg):
            yield ("slot", "http://127.0.0.1/snap.jpg")

        async def fake_show(url, *a, **k):
            shown.append(url)
            return "picture sent to the Echo"

        td = tempfile.TemporaryDirectory(prefix="vision-snap-")
        look = Path(td.name) / "look"
        look.mkdir()
        with patch.object(bridge, "CACHE_LOOK", look), \
             patch.object(bridge, "_vision_is_on", lambda: True), \
             patch.object(bridge, "_turn_origin", lambda _n: "echo"), \
             patch.object(bridge, "_vision_snapshot", lambda *a, **k: (jpeg, ["person", "cup"])), \
             patch.object(bridge, "_capture_still", fake_still), \
             patch.object(bridge, "_lan_jpeg", fake_lan), \
             patch.object(bridge, "_show_picture_checked", fake_show), \
             patch.object(bridge, "_wait_echo_fetched", AsyncMock()):
            out = await bridge.look()
        self.assertIn("person, cup", out)
        self.assertIn("white background", out)
        self.assertIn("SAVED:", out)
        self.assertIn("picture sent to the Echo", out)
        self.assertEqual(captured, [])
        saved = Path(out.split("SAVED:", 1)[1].strip())
        self.assertTrue(str(saved).startswith(str(look)))
        self.assertEqual(saved.read_bytes(), jpeg)
        td.cleanup()

    async def test_look_during_vision_422_does_not_fall_back(self):
        captured = []

        async def fake_still():
            captured.append("still")
            return b"\xff\xd8nope"

        def snap(*_a, **_k):
            raise RuntimeError("I can't see any objects right now")

        with patch.object(bridge, "_vision_is_on", lambda: True), \
             patch.object(bridge, "_turn_origin", lambda _n: "echo"), \
             patch.object(bridge, "_vision_snapshot", snap), \
             patch.object(bridge, "_capture_still", fake_still):
            with self.assertRaisesRegex(RuntimeError, "I can't see any objects right now"):
                await bridge.look()
        self.assertEqual(captured, [])

    async def test_look_during_vision_409_does_not_fall_back(self):
        captured = []

        async def fake_still():
            captured.append("still")
            return b"\xff\xd8nope"

        def snap(*_a, **_k):
            raise RuntimeError("Terminator vision is not running, so I can't take that picture")

        with patch.object(bridge, "_vision_is_on", lambda: True), \
             patch.object(bridge, "_turn_origin", lambda _n: "echo"), \
             patch.object(bridge, "_vision_snapshot", snap), \
             patch.object(bridge, "_capture_still", fake_still):
            with self.assertRaisesRegex(RuntimeError, "not running"):
                await bridge.look()
        self.assertEqual(captured, [])



class VisionLogPlayingTests(unittest.TestCase):
    def test_playing_vision_ts(self):
        text = (
            "I [1] video: starting id=2 url=http://192.0.2.148:8092/vision.ts origin=0 from=\n"
            "I [2] video: playing id=2 url=http://192.0.2.148:8092/vision.ts video=h264\n"
        )
        self.assertTrue(bridge._echo_vision_playing_from_log(text))

    def test_user_stopped_is_not_playing(self):
        text = (
            "I [1] video: starting id=2 url=http://192.0.2.148:8092/vision.ts origin=0 from=\n"
            "I [2] video: playing id=2 url=http://192.0.2.148:8092/vision.ts video=h264\n"
            "I [3] video: stopped id=2\n"
            "I [3] video: over id=2 by itself=false shown=5741 dropped=8\n"
        )
        self.assertFalse(bridge._echo_vision_playing_from_log(text))

    def test_covered_after_vision_still_counts(self):
        text = (
            "I [0] video: starting id=2 url=http://192.0.2.148:8092/vision.ts origin=0 from=\n"
            "I [1] video: playing id=2 url=http://192.0.2.148:8092/vision.ts video=h264\n"
            "I [2] video: covered; paused under it id=2\n"
        )
        self.assertTrue(bridge._echo_vision_playing_from_log(text))

    def test_other_video_is_not_vision(self):
        text = (
            "I [1] video: starting id=3 url=http://192.0.2.148:8091/media/clip.mp4 origin=0 from=\n"
            "I [2] video: playing id=3 url=http://192.0.2.148:8091/media/clip.mp4\n"
        )
        self.assertFalse(bridge._echo_vision_playing_from_log(text))

    def test_empty_log_is_unknown(self):
        self.assertIsNone(bridge._echo_vision_playing_from_log(""))

    def test_showing_prefers_log_over_sensors(self):
        sensors = {"video_state": "idle", "video_title": ""}
        self.assertTrue(bridge._showing_vision_ts(sensors, True))
        self.assertFalse(bridge._showing_vision_ts(sensors, False))

    def test_showing_falls_back_to_title_when_log_unknown(self):
        self.assertTrue(bridge._showing_vision_ts(
            {"video_state": "paused", "video_title": "Terminator vision"}, None
        ))
        self.assertFalse(bridge._showing_vision_ts(
            {"video_state": "idle", "video_title": "Terminator vision"}, None
        ))
        self.assertFalse(bridge._showing_vision_ts(
            {"video_state": "playing", "video_title": "something else"}, None
        ))


class VisionWatchdogTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bridge._vision_held = False
        bridge._vision_watchdog_cancel()

    def tearDown(self):
        bridge._vision_held = False
        bridge._vision_watchdog_cancel()

    async def test_tears_down_after_15s_not_showing(self):
        clock = [0.0]
        events = []
        logs = []

        async def sleep(s):
            clock[0] += s
            events.append(("sleep", s, clock[0]))

        def now():
            return clock[0]

        async def snap():
            return {"video_state": "idle", "video_title": "", "showing_vision": False}

        async def status():
            return {"state": "running", "stream_clients": 0}

        async def release(**k):
            events.append(("release", k, clock[0]))
            bridge._vision_held = False
            return []

        def log(*a):
            logs.append(" ".join(str(x) for x in a))

        bridge._vision_held = True
        with patch.object(bridge, "_log", log):
            await bridge._vision_watchdog_loop(
                poll_s=10, absent_s=15, sleep=sleep, clock=now,
                echo_snap=snap, dell_status=status, release=release,
            )
        releases = [e for e in events if e[0] == "release"]
        self.assertEqual(len(releases), 1)
        self.assertEqual(releases[0][1], {"stop_echo_video": False})
        self.assertGreaterEqual(releases[0][2], 15)
        self.assertFalse(any(e[0] == "release" and e[1].get("stop_echo_video") for e in events))
        self.assertFalse(bridge._vision_held)
        self.assertTrue(any("not showing vision.ts" in x for x in logs))
        self.assertTrue(any("tearing down" in x and "Full off" in x for x in logs))

    async def test_does_not_tear_down_while_showing(self):
        clock = [0.0]
        polls = [0]
        events = []

        async def sleep(s):
            clock[0] += s
            if clock[0] >= 40:
                bridge._vision_held = False

        def now():
            return clock[0]

        async def snap():
            polls[0] += 1
            return {"video_state": "playing", "video_title": "Terminator vision", "showing_vision": True}

        async def status():
            return {"state": "running", "stream_clients": 1}

        async def release(**k):
            events.append(("release", k))
            return []

        bridge._vision_held = True
        await bridge._vision_watchdog_loop(
            poll_s=10, absent_s=15, sleep=sleep, clock=now,
            echo_snap=snap, dell_status=status, release=release,
        )
        self.assertEqual(events, [])
        self.assertGreaterEqual(polls[0], 2)

    async def test_showing_again_resets_the_timer(self):
        clock = [0.0]
        events = []
        # not showing at 0, showing at 10 (resets the 15s timer), gone again from 20, tear down at 35.

        async def sleep(s):
            clock[0] += s

        def now():
            return clock[0]

        async def snap():
            t = clock[0]
            showing = 10 <= t < 20
            return {"video_state": "playing" if showing else "idle", "video_title": "Terminator vision" if showing else "", "showing_vision": showing}

        async def status():
            return {"state": "running", "stream_clients": 0}

        async def release(**k):
            events.append(("release", k, clock[0]))
            bridge._vision_held = False
            return []

        bridge._vision_held = True
        await bridge._vision_watchdog_loop(
            poll_s=10, absent_s=15, sleep=sleep, clock=now,
            echo_snap=snap, dell_status=status, release=release,
        )
        self.assertEqual(len(events), 1)
        self.assertGreaterEqual(events[0][2], 35)
        self.assertEqual(events[0][1], {"stop_echo_video": False})

    async def test_poll_failure_does_not_tear_down(self):
        clock = [0.0]
        events = []
        fails = [0]

        async def sleep(s):
            clock[0] += s
            if clock[0] >= 30:
                bridge._vision_held = False

        def now():
            return clock[0]

        async def snap():
            fails[0] += 1
            raise RuntimeError("echo down")

        async def status():
            return {"state": "running", "stream_clients": 0}

        async def release(**k):
            events.append("release")
            return []

        logs = []

        def log(*a):
            logs.append(" ".join(str(x) for x in a))

        bridge._vision_held = True
        with patch.object(bridge, "_log", log):
            await bridge._vision_watchdog_loop(
                poll_s=10, absent_s=15, sleep=sleep, clock=now,
                echo_snap=snap, dell_status=status, release=release,
            )
        self.assertEqual(events, [])
        self.assertTrue(any("poll failed" in x for x in logs))

    async def test_adopt_starts_watchdog_when_worker_running(self):
        started = []

        async def ensure():
            started.append(True)
            return "task"

        with patch.object(bridge, "_vision_request", lambda *a, **k: {"state": "running", "stream_clients": 0}), \
             patch.object(bridge, "_vision_watchdog_ensure", ensure):
            ok = await bridge._vision_adopt_running_session()
        self.assertTrue(ok)
        self.assertTrue(bridge._vision_held)
        self.assertEqual(started, [True])

    async def test_adopt_skips_when_idle(self):
        started = []

        async def ensure():
            started.append(True)

        with patch.object(bridge, "_vision_request", lambda *a, **k: {"state": "idle"}), \
             patch.object(bridge, "_vision_watchdog_ensure", ensure):
            ok = await bridge._vision_adopt_running_session()
        self.assertFalse(ok)
        self.assertFalse(bridge._vision_held)
        self.assertEqual(started, [])

    async def test_on_success_starts_watchdog(self):
        order = []

        async def cam(wanted=None, timeout=5):
            order.append(("cam", wanted))
            return bool(wanted)

        def req(method, path, body=None, timeout=15):
            order.append((method, path, body))
            return {"state": "running"}

        async def play(*_a, **_k):
            order.append("play")
            return "playing on the Echo"

        def wait(*_a, **_k):
            return 0.0

        async def ensure():
            order.append("watchdog")
            return None

        with patch.object(bridge, "_turn_origin", lambda _n: "echo"), \
             patch.object(bridge, "_camera_streaming_state", cam), \
             patch.object(bridge, "_vision_request", req), \
             patch.object(bridge, "_play_video_confirmed", play), \
             patch.object(bridge, "_wait_rtsp_listening", wait), \
             patch.object(bridge, "_vision_watchdog_ensure", ensure), \
             patch.object(bridge, "video_relay") as tr:
            # the video relay may not be hit on on()
            out = await bridge.glados_vision("on")
        self.assertIn("watchdog", order)
        self.assertEqual(order[-1], "watchdog")
        self.assertIn("playing", out)
        self.assertTrue(bridge._vision_held)


VISION = "http://192.0.2.148:8092/vision.ts"
CLIP = "http://192.0.2.148:8091/media/clip.mp4"


class VisionLogPairingTests(unittest.TestCase):
    """The 2026-11-14 false teardowns: progress lines without a URL read as 'not vision'."""

    def test_progress_lines_after_start_count_as_playing(self):
        # Exact shape of session id=3 at 10:55 London that was torn down by mistake.
        text = (
            "I [408911.35] video: starting id=3 url=%s origin=0 from=\n"
            "I [408911.35] video: covered; paused under it id=3\n"
            "I [408916.89] video: playing id=3 url=%s video=h264 size=960x480 fps=30\n"
            "I [408970.41] video: uncovered; going on id=3\n"
            "I [408970.49] video: playing id=3 shown=3 dropped=0 at=100ms fps=0.3\n"
            "I [408976.89] video: playing id=3 shown=191 dropped=1 at=6.5s fps=19.1\n"
            "I [408986.93] video: playing id=3 shown=301 dropped=0 at=16.5s fps=30.1\n"
        ) % (VISION, VISION)
        self.assertIs(bridge._echo_vision_playing_from_log(text), True)

    def test_over_means_not_playing(self):
        text = (
            "I [1] video: starting id=3 url=%s origin=0 from=\n"
            "I [2] video: playing id=3 shown=301 dropped=0 at=16.5s fps=30.1\n"
            "I [3] video: over id=3 by itself=true shown=669 dropped=4\n"
        ) % VISION
        self.assertIs(bridge._echo_vision_playing_from_log(text), False)

    def test_stopped_means_not_playing(self):
        text = (
            "I [1] video: starting id=2 url=%s origin=0 from=\n"
            "I [2] video: playing id=2 shown=299 dropped=2 at=3m11.2s fps=29.9\n"
            "I [3] video: stopped id=2\n"
        ) % VISION
        self.assertIs(bridge._echo_vision_playing_from_log(text), False)

    def test_start_older_than_40_lines_is_still_seen(self):
        lines = ["I [1] video: starting id=7 url=%s origin=0 from=" % VISION]
        lines += ["I [%d] video: playing id=7 shown=300 dropped=0 at=%ds fps=30" % (i, i * 10) for i in range(2, 200)]
        self.assertGreater(len(lines), 40)
        self.assertIs(bridge._echo_vision_playing_from_log("\n".join(lines) + "\n"), True)

    def test_echo_grep_keeps_only_start_and_end_lines(self):
        # The remote command filters to start/end lines, so progress lines cannot push the start out.
        seen = []

        def fake_ssh(remote, timeout=12):
            seen.append(remote)
            return "I [1] video: starting id=7 url=%s origin=0 from=\n" % VISION

        with patch.object(bridge, "_echo_ssh", fake_ssh):
            self.assertIs(bridge._echo_vision_playing_from_log(), True)
        self.assertEqual(len(seen), 1)
        self.assertIn('video: (starting|over|stopped) id=', seen[0])
        self.assertNotIn('"video: "', seen[0])

    def test_other_video_id_is_not_mixed_up(self):
        # An older clip (id=4) ending after vision (id=5) started does not end vision.
        text = (
            "I [1] video: starting id=4 url=%s origin=0 from=\n"
            "I [2] video: starting id=5 url=%s origin=0 from=\n"
            "I [3] video: over id=4 by itself=true shown=80 dropped=1\n"
            "I [4] video: playing id=5 shown=300 dropped=0 at=10s fps=30\n"
        ) % (CLIP, VISION)
        self.assertIs(bridge._echo_vision_playing_from_log(text), True)
        # A newer clip (id=6) after vision is not vision, playing or ended.
        clip_after = text + "I [5] video: starting id=6 url=%s origin=0 from=\n" % CLIP
        self.assertIs(bridge._echo_vision_playing_from_log(clip_after), False)
        self.assertIs(bridge._echo_vision_playing_from_log(clip_after + "I [6] video: over id=6 by itself=true\n"), False)
        # A vision "over" for an earlier id (before an echod restart reset ids) does not end the new one.
        reset = (
            "I [1] video: starting id=1 url=%s origin=0 from=\n"
            "I [2] video: over id=1 by itself=false\n"
            "I [3] video: starting id=1 url=%s origin=0 from=\n"
        ) % (VISION, VISION)
        self.assertIs(bridge._echo_vision_playing_from_log(reset), True)

    def test_url_only_from_starting_line(self):
        # A progress or playing line mentioning vision.ts cannot make another video count as vision.
        text = (
            "I [1] video: starting id=8 url=%s origin=0 from=\n"
            "I [2] video: playing id=8 url=%s video=h264\n"
        ) % (CLIP, VISION)
        self.assertIs(bridge._echo_vision_playing_from_log(text), False)

    def test_tap_closing_vision_is_not_playing(self):
        # Exact lines from 08:21-08:25 London: Mo tapped the video away, and vision stayed on until 09:45
        # because there was no watchdog then. This must read as "not playing" so the watchdog releases it.
        text = (
            "I [399722.34] video: starting id=2 url=%s origin=0 from=\n"
            "I [399722.34] video: covered; paused under it id=2\n"
            "I [399727.76] video: playing id=2 url=%s video=h264 size=960x480 fps=30\n"
            "I [399967.81] video: playing id=2 shown=299 dropped=2 at=3m11.2s fps=29.9\n"
            "I [399968.25] screen: video stopped\n"
            "I [399968.25] video: stopped id=2\n"
            "I [399968.28] video: over id=2 by itself=false shown=5741 dropped=8\n"
        ) % (VISION, VISION)
        self.assertIs(bridge._echo_vision_playing_from_log(text), False)
        self.assertFalse(bridge._showing_vision_ts({"video_state": "idle", "video_title": ""}, False))

    def test_no_start_line_is_unknown(self):
        text = "I [1] video: playing id=3 shown=300 dropped=0 at=10s fps=30\n"
        self.assertIsNone(bridge._echo_vision_playing_from_log(text))


_HOLD_LOCK = r"""
import fcntl, os, sys, time
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
os.ftruncate(fd, 0); os.write(fd, b"%d\n" % os.getpid())
print("held", flush=True)
sys.stdin.read()
"""


class VisionWatchdogLockTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bridge._vision_held = False
        bridge._vision_watchdog_cancel()
        bridge._vision_watchdog_lock_release()
        self.td = tempfile.TemporaryDirectory(prefix="vision-lock-")
        self._path = patch.object(bridge, "_VISION_WATCHDOG_LOCK_PATH", Path(self.td.name) / "wd.lock")
        self._path.start()

    def tearDown(self):
        bridge._vision_held = False
        bridge._vision_watchdog_cancel()
        bridge._vision_watchdog_lock_release()
        self._path.stop()
        self.td.cleanup()

    def _other_process_holds_lock(self):
        import subprocess, sys
        p = subprocess.Popen(
            [sys.executable, "-c", _HOLD_LOCK, str(bridge._VISION_WATCHDOG_LOCK_PATH)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        self.assertEqual(p.stdout.readline().strip(), "held")
        self.addCleanup(lambda: (p.stdin.close(), p.wait(5), p.stdout.close()))
        return p

    def test_only_one_process_can_take_the_lock(self):
        holder = self._other_process_holds_lock()
        self.assertFalse(bridge._vision_watchdog_lock_acquire())
        self.assertIsNone(bridge._vision_watchdog_lock_fd)
        holder.stdin.close()
        holder.wait(5)
        self.assertTrue(bridge._vision_watchdog_lock_acquire())
        self.assertEqual(bridge._VISION_WATCHDOG_LOCK_PATH.read_text().strip(), str(os.getpid()))

    async def test_non_owner_does_not_run_watchdog_and_logs_it(self):
        holder = self._other_process_holds_lock()
        logs = []
        bridge._vision_held = True
        loop = AsyncMock()
        with patch.object(bridge, "_log", lambda *a: logs.append(" ".join(map(str, a)))), \
             patch.object(bridge, "_vision_watchdog_loop", loop):
            t = await bridge._vision_watchdog_ensure()
        self.assertIsNone(t)
        self.assertIsNone(bridge._vision_watchdog_task)
        loop.assert_not_called()
        self.assertTrue(any("NOT watching" in x and str(holder.pid) in x for x in logs), logs)

    async def test_owner_runs_watchdog_and_releases_lock_when_done(self):
        bridge._vision_held = True
        gate = asyncio.Event()

        async def fake_loop():
            await gate.wait()

        with patch.object(bridge, "_log", lambda *a: None), \
             patch.object(bridge, "_vision_watchdog_loop", fake_loop):
            t = await bridge._vision_watchdog_ensure()
            self.assertIsNotNone(t)
            self.assertIs(await bridge._vision_watchdog_ensure(), t)
            self.assertIsNotNone(bridge._vision_watchdog_lock_fd)
            gate.set()
            await t
            await asyncio.sleep(0)
        self.assertIsNone(bridge._vision_watchdog_lock_fd)
        # Free again: another process could now take it.
        self.assertTrue(bridge._vision_watchdog_lock_acquire())

    async def test_cancel_releases_lock(self):
        bridge._vision_held = True

        async def fake_loop():
            await asyncio.Event().wait()

        with patch.object(bridge, "_log", lambda *a: None), \
             patch.object(bridge, "_vision_watchdog_loop", fake_loop):
            t = await bridge._vision_watchdog_ensure()
            await asyncio.sleep(0)
            bridge._vision_watchdog_cancel()
            with self.assertRaises(asyncio.CancelledError):
                await t
            await asyncio.sleep(0)
        self.assertIsNone(bridge._vision_watchdog_lock_fd)

    async def test_lock_open_failure_is_loud_and_does_not_watch(self):
        bridge._vision_held = True
        logs = []
        with patch.object(bridge, "_VISION_WATCHDOG_LOCK_PATH", Path(self.td.name) / "missing-dir" / "wd.lock"), \
             patch.object(bridge, "_log", lambda *a: logs.append(" ".join(map(str, a)))):
            t = await bridge._vision_watchdog_ensure()
        self.assertIsNone(t)
        self.assertTrue(any("ERROR" in x and "NOT watching" in x for x in logs), logs)


class GladosVisionDocstringTests(unittest.TestCase):
    def test_docstring_maps_the_new_names(self):
        src = Path(bridge.__file__).read_text()
        for name in ("terminator vision", "robot vision", "computer vision", "GlaDOS vision"):
            self.assertIn(name, src)
        self.assertIn('not explain the field', src)
        self.assertIn('"stop\n    terminator vision"', src)

    def test_title_is_terminator_vision_and_old_title_does_not_count(self):
        self.assertEqual(bridge.VISION_TITLE, "Terminator vision")
        self.assertTrue(bridge._showing_vision_ts({"video_state": "playing", "video_title": "Terminator vision"}, None))
        self.assertFalse(bridge._showing_vision_ts({"video_state": "playing", "video_title": "GlaDOS vision"}, None))

    def test_docstring_describes_terminator_look(self):
        doc = bridge.glados_vision.__doc__ if hasattr(bridge.glados_vision, "__doc__") else ""
        src = Path(bridge.__file__).read_text()
        self.assertNotIn("red when close", src)
        self.assertNotIn("white outlines", src.replace("\n    ", " "))
        flat = " ".join(src.split())
        self.assertIn("depth is grayscale, white when close and black when far", flat)
        self.assertIn("outline and label in its own colour by label type", flat)
        self.assertIn("dotted lines in the subject's colour", flat)


if __name__ == "__main__":
    unittest.main(verbosity=2)
