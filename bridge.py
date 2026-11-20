# /// script
# requires-python = ">=3.11"
# dependencies = ["mcp==1.*", "aioesphomeapi", "yt-dlp==2026.10.26", "pillow==12.*"]
# ///
"""MCP server giving Hermes the Echo Show's device tools over its ESPHome API (LAN only).

Env: ECHO_HOST (the Echo LAN IP), ECHO_KEY_FILE (path to the ESPHome noise key), ECHO_SSH_KEY (path to the
existing dedicated Echo SSH private key, e.g. ~/.ssh/echo_key), DELL_MEDIA_URL (the Dell media gateway,
e.g. http://<dell-ip>:8091) and DELL_LLM_API_KEY (its Bearer key). Fails loudly if unset. The SSH key is
used only as an identity file path; this process never prints or returns its contents.

yt-dlp is pinned in the script header (yt-dlp==2026.10.26). To update it, bump that pin; the next
`uv run --script` of this file (a hermes-gateway restart) installs the new version.
"""
import atexit, base64, fcntl, io, json, os, pathlib, asyncio, re, secrets, socket, sqlite3, subprocess, sys, threading, time, urllib.error, urllib.parse, urllib.request
from typing import Annotated
from pydantic import Field
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from aioesphomeapi import APIClient, CameraState, MediaPlayerCommand
from PIL import Image, ImageOps, UnidentifiedImageError
from mcp.server.fastmcp import FastMCP
import video_relay
import long_jobs
import hermes_handoff

HOST = os.environ["ECHO_HOST"]
KEY = pathlib.Path(os.environ["ECHO_KEY_FILE"]).expanduser().read_text().strip()
SSH_KEY = pathlib.Path(os.environ["ECHO_SSH_KEY"]).expanduser()
if not SSH_KEY.is_file():
    raise SystemExit(f"ECHO_SSH_KEY is not a file: {SSH_KEY}")
MEDIA = os.environ["DELL_MEDIA_URL"].rstrip("/")
_media_host = urllib.parse.urlsplit(MEDIA).hostname
if not _media_host:
    raise SystemExit("DELL_MEDIA_URL has no host")
LLM = f"http://{_media_host}:8090"
LLM_TIMEOUT_S = 180  # at least a Dell LLM-proxy hold (~110s) plus reload (~23s)
# Dell vision worker, same host as the media gateway, dedicated port, LAN-only.
VISION_PORT = 8092
VISION = f"http://{_media_host}:{VISION_PORT}"
VISION_STREAM = f"{VISION}/vision.ts"
# The Echo video title for the feed. _showing_vision_ts matches it when the log is inconclusive.
VISION_TITLE = "Terminator vision"
VISION_START_TIMEOUT = 30
RTSP_PORT = 8554
RTSP_READY_TIMEOUT_S = 15
RTSP_READY_POLL_S = 0.2
_VISION_CLASS_RE = re.compile(r"^[A-Za-z0-9 _'.\-]{1,40}$")
_VISION_PHONE_ALIASES = frozenset({"phone"})
_vision_held = False
_VISION_WATCHDOG_POLL_S = 10
_VISION_WATCHDOG_ABSENT_S = 15
_vision_watchdog_task = None
# One watchdog per machine: hermes-gateway and hermes-dashboard each run their own echo MCP,
# and each used to watch and tear down. Only the process holding this flock runs the watchdog.
_VISION_WATCHDOG_LOCK_PATH = pathlib.Path.home() / ".hermes" / "echo-vision-watchdog.lock"
_vision_watchdog_lock_fd = None
LOOK_TIMEOUT = 10
# In-memory photo server for look(): LAN-only, never 0.0.0.0, never written to disk.
LOOK_BIND = os.environ["BRIDGE_BIND"]
LOOK_PORT = 8643
LOOK_SHOW_S = 20
# LOOK_SERVE_S is only the fetch window for the Echo (and the pre-check) to take the
# JPEG from memory. Display length is LOOK_SHOW_S and does not need the server to stay up.
LOOK_SERVE_S = 30
SPEAK_BIND = os.environ["BRIDGE_BIND"]
SPEAK_PORT = 8645
if not os.environ["DELL_LLM_API_KEY"] or os.environ["DELL_LLM_API_KEY"].startswith("${"):
    raise SystemExit("DELL_LLM_API_KEY is empty or was not substituted")
@asynccontextmanager
async def _echo_lifespan(_app):
    """On MCP start, adopt a Dell worker left running by a previous process and watch it.

    Only the hermes-gateway echo MCP runs the long-job notifier. hermes-dashboard starts the
    same MCP for its Chat tab, and a dashboard-owned notifier used to hold .notifier.lock
    across gateway restarts and keep sending notices from stale code.
    """
    adopt = globals().get("_vision_adopt_running_session")
    if adopt is not None:
        await adopt()
    if long_jobs.launched_by_hermes_gateway():
        long_jobs.ensure_notifier(_log, _long_job_notify)
    else:
        _log("long-job", "notifier: not starting (only the hermes-gateway MCP runs it)")
    yield {}


mcp = FastMCP("echo", lifespan=_echo_lifespan)
CONFIRM_S = 10
VIDEO_CONFIRM_S = 30
# A just-started clip often sits in loading/paused while the first frames and sound come up.
# Confirming only video_state=playing for 10s produced a false "Video failed" notify on
# 2026-11-13 job 11f63fb63df7429a984183b22b5e1fc5: state stayed paused and the Echo log
# still showed "playing received audio from=Video".

@asynccontextmanager
async def echo():
    c = APIClient(HOST, 6053, None, noise_psk=KEY)
    await c.connect(login=True)
    try:
        ents, svcs = await c.list_entities_services()
        yield c, {e.object_id: e for e in ents}, {s.name: s for s in svcs}
    finally:
        await c.disconnect()

async def call(name, answers=False, **data):
    async with echo() as (c, _, svcs):
        r = await c.execute_service(svcs[name], data, return_response=True if answers else None)
        if r is None:
            return "ok"
        if not r.success:
            raise RuntimeError(r.error_message)
        return r.response_data.decode() if r.response_data else "ok"

async def text_sensor(object_id):
    async with echo() as (c, ents, _):
        key, got = ents[object_id].key, asyncio.get_running_loop().create_future()
        c.subscribe_states(lambda s: s.key == key and not got.done() and got.set_result(s))
        s = await asyncio.wait_for(got, 5)
        return s.state

async def act_and_confirm(act, ids, judge, timeout=CONFIRM_S):
    """Run act(c, ents, svcs) on one connection, then watch the states in ids until judge(before, now)
    gives a verdict string (success) or raises (the Echo's own error). No verdict in time is an error."""
    async with echo() as (c, ents, svcs):
        keys = {ents[i].key: i for i in ids}
        now, changed = {}, asyncio.Event()
        def on(s):
            if s.key in keys:
                now[keys[s.key]] = s.state
                changed.set()
        c.subscribe_states(on)
        end = time.monotonic() + 3
        while len(now) < len(keys) and time.monotonic() < end:
            changed.clear()
            try:
                await asyncio.wait_for(changed.wait(), end - time.monotonic())
            except asyncio.TimeoutError:
                break
        before = dict(now)
        r = act(c, ents, svcs)
        if asyncio.iscoroutine(r):
            await r
        end = time.monotonic() + timeout
        while True:
            verdict = judge(before, now)
            if verdict:
                return verdict
            left = end - time.monotonic()
            if left <= 0:
                raise RuntimeError("not confirmed by the Echo within %ds: %s" % (timeout, {k: str(v) for k, v in now.items()}))
            changed.clear()
            try:
                await asyncio.wait_for(changed.wait(), left)
            except asyncio.TimeoutError:
                pass

# lens_cover is a binary sensor with device_class "opening": True means the shutter is
# open (uncovered), False means it is closed. Missing or unknown is not treated as closed.
COVER_SETTLE_S = 2.5

def _lens_cover_closed(value):
    if value is None or value == "":
        return False
    if isinstance(value, bool):
        return value is False
    if isinstance(value, (int, float)) and value in (0, 1):
        return int(value) == 0
    s = str(value).strip().lower()
    return s in ("off", "closed", "false", "0")

def _covered_status(now, reason):
    st = now.get("video_state") or "unknown"
    if reason == "lens":
        return (
            "on the Echo screen but paused under the camera cover "
            "(lens_cover closed; video_state=%s). Open the cover to resume." % st
        )
    return (
        "on the Echo screen but paused under a cover "
        "(video: covered; paused under it; video_state=%s). "
        "Open the camera cover, or wait until a voice turn, call, or other page is gone, and it should resume." % st
    )

def _video_judge(before, now):
    err = now.get("video_error", "")
    if err and err != before.get("video_error", ""):
        raise RuntimeError("Echo video error: " + err)
    if now.get("video_state") == "playing" and not _lens_cover_closed(now.get("lens_cover")):
        return "playing on the Echo screen (video_state=playing)"
    return None

def _echo_ssh(remote, timeout=12):
    """Run one read-only command on the Echo as root. Never logs or returns the private key."""
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "IdentitiesOnly=yes",
        "-o", "ConnectTimeout=8",
        "-o", "StrictHostKeyChecking=yes",
        "-i", str(SSH_KEY),
        f"root@{HOST}",
        remote,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "ssh failed").strip().replace(str(SSH_KEY), "ECHO_SSH_KEY")
        raise RuntimeError(f"Echo SSH failed ({r.returncode}): {err[:500]}")
    return r.stdout

def _echo_video_cover_reason():
    """From the Echo log, whether the current/last video is still paused under a cover."""
    out = _echo_ssh(
        "echo UP:$(cut -d' ' -f1 /proc/uptime); "
        "grep 'video: ' /data/techo5-linux/techo5.log | tail -n 30"
    )
    lines = [ln for ln in out.splitlines() if ln.strip() and not ln.startswith("UP:")]
    if not lines:
        return None
    # Walk from the most recent "video: starting"; if we then see covered without a later
    # playing / uncovered / over for that start, the clip is still paused under a cover.
    start_at = None
    covered = False
    for ln in lines:
        if "video: starting" in ln:
            start_at = ln
            covered = False
        elif start_at is not None and "video: covered; paused under it" in ln:
            covered = True
        elif start_at is not None and (
            "video: playing" in ln or "video: uncovered; going on" in ln or "video: over" in ln
        ):
            covered = False
    return "log" if covered else None

async def _play_video_confirmed(url, title):
    """Play a video and confirm Echo state. 'covered; paused under it' is its own status, not a failure to confirm."""
    # A new non-relay video must tear down the LAN video relay; a relay URL is this play.
    if not video_relay.is_relay_url(url):
        await asyncio.to_thread(video_relay.stop, _log)
    if _vision_held and not _is_vision_url(url):
        await _vision_release(stop_echo_video=False)
    last = {}
    async with echo() as (c, ents, svcs):
        ids = [i for i in ("video_state", "video_error", "lens_cover") if i in ents]
        if "video_state" not in ids:
            raise RuntimeError("Echo has no video_state entity")
        keys = {ents[i].key: i for i in ids}
        now, changed = {}, asyncio.Event()
        def on(s):
            if s.key in keys:
                now[keys[s.key]] = s.state
                changed.set()
        c.subscribe_states(on)
        end = time.monotonic() + 3
        while len(now) < len(keys) and time.monotonic() < end:
            changed.clear()
            try:
                await asyncio.wait_for(changed.wait(), end - time.monotonic())
            except asyncio.TimeoutError:
                break
        before = dict(now)
        r = c.execute_service(svcs["play_video"], {"url": url, "title": title})
        if asyncio.iscoroutine(r):
            await r
        end = time.monotonic() + VIDEO_CONFIRM_S
        paused_since = None
        cover_reason = None
        while True:
            last.clear()
            last.update(now)
            verdict = _video_judge(before, now)
            if verdict:
                return verdict
            st = now.get("video_state")
            if st == "paused":
                if paused_since is None:
                    paused_since = time.monotonic()
                    if _lens_cover_closed(now.get("lens_cover")):
                        cover_reason = "lens"
                    else:
                        try:
                            cover_reason = await asyncio.to_thread(_echo_video_cover_reason)
                        except Exception as e:
                            _log("video-cover-log", e)
                            cover_reason = None
                elif (
                    time.monotonic() - paused_since >= COVER_SETTLE_S
                    and (cover_reason or _lens_cover_closed(now.get("lens_cover")))
                ):
                    if _lens_cover_closed(now.get("lens_cover")):
                        cover_reason = "lens"
                    return _covered_status(now, cover_reason or "log")
            else:
                paused_since = None
                cover_reason = None
            left = end - time.monotonic()
            if left <= 0:
                break
            changed.clear()
            try:
                await asyncio.wait_for(changed.wait(), min(left, 1.0))
            except asyncio.TimeoutError:
                pass
    # Timed out still not playing. Prefer the cover status over a generic "not confirmed".
    if last.get("video_state") in ("paused", "loading", "playing", "asking"):
        if _lens_cover_closed(last.get("lens_cover")):
            return _covered_status(last, "lens")
        try:
            if await asyncio.to_thread(_echo_video_cover_reason):
                return _covered_status(last, "log")
        except Exception as e:
            _log("video-cover-log", e)
        return "on the Echo screen (video_state=%s; play not confirmed as playing within %ds)" % (
            last.get("video_state"), VIDEO_CONFIRM_S)
    raise RuntimeError("not confirmed by the Echo within %ds: %s" % (
        VIDEO_CONFIRM_S, {k: str(v) for k, v in last.items()}))

def _audio_judge(before, now):
    st = now.get("speaker")
    if st is not None and getattr(st, "name", str(st)) == "PLAYING":
        return "playing on the Echo speaker (speaker=PLAYING)"
    return None

async def _play_audio_confirmed(url):
    def act(c, ents, svcs):
        c.media_player_command(ents["speaker"].key, media_url=url)
    return await act_and_confirm(act, ["speaker"], _audio_judge)

def _picture_check(url):
    # The Echo fetches pictures after notify_picture returns and reports a failed fetch only on its screen
    # and in its log, never over ESPHome. So the address is fetched here first, as the Echo's Go client
    # would ask for it, and a refusal or a non-image is the error.
    req = urllib.request.Request(url, headers={"User-Agent": "Go-http-client/1.1"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            ctype = r.headers.get("Content-Type", "")
            r.read(65536)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"picture not fetchable: HTTP {e.code} {e.reason} from {urllib.parse.urlsplit(url).netloc}")
    if not ctype.startswith("image/"):
        raise RuntimeError(f"picture URL is not an image (Content-Type {ctype!r})")

async def _show_picture_checked(url, message, title, seconds):
    await asyncio.to_thread(_picture_check, url)
    await call("notify_picture", message=message, title=title, picture=url, chime="off", speak="off", sound="off", tag="", seconds=seconds)
    return "picture sent to the Echo (address checked fetchable; the Echo gives no picture-shown signal)"

class _LookPhotoHandler(BaseHTTPRequestHandler):
    """Serve one in-memory JPEG at a one-time path, then 404."""

    def log_message(self, fmt, *args):
        _log("look-http", fmt % args)

    def _slot(self):
        return self.server.slot

    def do_HEAD(self):
        self._reply(body=False)

    def do_GET(self):
        self._reply(body=True)

    def _reply(self, body):
        slot = self._slot()
        with slot["lock"]:
            data = slot["data"]
            path = slot["path"]
        if data is None or self.path != path:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "image/jpeg")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        if body:
            self.wfile.write(data)
            with slot["lock"]:
                slot["fetches"] += 1
                if slot["fetches"] >= 2:
                    slot["data"] = None
                    slot["echo_fetched"].set()


@asynccontextmanager
async def _lan_jpeg(jpeg):
    """Serve jpeg from memory on LOOK_BIND:LOOK_PORT at a random one-time path.

    Dropped after the Echo has fetched it (the second GET: the first is
    _picture_check) or after LOOK_SERVE_S seconds. Bind is the LAN address only.
    """
    if not jpeg or jpeg[:2] != b"\xff\xd8":
        raise RuntimeError("camera did not return a JPEG")
    path = "/look/" + secrets.token_urlsafe(32) + ".jpg"
    slot = {
        "path": path,
        "data": jpeg,
        "fetches": 0,
        "lock": threading.Lock(),
        "echo_fetched": threading.Event(),
        "started": time.monotonic(),
    }
    class _LookHTTPServer(HTTPServer):
        allow_reuse_address = True

    try:
        httpd = _LookHTTPServer((LOOK_BIND, LOOK_PORT), _LookPhotoHandler)
    except OSError as e:
        raise RuntimeError(f"cannot bind look photo server on {LOOK_BIND}:{LOOK_PORT}: {e}")
    httpd.slot = slot
    thread = threading.Thread(target=httpd.serve_forever, name="look-photo", daemon=True)
    thread.start()

    def expire():
        if slot["echo_fetched"].wait(LOOK_SERVE_S):
            return
        with slot["lock"]:
            slot["data"] = None

    exp = threading.Thread(target=expire, name="look-photo-expire", daemon=True)
    exp.start()
    url = f"http://{LOOK_BIND}:{LOOK_PORT}{path}"
    try:
        yield slot, url
    finally:
        with slot["lock"]:
            slot["data"] = None
        slot["echo_fetched"].set()
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


async def _wait_echo_fetched(slot):
    left = LOOK_SERVE_S - (time.monotonic() - slot["started"])
    if left <= 0:
        raise RuntimeError("photo expired before the Echo fetched it")
    ok = await asyncio.to_thread(slot["echo_fetched"].wait, left)
    if not ok:
        raise RuntimeError(f"Echo did not fetch the photo within {LOOK_SERVE_S}s")


class DellMediaError(RuntimeError):
    """A Dell media-gateway HTTP error. code is the status; the message is already explicit. No retries."""
    def __init__(self, code, msg):
        self.code = int(code)
        self.msg = msg
        super().__init__(f"Dell media gateway {self.code}: {msg}")


def _media(method, path, body=None, timeout=120):
    req = urllib.request.Request(MEDIA + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Authorization": "Bearer " + os.environ["DELL_LLM_API_KEY"], "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            msg = json.loads(raw)["error"]
        except (ValueError, KeyError, TypeError):
            msg = raw[:300]
        raise DellMediaError(e.code, msg)

def _is_vision_url(url):
    if not url:
        return False
    base = VISION_STREAM.rstrip("/")
    u = url.strip()
    return u == base or u.startswith(base + "?") or u.startswith(base + "#")


def _parse_vision_classes(classes: str | None) -> list[str]:
    """Split a comma-separated class list. Empty or omitted means prompt-free (detect everything).

    Raises on a blank name in the list, bad characters, duplicates, or more than 32 names.
    The word "phone" is stored as "cell phone", which is the detector's name for it.
    """
    if classes is None:
        raw = ""
    elif not isinstance(classes, str):
        raise RuntimeError("classes must be a comma-separated string")
    else:
        raw = classes
    if not raw.strip():
        return []
    names = []
    seen_lower = set()
    for part in raw.split(","):
        name = part.strip()
        if not name:
            raise RuntimeError("classes has an empty name")
        if name.lower() in _VISION_PHONE_ALIASES:
            name = "cell phone"
        if not _VISION_CLASS_RE.fullmatch(name):
            raise RuntimeError(
                "class %r is not allowed: each name must be 1 to 40 letters, digits, spaces, or _'.-" % name
            )
        key = name.lower()
        if key in seen_lower:
            raise RuntimeError("class %r is repeated" % name)
        seen_lower.add(key)
        names.append(name)
    if len(names) > 32:
        raise RuntimeError("classes must list between 1 and 32 names")
    return names


def _vision_start_body(names: list[str]) -> dict:
    """POST /v1/vision/start body: {} is prompt-free; a non-empty list is text mode.

    Do not send "relations"; the Dell default is on, and this bridge leaves that choice alone.
    """
    return {"classes": names} if names else {}


def _vision_request(method, path, body=None, timeout=15):
    """Call the Dell vision control API. Reuses DELL_LLM_API_KEY; never logs or returns it."""
    req = urllib.request.Request(
        VISION + path,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Authorization": "Bearer " + os.environ["DELL_LLM_API_KEY"],
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            if not raw:
                return {}
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            obj = json.loads(raw)
            msg = obj.get("error") or obj.get("last_error") or raw[:300]
        except (ValueError, TypeError, AttributeError):
            msg = raw[:300]
        raise RuntimeError(f"Dell vision {e.code}: {msg}")


def _vision_is_on():
    """Whether a terminator vision session is running, from this process or the Dell worker."""
    if _vision_held:
        return True
    try:
        st = _vision_request("GET", "/v1/vision/status", None, 2)
    except Exception:
        return False
    return isinstance(st, dict) and st.get("state") == "running"


def _parse_snapshot_labels(header) -> list[str]:
    if not header:
        return []
    if isinstance(header, bytes):
        header = header.decode("utf-8", "replace")
    names = []
    seen = set()
    for part in str(header).split(","):
        name = part.strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(name)
    return names


def _vision_snapshot_http_error(code, raw) -> RuntimeError:
    """Spoken errors for snapshot failures. Never falls back to a camera still."""
    if code == 409:
        return RuntimeError("Terminator vision is not running, so I can't take that picture")
    if code == 422:
        return RuntimeError("I can't see any objects right now")
    if code == 400:
        return RuntimeError("the vision snapshot format was rejected")
    if code == 504:
        return RuntimeError("the vision snapshot timed out")
    msg = raw[:300]
    try:
        obj = json.loads(raw)
        msg = obj.get("error") or obj.get("last_error") or msg
    except (ValueError, TypeError, AttributeError):
        pass
    return RuntimeError("Dell vision %s: %s" % (code, msg))


def _vision_snapshot(timeout=10):
    """POST /v1/vision/snapshot?format=jpeg. Returns (jpeg_bytes, labels). Never logs the key."""
    req = urllib.request.Request(
        VISION + "/v1/vision/snapshot?format=jpeg",
        method="POST",
        data=b"",
        headers={"Authorization": "Bearer " + os.environ["DELL_LLM_API_KEY"]},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            jpeg = r.read()
            labels = _parse_snapshot_labels(r.headers.get("X-Vision-Labels"))
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        raise _vision_snapshot_http_error(e.code, raw) from e
    except urllib.error.URLError as e:
        raise RuntimeError("the vision snapshot timed out") from e
    if not jpeg or jpeg[:2] != b"\xff\xd8":
        raise RuntimeError("the vision snapshot was not a JPEG")
    if len(jpeg) > _IMAGE_MAX_DECODED:
        raise RuntimeError("the vision snapshot is larger than 10 MiB")
    return jpeg, labels


def _wait_rtsp_listening(host, port=RTSP_PORT, timeout=RTSP_READY_TIMEOUT_S, connect=None, sleep=None):
    """Poll until the Echo RTSP port accepts a TCP connection. Fail loudly if it never opens.

    This is a readiness wait, not a retry of /v1/vision/start. A refused port is not treated
    as success; the later start is called only after the port is open.
    """
    connect = connect or socket.create_connection
    sleep = sleep or time.sleep
    deadline = time.monotonic() + float(timeout)
    last_err = None
    attempts = 0
    t0 = time.monotonic()
    while True:
        left = deadline - time.monotonic()
        attempts += 1
        try:
            sock = connect((host, int(port)), timeout=min(1.0, max(0.05, left if left > 0 else 0.05)))
            try:
                sock.close()
            except Exception:
                pass
            waited = time.monotonic() - t0
            _log("vision", "rtsp listening on port %s after %.2fs (%s attempt%s)" % (
                port, waited, attempts, "" if attempts == 1 else "s",
            ))
            return waited
        except OSError as e:
            last_err = e
        if deadline - time.monotonic() <= 0:
            break
        sleep(min(RTSP_READY_POLL_S, max(0.0, deadline - time.monotonic())))
    raise RuntimeError(
        "Echo camera RTSP port %s:%s did not open within %ss after camera_streaming was turned on (%s). "
        "The stream password may not be set, or the camera stream failed to start."
        % (host, port, int(timeout), last_err or "no connection attempt")
    )


async def _camera_streaming_state(wanted=None, timeout=5):
    """Read the Echo camera_streaming switch, or set it and wait until it matches. Never starts the camera by itself."""
    async with echo() as (c, ents, _):
        if "camera_streaming" not in ents:
            raise RuntimeError("Echo has no camera_streaming switch")
        ent = ents["camera_streaming"]
        now = {}
        changed = asyncio.Event()

        def on(s):
            if s.key == ent.key:
                now["state"] = bool(getattr(s, "state", False))
                changed.set()

        c.subscribe_states(on)
        if wanted is not None:
            c.switch_command(ent.key, bool(wanted))
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if "state" in now and (wanted is None or now["state"] is bool(wanted)):
                return now["state"]
            left = end - time.monotonic()
            if left <= 0:
                break
            changed.clear()
            try:
                await asyncio.wait_for(changed.wait(), left)
            except asyncio.TimeoutError:
                break
        if "state" not in now:
            raise RuntimeError("camera_streaming state was not reported by the Echo")
        if wanted is not None and now["state"] is not bool(wanted):
            raise RuntimeError(
                "camera_streaming is %s after asking for it to be %s"
                % ("on" if now["state"] else "off", "on" if wanted else "off")
            )
        return now["state"]


async def _vision_release(stop_echo_video=False):
    """Stop the Dell worker and turn camera_streaming off. Cleanup on failure or end, not a fallback feed."""
    global _vision_held
    errors = []
    if stop_echo_video:
        try:
            await call("stop_video")
        except Exception as e:
            errors.append("stop_video: %s" % e)
    try:
        await asyncio.to_thread(_vision_request, "POST", "/v1/vision/stop", None, 15)
    except Exception as e:
        errors.append("worker stop: %s" % e)
    try:
        off = await _camera_streaming_state(False)
        if off:
            errors.append("camera_streaming is still on")
    except Exception as e:
        errors.append("camera_streaming: %s" % e)
    _vision_held = False
    _vision_watchdog_cancel()
    return errors


_VIDEO_EVENT_RE = re.compile(r"\bvideo: (starting|over|stopped) id=(\d+)\b(.*)$")
_VIDEO_URL_RE = re.compile(r"\burl=(\S+)")


def _echo_vision_playing_from_log(text=None):
    """True if the Echo's latest video is vision.ts and has not ended, False if it ended or is another
    video, None if the log has no start line.

    Only start and end lines count ("video: starting|over|stopped id=N"), paired by id: the latest
    "starting" decides, and an "over"/"stopped" with the same id after it means it ended. The URL is
    taken only from the starting line. Progress lines ("video: playing id=N shown=...") and
    covered/uncovered lines never count; they carry no URL and used to read as "not vision" and
    trigger a false teardown 15 s later (2026-11-14 10:55 and 11:00)."""
    if text is None:
        text = _echo_ssh(
            'grep -E "video: (starting|over|stopped) id=" /data/techo5-linux/techo5.log | tail -n 20'
        )
    ended = set()
    for ln in reversed(text.splitlines()):
        m = _VIDEO_EVENT_RE.search(ln)
        if not m:
            continue
        kind, vid, rest = m.groups()
        if kind != "starting":
            ended.add(vid)
            continue
        if vid in ended:
            return False
        u = _VIDEO_URL_RE.search(rest)
        return bool(u) and _is_vision_url(u.group(1))
    return None


def _showing_vision_ts(sensors, playing_from_log):
    """Whether the Echo is still showing the terminator vision stream."""
    if playing_from_log is True:
        return True
    if playing_from_log is False:
        return False
    state = (sensors.get("video_state") or "").strip().lower()
    title = sensors.get("video_title") or ""
    if state in ("", "idle", "stopped", "unknown"):
        return False
    return title == VISION_TITLE


async def _echo_video_sensor_snapshot(timeout=5):
    """Read video_state and video_title from the Echo. Does not change the screen."""
    async with echo() as (c, ents, _):
        ids = [i for i in ("video_state", "video_title") if i in ents]
        if "video_state" not in ids:
            raise RuntimeError("Echo has no video_state entity")
        keys = {ents[i].key: i for i in ids}
        now, changed = {}, asyncio.Event()

        def on(s):
            if s.key in keys:
                now[keys[s.key]] = s.state
                changed.set()

        c.subscribe_states(on)
        end = time.monotonic() + timeout
        while "video_state" not in now and time.monotonic() < end:
            changed.clear()
            left = end - time.monotonic()
            if left <= 0:
                break
            try:
                await asyncio.wait_for(changed.wait(), left)
            except asyncio.TimeoutError:
                break
        if "video_state" not in now:
            raise RuntimeError("video_state was not reported by the Echo")
        return {
            "video_state": now.get("video_state") or "",
            "video_title": now.get("video_title") or "",
        }


async def _echo_vision_snapshot():
    """video_state, title, and whether vision.ts is on screen (log first, then the title)."""
    sensors = await _echo_video_sensor_snapshot()
    try:
        playing = await asyncio.to_thread(_echo_vision_playing_from_log)
    except Exception as e:
        _log("vision-watchdog", "Echo log check failed (%s); using video_state=%s" % (e, sensors.get("video_state")))
        playing = None
    return {
        "video_state": sensors.get("video_state") or "",
        "video_title": sensors.get("video_title") or "",
        "showing_vision": _showing_vision_ts(sensors, playing),
    }


def _vision_watchdog_cancel():
    """Stop the watcher task. Safe to call from the task itself."""
    global _vision_watchdog_task
    t = _vision_watchdog_task
    _vision_watchdog_task = None
    try:
        current = asyncio.current_task()
    except RuntimeError:
        current = None
    if t is not None and t is not current and not t.done():
        t.cancel()


async def _vision_watchdog_loop(
    poll_s=_VISION_WATCHDOG_POLL_S,
    absent_s=_VISION_WATCHDOG_ABSENT_S,
    sleep=None,
    clock=None,
    echo_snap=None,
    dell_status=None,
    release=None,
):
    """While vision is held, poll the Echo and the Dell worker. If vision.ts is gone for absent_s, full off.

    Full off is _vision_release(stop_echo_video=False): stop the worker and camera, do not touch Echo video
    (it may already be idle, or showing something else). No fallback feed. Failures are logged loudly.
    """
    sleep = sleep or asyncio.sleep
    clock = clock or time.monotonic
    echo_snap = echo_snap or _echo_vision_snapshot
    release = release or (lambda **k: _vision_release(**k))

    async def _status():
        if dell_status is not None:
            return await dell_status()
        return await asyncio.to_thread(_vision_request, "GET", "/v1/vision/status", None, 5)

    missing_since = None
    while _vision_held:
        try:
            echo = await echo_snap()
            dell = await _status()
        except Exception as e:
            _log("vision-watchdog", "poll failed (%s); will retry, not tearing down on a failed poll" % e)
            await sleep(poll_s)
            continue
        if not isinstance(echo, dict):
            echo = {}
        if not isinstance(dell, dict):
            dell = {}
        showing = bool(echo.get("showing_vision"))
        state = echo.get("video_state")
        title = echo.get("video_title")
        clients = dell.get("stream_clients")
        if showing:
            if missing_since is not None:
                _log(
                    "vision-watchdog",
                    "vision.ts is on the Echo again (video_state=%s, stream_clients=%s)"
                    % (state, clients),
                )
            missing_since = None
            await sleep(poll_s)
            continue
        now = clock()
        if missing_since is None:
            missing_since = now
            _log(
                "vision-watchdog",
                "Echo is not showing vision.ts (video_state=%s, title=%r, stream_clients=%s); "
                "will do a full off if this lasts %.0fs"
                % (state, title, clients, absent_s),
            )
        elapsed = now - missing_since
        if elapsed >= absent_s:
            _log(
                "vision-watchdog",
                "tearing down: Echo has not shown vision.ts for %.1fs "
                "(video_state=%s, title=%r, stream_clients=%s). Full off; no fallback."
                % (elapsed, state, title, clients),
            )
            errors = await release(stop_echo_video=False)
            if errors:
                _log("vision-watchdog", "full off had problems: %s" % "; ".join(errors))
            return
        await sleep(min(poll_s, absent_s - elapsed))


def _vision_watchdog_lock_holder():
    """The pid written in the lock file by its owner, or '?' if it cannot be read."""
    try:
        return _VISION_WATCHDOG_LOCK_PATH.read_text().strip() or "?"
    except OSError:
        return "?"


def _vision_watchdog_lock_acquire():
    """Take the machine-wide watchdog lock (exclusive, non-blocking flock). True if this process owns it.

    False if another process holds it. OSError (cannot open the lock file) is raised to the caller."""
    global _vision_watchdog_lock_fd
    if _vision_watchdog_lock_fd is not None:
        return True
    fd = os.open(str(_VISION_WATCHDOG_LOCK_PATH), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return False
    except BaseException:
        os.close(fd)
        raise
    os.ftruncate(fd, 0)
    os.write(fd, ("%d\n" % os.getpid()).encode())
    _vision_watchdog_lock_fd = fd
    return True


def _vision_watchdog_lock_release():
    """Give the watchdog lock back (closing the fd drops the flock)."""
    global _vision_watchdog_lock_fd
    fd = _vision_watchdog_lock_fd
    _vision_watchdog_lock_fd = None
    if fd is None:
        return
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _vision_watchdog_task_done(task):
    """Release the lock when the watching task ends, unless a newer task in this process took over."""
    current = _vision_watchdog_task
    if current is None or current is task or current.done():
        _vision_watchdog_lock_release()


async def _vision_watchdog_ensure():
    """Start the watcher if vision is held, it is not already running, and this process owns the lock.

    Exactly one echo MCP process on this machine runs the watchdog. A process that cannot take the
    lock does not watch, and says so in the log."""
    global _vision_watchdog_task
    if not _vision_held:
        return None
    t = _vision_watchdog_task
    if t is not None and not t.done():
        return t
    try:
        owned = _vision_watchdog_lock_acquire()
    except OSError as e:
        _log(
            "vision-watchdog",
            "ERROR: cannot open the watchdog lock %s (%s); this process (pid %d) is NOT watching"
            % (_VISION_WATCHDOG_LOCK_PATH, e, os.getpid()),
        )
        return None
    if not owned:
        _log(
            "vision-watchdog",
            "another echo MCP process (pid %s) owns the watchdog lock %s; this process (pid %d) is NOT watching"
            % (_vision_watchdog_lock_holder(), _VISION_WATCHDOG_LOCK_PATH, os.getpid()),
        )
        return None
    _log(
        "vision-watchdog",
        "this process (pid %d) owns the watchdog lock %s; watching" % (os.getpid(), _VISION_WATCHDOG_LOCK_PATH),
    )
    task = asyncio.get_running_loop().create_task(_vision_watchdog_loop(), name="vision-watchdog")
    _vision_watchdog_task = task
    task.add_done_callback(_vision_watchdog_task_done)
    return task


async def _vision_adopt_running_session():
    """If the Dell worker is already running, take ownership and start the watcher."""
    global _vision_held
    try:
        st = await asyncio.to_thread(_vision_request, "GET", "/v1/vision/status", None, 5)
    except Exception as e:
        _log("vision-watchdog", "startup status failed (%s); not adopting" % e)
        return False
    if not isinstance(st, dict) or st.get("state") != "running":
        return False
    _vision_held = True
    _log(
        "vision-watchdog",
        "adopted a running Dell worker (stream_clients=%s); watching for the Echo leaving vision.ts"
        % st.get("stream_clients"),
    )
    await _vision_watchdog_ensure()
    return True


def _vision_atexit():
    """Best-effort cleanup if the MCP process ends while vision is held."""
    global _vision_held
    if not _vision_held:
        return
    try:
        _vision_request("POST", "/v1/vision/stop", timeout=8)
    except Exception as e:
        _log("vision-atexit-stop", type(e).__name__)
    try:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_camera_streaming_state(False, timeout=5))
        finally:
            loop.close()
    except Exception as e:
        _log("vision-atexit-switch", type(e).__name__)
    _vision_held = False


atexit.register(_vision_atexit)

JOBS = set()
_last_video = None
_last_video_by_session = {}

def _log(*a):
    print("[echo-bridge]", *a, file=sys.stderr, flush=True)

def _job_is_rendering(j):
    """True once the Dell job has left the queue and is actually rendering.

    Queue wait (status queued/waiting/accepted/pending, or waiting_for set) is not rendering
    and must not consume the render timeout.
    """
    status = (j.get("status") or "").lower()
    if status in ("done", "failed"):
        return False
    if status in ("queued", "waiting", "accepted", "pending"):
        return False
    if j.get("waiting_for"):
        return False
    return True


def _queue_cap_s(j=None):
    """How long we will wait in the queue: 30 min at the head, times position or the max queue."""
    pos = None
    if j:
        pos = j.get("queue_position", j.get("position"))
    try:
        pos = int(pos)
    except (TypeError, ValueError):
        pos = None
    slots = pos if pos is not None and pos > 0 else QUEUE_MAX
    return QUEUE_HEAD_WAIT_S * slots


async def _wait_job(kind, job_id, timeout, *, poll_s=5, queue_cap_s=None, nowfn=time.monotonic):
    """Poll a Dell media job. The render timeout starts when rendering starts, not while queued.

    Queue wait honours queue_position and waiting_for, capped at 30 min times position (or the
    max queue of 10 if position is unknown). A failed or lost job still fails loudly.
    """
    started = nowfn()
    render_deadline = None
    effective_queue_cap = queue_cap_s if queue_cap_s is not None else _queue_cap_s()
    queue_deadline = started + effective_queue_cap
    while True:
        await asyncio.sleep(poll_s)
        j = await asyncio.to_thread(_media, "GET", f"/v1/{kind}/{job_id}")
        if j["status"] == "done":
            return j
        if j["status"] == "failed":
            raise RuntimeError(j.get("error") or "failed")
        now = nowfn()
        if _job_is_rendering(j):
            if render_deadline is None:
                render_deadline = now + timeout
            if now > render_deadline:
                raise RuntimeError("still %s after %ds" % (j["status"], timeout))
        else:
            cap = queue_cap_s if queue_cap_s is not None else _queue_cap_s(j)
            queue_deadline = started + cap
            if now > queue_deadline:
                raise RuntimeError("still %s after %ds in the queue" % (j["status"], int(now - started)))

def _last_video_check(url):
    req = urllib.request.Request(url, method="GET",
                                 headers={"Authorization": "Bearer " + os.environ["DELL_LLM_API_KEY"]})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            r.read(1024)
            if r.status != 200:
                raise RuntimeError("last video is not fetchable: HTTP %s from %s" % (r.status, urllib.parse.urlsplit(url).netloc))
    except urllib.error.HTTPError as e:
        if e.code in (404, 410):
            raise RuntimeError("last video has expired: HTTP %s from %s" % (e.code, urllib.parse.urlsplit(url).netloc))
        raise RuntimeError("last video is not fetchable: HTTP %s from %s" % (e.code, urllib.parse.urlsplit(url).netloc))
    except urllib.error.URLError as e:
        raise RuntimeError("last video is not fetchable: %s" % e.reason)

# --- Long-running jobs: say "working on it" at once, deliver the result where it was asked for -------------
# Measured 16 Nov: Echo voice turns are cut at 60 s, and the model's FIRST call alone took 42.8 s (12:39) and
# 59.6 s (12:56) whenever the Dell's slot-0 prompt cache was cold, so a reply written by the model after the
# tool cannot reliably beat the cut. On an Echo turn the bridge itself speaks the acknowledgement through the
# Echo's notify service the moment the tool runs, independent of the voice turn. On a chat turn the tool
# result carries the acknowledgement for the model's reply. When the job finishes, the result goes back to
# where it was asked: the Echo plays or speaks it; a live Hermex/TUI chat gets a queued notice through the
# echo-job-notify Hermes plugin (ctx.inject_message), which drains CHAT_OUTBOX/pending.
CHAT_OUTBOX = pathlib.Path.home() / ".hermes" / "echo-job-outbox"
# Hermex (gui) and TUI sessions live in the gateway's TUI host, whose session key is the stored session id.
# Chats that can receive a later message, and how the echo-job-notify plugin reaches them:
#   desktop route: Hermex (gui/desktop) sessions live in the dashboard's TUI host, keyed by the session id;
#   gateway route: TUI (`hermes --tui`, stored as tui or local) sessions live in the gateway, keyed by the
#                  stored gateway session key (agent:main:local:dm:<id>).
_DESKTOP_ROUTED = frozenset({"gui", "desktop"})
_GATEWAY_ROUTED = frozenset({"tui", "local"})
_LIVE_CHAT_SOURCES = _DESKTOP_ROUTED | _GATEWAY_ROUTED


def _stored_session_key(session_id):
    uri = "file:{}?mode=ro".format(urllib.parse.quote(str(_state_db().resolve()), safe="/"))
    con = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        row = con.execute("SELECT session_key FROM sessions WHERE id = ?", (session_id,)).fetchone()
    finally:
        con.close()
    if not row or not row[0]:
        raise RuntimeError(f"session {session_id} has no stored gateway session key; cannot route a later message")
    return row[0]
NOTICE_PREFIX = "[Automatic notice from GlaDOS's background job runner, not typed by Mo]"
RELAY_VERBATIM = (
    "Relay the following reply to Mo verbatim, including every Viewer: and MEDIA: line, "
    "each on its own line. Do not paraphrase, shorten, or add a joke:"
)
VIDEO_ETA_S = 180   # typical_render_s ~100 plus 30-60 s LLM unload/reload; queue wait is extra
LONG_JOB_TIMEOUT_S = 30 * 60  # render timeout, from when the Dell job starts rendering
QUEUE_HEAD_WAIT_S = 1800      # Dell /v1/health queue.wait_limits_s.videos_music_at_head
QUEUE_MAX = 10


def _job_origin(tool_name):
    """Where this tool call came from: {"kind": echo|chat, "session_id", "source", "deliverable"}. Fails closed."""
    sessions = _pending_sessions(tool_name)
    if len(sessions) != 1:
        raise RuntimeError(_ORIGIN_FAIL)
    sid, source = sessions[0]
    kind = _classify_session(sid, source)
    if kind is None:
        raise RuntimeError(_ORIGIN_FAIL)
    source = source or ""
    origin = {"kind": kind, "session_id": sid, "source": source,
              "deliverable": kind == "echo" or source in _LIVE_CHAT_SOURCES}
    if kind == "chat" and source in _GATEWAY_ROUTED:
        origin["session_key"] = _stored_session_key(sid)
    return origin


def _minutes_words(minutes):
    m = max(1, int(round(minutes)))
    return "about a minute" if m == 1 else f"about {m} minutes"


def _wyoming_tts(text):
    """Synthesize text with the Echo's own voice (Wyoming Piper at ECHO_TTS host:port, voice ECHO_TTS_VOICE,
    the same server and voice the Echo uses for replies). Returns (wav_bytes, seconds)."""
    host, port = os.environ["ECHO_TTS"].rsplit(":", 1)
    voice = os.environ["ECHO_TTS_VOICE"]
    body = json.dumps({"text": text, "voice": {"name": voice}}).encode()
    with socket.create_connection((host, int(port)), timeout=15) as sock:
        sock.sendall(json.dumps({"type": "synthesize", "data_length": len(body)}).encode() + b"\n" + body)
        f = sock.makefile("rb")
        fmt, pcm = None, bytearray()
        while True:
            line = f.readline()
            if not line:
                raise RuntimeError(f"TTS server {host}:{port} closed the connection before audio-stop")
            h = json.loads(line)
            data = h.get("data") or {}
            if h.get("data_length"):
                data.update(json.loads(f.read(h["data_length"])))
            payload = f.read(h["payload_length"]) if h.get("payload_length") else b""
            if h["type"] == "audio-start":
                fmt = data
            elif h["type"] == "audio-chunk":
                pcm += payload
            elif h["type"] == "audio-stop":
                break
            elif h["type"] == "error":
                raise RuntimeError(f"TTS server {host}:{port}: {data.get('text')}")
    if not fmt or not pcm:
        raise RuntimeError(f"TTS server {host}:{port} returned no audio")
    if fmt["width"] != 2:
        raise RuntimeError(f"TTS server {host}:{port} sent {fmt['width'] * 8}-bit audio; expected 16-bit")
    # The Echo player refuses Piper's 22050 Hz mono ("cannot play 22050 Hz 1 channel 16 bit audio", 16 Nov),
    # so resample to 48 kHz stereo.
    r = subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "s16le", "-ar", str(fmt["rate"]),
                        "-ac", str(fmt["channels"]), "-i", "pipe:0", "-ar", "48000", "-ac", "2", "-f", "wav", "pipe:1"],
                       input=bytes(pcm), capture_output=True, timeout=30)
    if r.returncode != 0 or not r.stdout:
        raise RuntimeError(f"ffmpeg could not resample the speech: {r.stderr.decode(errors='replace')[:200]}")
    return r.stdout, len(pcm) / (fmt["rate"] * fmt["width"] * fmt["channels"])


@asynccontextmanager
async def _lan_wav(wav):
    """Serve one WAV from memory at a random path on SPEAK_BIND:SPEAK_PORT (LAN address only) while open."""
    from http.server import ThreadingHTTPServer
    path = "/say/" + secrets.token_urlsafe(32) + ".wav"

    class H(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def do_GET(self):
            if self.path != path:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "audio/wav")
            self.send_header("Content-Length", str(len(wav)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(wav)

    class S(ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True
    try:
        httpd = S((SPEAK_BIND, SPEAK_PORT), H)
    except OSError as e:
        raise RuntimeError(f"cannot bind the speech server on {SPEAK_BIND}:{SPEAK_PORT}: {e}")
    t = threading.Thread(target=httpd.serve_forever, name="say-wav", daemon=True)
    t.start()
    try:
        yield f"http://{SPEAK_BIND}:{SPEAK_PORT}{path}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        t.join(timeout=2)


_SPEAK_LOCK = asyncio.Lock()


async def _speak_on_echo(text, seconds=20):
    """Show text as a card on the Echo and say it aloud in the Echo's voice. Returns once it has been said.
    (The notify service's own speak option routes to Home Assistant, which Mo does not run, so the speech is
    synthesized here and played on the Echo speaker.)"""
    await call("notify", message=text, title="GLaDOS", icon="", chime="off", speak="off", tag="", seconds=seconds)
    async with _SPEAK_LOCK:
        wav, secs = await asyncio.to_thread(_wyoming_tts, text)
        async with _lan_wav(wav) as url:
            await _play_audio_confirmed(url)
            await asyncio.sleep(secs + 1.5)
        # The speaker state says PLAYING even when the Echo then refuses the file, so read its log back.
        token = url.rsplit("/", 1)[1]
        log = "/data/techo5-linux/techo5.log"
        seen = await asyncio.to_thread(
            _echo_ssh, f"n=$(grep -n -F -e '{token}' {log} | head -n 1 | cut -d: -f1); "
                       f"[ -n \"$n\" ] && sed -n \"$n,$((n + 8))p\" {log} | grep -v 'wake frames'")
        # The Echo's failure line does not repeat the URL, so judge the lines between start and finish.
        window = seen.split(f"media finished item={url}")[0] if f"media finished item={url}" in seen else seen
        if f"playing media url={url}" not in seen or "playing media failed" in window:
            raise RuntimeError(f"the Echo did not play the speech: {seen.strip()[-300:]}")
    return f"said on the Echo ({secs:.1f} s of speech)"


def _queue_note(resp):
    """A short clause for Mo if the Dell 202 named a queue position or waiting_for. Empty if not."""
    if not isinstance(resp, dict):
        return ""
    bits = []
    pos = resp.get("queue_position", resp.get("position"))
    if pos is not None and pos != "":
        bits.append(f"queue position {pos}")
    waiting = resp.get("waiting_for")
    if waiting:
        bits.append(f"waiting for {waiting}")
    if not bits:
        return ""
    return " You are " + ", ".join(str(b) for b in bits) + "."


async def _acknowledge(origin, what, minutes, extra=""):
    """Acknowledge a long job at once. Returns the instruction for the model's reply.

    extra, if given, is appended to the spoken/replied sentence (for example a queue position).
    """
    when = _minutes_words(minutes)
    extra = (" " + extra.strip()) if (extra or "").strip() else ""
    if origin["kind"] == "echo":
        text = f"Working on it. {what} will be ready in {when}. I'll tell you when it's done.{extra}"
        await _speak_on_echo(text)
        _log("ack", "spoken on the Echo:", text)
        return f'The Echo has already said aloud: "{text}" Reply with just "On it." and call no other tool.'
    if origin["deliverable"]:
        msg = (
            "Working on it. " + what + " will be ready in " + when
            + "; I'll send it here when it's done." + extra
        )
        return "Reply now in one short sentence and call no other tool: " + '"' + msg + '"'
    msg = "Working on it. " + what + " will be ready in " + when + "." + extra
    return (
        "Reply now in one short sentence and call no other tool: " + '"' + msg + '" '
        + "This chat (" + str(origin["source"]) + ") cannot receive a later message, so also tell Mo to ask again then."
    )


def _chat_notice(body, media_paths=()):
    """Chat notice: instruct GlaDOS to relay the job reply to Mo verbatim, then the reply itself.

    Viewer: lines stay in the body. MEDIA paths are appended as MEDIA: lines if not already present.
    """
    lines = (body or "").split("\n")
    seen = {l[len("MEDIA:"):].strip() for l in lines if l.startswith("MEDIA:")}
    extra = [f"MEDIA:{p}" for p in media_paths if p not in seen]
    reply = "\n".join([*lines, *extra]).strip()
    return f"{NOTICE_PREFIX} {RELAY_VERBATIM}\n{reply}"


def _chat_outbox_put(origin, content):
    """Queue a notice for the echo-job-notify plugin to inject into the origin chat session."""
    if origin["kind"] != "chat" or not origin["deliverable"]:
        raise RuntimeError(f"cannot queue a chat notice for a {origin['kind']}/{origin['source']} origin")
    pending = CHAT_OUTBOX / "pending"
    pending.mkdir(parents=True, exist_ok=True)
    name = f"{time.time():.3f}-{secrets.token_hex(4)}.json"
    tmp = CHAT_OUTBOX / f".{name}.tmp"
    if origin["source"] in _DESKTOP_ROUTED:
        route, key = "desktop", origin["session_id"]
    else:
        route, key = "gateway", origin["session_key"]
    tmp.write_text(json.dumps({"route": route, "session_key": key, "content": content, "created_at": time.time()}))
    os.replace(tmp, pending / name)
    return f"queued for chat session {origin['session_id']} via the {route} route ({pending / name})"


async def _deliver(origin, spoken, chat_body, media_paths=()):
    """Completion notice: spoken on the Echo, or queued for the origin chat. Returns how it went."""
    if origin["kind"] == "echo":
        await _speak_on_echo(spoken, 30)
        return "spoken on the Echo"
    if not origin["deliverable"]:
        return f"not delivered: a {origin['source'] or 'this'} chat cannot receive later messages"
    return _chat_outbox_put(origin, _chat_notice(chat_body, media_paths))


_VIDEO_MEDIA_ID_RE = re.compile(r"/media/([0-9a-f]{32})\.mp4(?:\?|#|$)", re.I)


def _video_id_from_url(url):
    """32-hex id from a Dell /media/<id>.mp4 URL, or None if the URL has no such id."""
    m = _VIDEO_MEDIA_ID_RE.search(url or "")
    return m.group(1).lower() if m else None


async def _follow_media_job(dell_kind, dell_job_id, job, on_echo):
    """Wait for a Dell media job in the background. Echo origin: play it there the moment it is ready, and
    record that as the notice. Chat origin: save the file and finish the long job; the notifier sends it."""
    jid, origin = job["job_id"], job["origin"]
    try:
        done = await _wait_job(dell_kind, dell_job_id, LONG_JOB_TIMEOUT_S)
        source_id = _video_id_from_url(done.get("url"))
        if origin["kind"] == "echo":
            how = await on_echo(done["url"])
            await asyncio.to_thread(long_jobs.finish, jid, f"played on the Echo (Dell job {dell_job_id})",
                                    source_video_id=source_id or "")
            await asyncio.to_thread(long_jobs.mark_notified, jid, how)
        else:
            path = await asyncio.to_thread(_save_generated, done["url"])
            await asyncio.to_thread(long_jobs.finish, jid, "", [path], source_video_id=source_id or "")
            how = "file saved; the notifier sends it"
        _log(dell_kind, dell_job_id, "long job", jid, "done", how)
    except Exception as e:
        _log(dell_kind, dell_job_id, "long job", jid, "error", e)
        await asyncio.to_thread(long_jobs.fail, jid, str(e))


async def _start_long_media(dell_kind, dell_job_id, origin, label, eta_min, on_echo, extra=""):
    """Record a long job for a Dell media job, follow it in the background, and acknowledge at once."""
    job = await asyncio.to_thread(long_jobs.new_job, dell_kind, label, origin, eta_min, f"Dell job {dell_job_id}")
    t = asyncio.get_running_loop().create_task(_follow_media_job(dell_kind, dell_job_id, job, on_echo))
    JOBS.add(t)
    t.add_done_callback(JOBS.discard)
    say = await _acknowledge(origin, f"Your {label}", eta_min, extra)
    return job, say


def _media_lines(paths):
    """Hermes MEDIA: lines (the chat renders each file) for absolute file paths that exist."""
    for p in paths:
        if not os.path.isabs(p) or not os.path.isfile(p):
            raise RuntimeError(f"file to send is missing: {p}")
    return "\n".join(f"MEDIA:{p}" for p in paths)


async def _play_local_video(path, title, tool_name):
    """Play a local video file on the Echo screen through the LAN relay (VOD copy), and remember it as this
    conversation's last video so replay_last_video can play it again."""
    url = await asyncio.to_thread(lambda: video_relay.start(path, kind="file", title=title, log=_log))
    played = await _play_video_confirmed(url, title)
    _remember_last_video(tool_name, {"url": url, "title": title, "local_video": path})
    return played


async def _long_job_notify(rec):
    """Completion notice for a finished long job (called once by the notifier), to where it was asked."""
    origin, label = rec["origin"], rec["label"]
    if rec["status"] == "failed":
        return await _deliver(origin, f"Sorry, your {label} failed: {rec['error'][:150]}",
                              rec["error"], rec.get("media") or [])
    offer = " Ask me to show it." if rec["echo_video"] else ""
    summary = rec["summary"] or f"Mo's {label} (job {rec['job_id']}) is ready."
    return await _deliver(origin, f"Your {label} is ready.{offer}",
                          summary, rec["media"])

# Chat-origin generated media lives here (Hermes MEDIA: delivery). Camera look
# photos are never written. Retention is 7 days: enough for Hermex scrollback,
# small files, not camera stills.
CACHE_GENERATED = pathlib.Path.home() / ".hermes" / "cache" / "echo-media" / "generated"
CACHE_LOOK = pathlib.Path.home() / ".hermes" / "cache" / "echo-media" / "look"
CACHE_RETENTION_S = 7 * 24 * 3600
_MAX_SAVE_BYTES = 128 * 1024 * 1024
_ORIGIN_WINDOW_S = 15 * 60
_CHAT_SOURCES = frozenset({"local", "desktop", "tui", "cli", "gui"})
_ORIGIN_FAIL = "cannot tell whether this request came from the chat or the Echo; not delivering"
_SAVE_FAIL = "generated file could not be saved"
_MEDIA_EXTS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp",
    ".mp4", ".webm", ".mov", ".mkv",
    ".mp3", ".wav", ".m4a", ".ogg", ".flac", ".opus",
}
_EXT_BY_CTYPE = {
    "image/png": ".png", "image/jpeg": ".jpg", "image/jpg": ".jpg",
    "image/gif": ".gif", "image/webp": ".webp", "image/bmp": ".bmp",
    "video/mp4": ".mp4", "video/webm": ".webm", "video/quicktime": ".mov",
    "video/x-matroska": ".mkv",
    "audio/mpeg": ".mp3", "audio/mp3": ".mp3", "audio/wav": ".wav",
    "audio/x-wav": ".wav", "audio/mp4": ".m4a", "audio/aac": ".m4a",
    "audio/ogg": ".ogg", "audio/flac": ".flac", "audio/opus": ".opus",
}


def _state_db():
    return pathlib.Path(os.environ.get("ECHO_ORIGIN_STATE_DB") or str(pathlib.Path.home() / ".hermes" / "state.db"))


def _tool_names(tool_name):
    short = tool_name.rsplit("__", 1)[-1]
    return {short, f"mcp__echo__{short}"}


def _classify_session(session_id, source):
    source = (source or "").strip().lower()
    sid = session_id or ""
    if sid.startswith("api-") or source == "api_server":
        return "echo"
    if sid.startswith("local-") or source in _CHAT_SOURCES:
        return "chat"
    return None


class _SameHostRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if urllib.parse.urlsplit(newurl).hostname != _media_host:
            raise RuntimeError(_SAVE_FAIL)
        return urllib.request.HTTPRedirectHandler.redirect_request(
            self, req, fp, code, msg, headers, newurl)


def _pending_origins(tool_name, now=None, db_path=None):
    """Origins of in-window assistant tool_calls for this tool with no tool result yet."""
    want = _tool_names(tool_name)
    cutoff = (now if now is not None else time.time()) - _ORIGIN_WINDOW_S
    db = pathlib.Path(db_path) if db_path else _state_db()
    if not db.is_file():
        raise RuntimeError(_ORIGIN_FAIL)
    uri = "file:{}?mode=ro".format(urllib.parse.quote(str(db.resolve()), safe="/"))
    try:
        con = sqlite3.connect(uri, uri=True, timeout=5)
    except sqlite3.Error:
        raise RuntimeError(_ORIGIN_FAIL)
    try:
        rows = con.execute(
            "SELECT m.session_id, m.tool_calls, s.source "
            "FROM messages m JOIN sessions s ON s.id = m.session_id "
            "WHERE m.role = 'assistant' AND m.active = 1 "
            "AND m.timestamp >= ? AND m.tool_calls IS NOT NULL AND length(m.tool_calls) > 2",
            (cutoff,),
        ).fetchall()
        origins = []
        for session_id, raw, source in rows:
            try:
                calls = json.loads(raw) if isinstance(raw, str) else raw
            except (ValueError, TypeError):
                continue
            if not isinstance(calls, list):
                continue
            for call in calls:
                if not isinstance(call, dict):
                    continue
                fn = call.get("function") if isinstance(call.get("function"), dict) else {}
                name = fn.get("name") or call.get("name") or ""
                if name not in want:
                    continue
                call_id = call.get("call_id") or call.get("id") or ""
                if not call_id:
                    continue
                done = con.execute(
                    "SELECT 1 FROM messages WHERE session_id = ? AND role = 'tool' "
                    "AND active = 1 AND tool_call_id = ? LIMIT 1",
                    (session_id, call_id),
                ).fetchone()
                if done:
                    continue
                origins.append(_classify_session(session_id, source))
        return origins
    except sqlite3.Error:
        raise RuntimeError(_ORIGIN_FAIL)
    finally:
        con.close()


def _turn_origin(tool_name):
    """'chat' or 'echo' from the pending tool_call row. Fail closed if ambiguous."""
    origins = _pending_origins(tool_name)
    kinds = set(origins)
    if len(kinds) == 1 and kinds != {None}:
        return origins[0]
    raise RuntimeError(_ORIGIN_FAIL)


def _ensure_generated_dir():
    CACHE_GENERATED.mkdir(parents=True, exist_ok=True)
    os.chmod(CACHE_GENERATED, 0o700)
    os.chmod(CACHE_GENERATED.parent, 0o700)


def _ensure_look_dir():
    CACHE_LOOK.mkdir(parents=True, exist_ok=True)
    os.chmod(CACHE_LOOK, 0o700)
    os.chmod(CACHE_LOOK.parent, 0o700)


def _clean_generated(now=None):
    """Delete generated files older than CACHE_RETENTION_S (7 days)."""
    _clean_generated_root(CACHE_GENERATED, now=now)


def _clean_generated_root(root, now=None):
    """Delete regular files under root older than CACHE_RETENTION_S. No symlinks."""
    try:
        root = root.resolve()
    except OSError:
        return
    if not root.is_dir():
        return
    cutoff = (now if now is not None else time.time()) - CACHE_RETENTION_S
    try:
        entries = list(root.iterdir())
    except OSError as e:
        _log("cache-clean", e)
        return
    for p in entries:
        try:
            if p.is_symlink() or not p.is_file():
                continue
            if not str(p.resolve()).startswith(str(root) + os.sep):
                continue
            if p.stat().st_mtime < cutoff:
                p.unlink()
        except OSError as e:
            _log("cache-clean", p.name, e)


def _suffix_for(url, ctype):
    ext = pathlib.Path(urllib.parse.urlsplit(url).path).suffix.lower()
    if ext == ".jpeg":
        ext = ".jpg"
    if ext in _MEDIA_EXTS:
        return ".jpg" if ext == ".jpeg" else ext
    ctype = (ctype or "").split(";", 1)[0].strip().lower()
    if ctype in _EXT_BY_CTYPE:
        return _EXT_BY_CTYPE[ctype]
    raise RuntimeError("generated file type is not a usable image, audio, or video")


def _save_generated(url):
    """Download a Dell media file into the chat cache. Never returns or raises the URL."""
    parts = urllib.parse.urlsplit(url or "")
    if parts.scheme not in ("http", "https") or parts.hostname != _media_host:
        raise RuntimeError(_SAVE_FAIL)
    _ensure_generated_dir()
    _clean_generated()
    opener = urllib.request.build_opener(_SameHostRedirect)
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": "Bearer " + os.environ["DELL_LLM_API_KEY"],
            "User-Agent": "echo-hermes-bridge",
        },
    )
    tmp = None
    try:
        with opener.open(req, timeout=120) as r:
            final = urllib.parse.urlsplit(r.geturl())
            if final.hostname != _media_host:
                raise RuntimeError(_SAVE_FAIL)
            ctype = r.headers.get("Content-Type", "")
            suffix = _suffix_for(r.geturl(), ctype)
            dest = CACHE_GENERATED / (secrets.token_hex(16) + suffix)
            tmp = dest.with_name(dest.name + ".part")
            fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            try:
                written = 0
                while True:
                    chunk = r.read(64 * 1024)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > _MAX_SAVE_BYTES:
                        raise RuntimeError(_SAVE_FAIL)
                    os.write(fd, chunk)
            finally:
                os.close(fd)
            if written < 16:
                raise RuntimeError(_SAVE_FAIL)
            os.rename(str(tmp), str(dest))
            os.chmod(dest, 0o600)
            tmp = None
            return str(dest)
    except RuntimeError:
        raise
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError):
        raise RuntimeError(_SAVE_FAIL)
    finally:
        if tmp is not None:
            try:
                os.unlink(str(tmp))
            except OSError:
                pass


def _save_look_snapshot(jpeg):
    """Write a captured Echo JPEG into the look cache and return its path."""
    if not jpeg or jpeg[:2] != b"\xff\xd8":
        raise RuntimeError("camera did not return a JPEG")
    if len(jpeg) > _IMAGE_MAX_DECODED:
        raise RuntimeError("the camera photo is larger than 10 MiB")
    _ensure_look_dir()
    _clean_generated_root(CACHE_LOOK)
    dest = CACHE_LOOK / (secrets.token_hex(16) + ".jpg")
    tmp = dest.with_name(dest.name + ".part")
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, jpeg)
    finally:
        os.close(fd)
    os.rename(str(tmp), str(dest))
    os.chmod(dest, 0o600)
    return str(dest)


def _remember_last_video(tool_name, rec):
    """Remember a video played on the Echo for this conversation only."""
    global _last_video
    _last_video = rec
    sessions = _pending_sessions(tool_name)
    if len(sessions) == 1:
        _last_video_by_session[sessions[0][0]] = rec


def _last_video_in_session(tool_name="replay_last_video"):
    sessions = _pending_sessions(tool_name)
    if len(sessions) != 1:
        raise RuntimeError("cannot tell which conversation the last video belongs to")
    rec = _last_video_by_session.get(sessions[0][0])
    if not rec:
        raise RuntimeError("no last video in this conversation")
    return rec


@mcp.tool()
async def start_timer(duration: str, label: str = "") -> str:
    """Start a countdown timer on the Echo speaker. duration like '5 minutes', '1h30m', '00:10:00'."""
    return await call("timer_start", True, duration=duration, label=label)

@mcp.tool()
async def list_timers_and_alarms() -> str:
    """List the timers, alarms and reminders on the Echo, with their ids and time left."""
    return await call("alarms_list", True)

@mcp.tool()
async def cancel_timer(id: str) -> str:
    """Cancel an Echo timer by its id from list_timers_and_alarms, or 'all'."""
    return await call("timer_cancel", id=id)

@mcp.tool()
async def set_alarm(time: str, days: str = "once", label: str = "") -> str:
    """Set an alarm on the Echo. time like '07:30' or '7:30 am'. days: 'once', 'today', 'tomorrow', 'weekdays', 'every day', weekday names, or a date YYYY-MM-DD."""
    return await call("alarm_set", time=time, days=days, label=label)

@mcp.tool()
async def set_reminder(time: str, label: str, days: str = "once") -> str:
    """Set a spoken reminder on the Echo. time is a time of day ('18:00') or from now ('in 20 minutes')."""
    return await call("reminder_set", True, time=time, days=days, label=label, ring_on="")

@mcp.tool()
async def delete_alarm(time: str, label: str = "") -> str:
    """Delete Echo alarms/reminders at this time (optionally only with this label)."""
    return await call("alarm_delete", time=time, label=label)

@mcp.tool()
async def stop_ringing() -> str:
    """Stop whatever is ringing or playing on the Echo speaker (alarm, timer, radio, music, podcast, audiobook)."""
    async with echo() as (c, ents, _):
        c.button_command(ents["alarm_stop"].key)
        c.media_player_command(ents["speaker"].key, command=MediaPlayerCommand.STOP)
    return "ok"

@mcp.tool()
async def set_volume(level: int) -> str:
    """Set the Echo speaker volume, 0 to 100."""
    if not 0 <= level <= 100:
        raise ValueError("level must be 0-100")
    async with echo() as (c, ents, _):
        c.media_player_command(ents["speaker"].key, volume=level / 100)
    return "ok"

@mcp.tool()
async def play_audio_url(url: str) -> str:
    """Play an audio stream or file URL on the Echo speaker (e.g. an internet radio stream)."""
    return await _play_audio_confirmed(url)

@mcp.tool()
async def show_on_screen(what: str) -> str:
    """Show a panel on the Echo screen: one of weather, radar, calendar, todo."""
    names = {"weather": "weather_show", "radar": "radar_show", "calendar": "calendar_show", "todo": "todo_show"}
    return await call(names[what])

@mcp.tool()
async def show_message(message: str, title: str = "", speak: str = "", seconds: int = 15) -> str:
    """Show a message card on the Echo screen; optional text to speak aloud."""
    return await call("notify", message=message, title=title, icon="", chime="", speak=speak, tag="", seconds=seconds)

@mcp.tool()
async def show_picture(url: str, message: str = "", title: str = "", seconds: int = 15) -> str:
    """Show a picture full screen on the Echo from an http or https image URL (JPEG/PNG), with optional words over it."""
    return await _show_picture_checked(url, message, title, seconds)


YT_FORMAT = "18/best[ext=mp4][vcodec^=avc1][height<=720]"


def _youtube_target(query: str) -> str:
    q = (query or "").strip()
    if not q:
        raise RuntimeError("query is empty")
    u = urllib.parse.urlsplit(q)
    host = (u.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if u.scheme in ("http", "https") and host:
        if host in ("youtu.be", "youtube.com", "youtube-nocookie.com", "m.youtube.com", "music.youtube.com"):
            return q
        raise RuntimeError(f"not a YouTube URL: {host}")
    return "ytsearch1:" + q


def _youtube_raise(exc: BaseException) -> None:
    msg = str(exc)
    low = msg.lower()
    if "sign in to confirm your age" in low or "age-restricted" in low or "age restricted" in low:
        raise RuntimeError("that YouTube video is age-restricted") from exc
    if "private video" in low:
        raise RuntimeError("that YouTube video is private") from exc
    if "live event" in low or " this live " in low or "is currently live" in low or "premier" in low:
        raise RuntimeError("that YouTube video is live or upcoming, which this tool cannot play") from exc
    if "requested format is not available" in low or "requested format not available" in low:
        raise RuntimeError("no progressive MP4 (H.264, 720p or lower) is available for that YouTube video") from exc
    if "video unavailable" in low or "is not available" in low:
        raise RuntimeError("that YouTube video is unavailable") from exc
    raise RuntimeError(f"YouTube could not be resolved: {msg[:300]}") from exc


def _duration_words(secs) -> str:
    if secs is None:
        return "unknown length"
    try:
        secs = int(secs)
    except (TypeError, ValueError):
        return "unknown length"
    if secs < 0:
        return "unknown length"
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h} hour" + ("" if h == 1 else "s"))
    if m:
        parts.append(f"{m} minute" + ("" if m == 1 else "s"))
    if s or not parts:
        parts.append(f"{s} second" + ("" if s == 1 else "s"))
    return " ".join(parts)


def _youtube_resolve(query: str) -> dict:
    import yt_dlp
    target = _youtube_target(query)
    # android still offers the progressive format 18 (H.264+AAC) that the Echo can play;
    # the default web client often has only DASH video-only streams.
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "format": YT_FORMAT,
        "no_color": True,
        "extractor_args": {"youtube": {"player_client": ["android"]}},
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(target, download=False)
    except Exception as e:
        _youtube_raise(e)
    if not info:
        raise RuntimeError(f"no YouTube video found for {query!r}")
    if "entries" in info:
        entries = [e for e in (info.get("entries") or []) if e]
        if not entries:
            raise RuntimeError(f"no YouTube video found for {query!r}")
        info = entries[0]
    if info.get("is_live") or info.get("live_status") in ("is_live", "is_upcoming", "post_live"):
        raise RuntimeError("that YouTube video is live or upcoming, which this tool cannot play")
    if int(info.get("age_limit") or 0) >= 18:
        raise RuntimeError("that YouTube video is age-restricted")
    avail = info.get("availability")
    if avail in ("needs_auth", "premium_only", "subscriber_only", "unavailable"):
        raise RuntimeError(f"that YouTube video is unavailable ({avail})")
    url = info.get("url") or ""
    ext = (info.get("ext") or "").lower()
    vcodec = info.get("vcodec") or ""
    acodec = info.get("acodec") or ""
    height = info.get("height") or 0
    if not url or ext != "mp4" or vcodec in ("", "none") or acodec in ("", "none"):
        raise RuntimeError("no progressive MP4 (H.264 + AAC, 720p or lower) is available for that YouTube video")
    if not (vcodec.startswith("avc1") or vcodec.startswith("avc") or "h264" in vcodec.lower()):
        raise RuntimeError("no progressive MP4 (H.264 + AAC, 720p or lower) is available for that YouTube video")
    if height and height > 720:
        raise RuntimeError("no progressive MP4 (H.264 + AAC, 720p or lower) is available for that YouTube video")
    return {
        "url": url,
        "title": (info.get("title") or "YouTube").strip(),
        "channel": (info.get("channel") or info.get("uploader") or "").strip(),
        "duration": info.get("duration"),
        "height": height,
    }



@mcp.tool()
async def play_video(url: str, title: str = "") -> str:
    """Play a video full screen on the Echo from a direct http or https URL (MP4, MKV, MPEG-TS or HLS; H.264 at 720p or less plays best). Not YouTube pages."""
    played = await _play_video_confirmed(url, title)
    _remember_last_video("play_video", {"url": url, "title": (title or "video")[:60]})
    return played

@mcp.tool()
async def stop_video() -> str:
    """Stop the video playing on the Echo screen."""
    await asyncio.to_thread(video_relay.stop, _log)
    stopped = await call("stop_video")
    if _vision_held:
        errors = await _vision_release(stop_echo_video=False)
        if errors:
            raise RuntimeError(
                "%s, but terminator vision did not fully stop: %s" % (stopped, "; ".join(errors))
            )
        return "%s. Terminator vision stopped; camera streaming is off" % stopped
    return stopped

@mcp.tool()
async def play_youtube(query: str) -> str:
    """Play a YouTube video on the Echo. query is a YouTube URL or search words.
    Resolves to one H.264 MP4 at 720p or lower and plays it full screen. Fails if the video is age-restricted, live, or has no such file."""
    info = await asyncio.to_thread(_youtube_resolve, query)
    played = await _play_video_confirmed(info["url"], info["title"][:60])
    _remember_last_video("play_youtube", {"url": info["url"], "title": info["title"][:60]})
    _log("youtube", info["title"], played)
    return (
        f"{played}: {info['title']} by {info['channel'] or 'unknown channel'}, "
        f"{_duration_words(info['duration'])}"
    )

@mcp.tool()
async def glados_vision(
    action: Annotated[str, Field(description="on or off")],
    classes: Annotated[str, Field(description=(
        "Comma-separated object names to outline, which narrows the detector to those names. "
        "Leave empty (the default) to detect everything. Use 'cell phone' for a phone; "
        "the word phone is mapped to that."
    ))] = "",
) -> str:
    """Show a live annotated Echo camera feed on the Echo screen, or stop it.
    action is on or off. On: depth is grayscale, white when close and black when far; each object
    gets an outline and label in its own colour by label type (the same name always gets the same
    colour), and relations between objects are drawn as dotted lines in the subject's colour with
    the relation word.
    classes is a comma-separated list of object names. If omitted or empty, the feed looks for
    everything it can recognise. If the user names objects, pass only those names so it narrows
    to them. The word "phone" is sent as "cell phone".
    This feature is called terminator vision; robot vision, computer vision and GlaDOS vision
    are the same thing. Call this with action on when the user says terminator vision, robot vision,
    computer vision, GlaDOS vision, "let's do GlaDOS vision", a depth view, or "show me what you see"
    for the live annotated feed — this is not a software project, and "computer vision" said as a
    command means switch the feed on, not explain the field. Call it with action off for "stop
    terminator vision", "stop robot vision", "stop computer vision", "stop vision" and the like.
    The feed plays only on the Echo. Refused on a chat/Hermex turn. The camera stream is switched
    on only while this is running. If anything fails, the worker is stopped and the camera stream
    is switched off; that is cleanup, not a substitute feed."""
    global _vision_held
    act = (action or "").strip().lower()
    if act not in ("on", "off"):
        raise RuntimeError("action must be 'on' or 'off'")
    if _turn_origin("glados_vision") == "chat":
        raise RuntimeError("The terminator vision feed shows only on the Echo screen")
    if act == "off":
        # Each step runs even if an earlier one fails, so camera_streaming is always switched off.
        problems = []
        video = worker_state = None
        try:
            video = await call("stop_video")
        except Exception as e:
            problems.append("stop_video: %s" % e)
        try:
            await asyncio.to_thread(video_relay.stop, _log)
        except Exception as e:
            problems.append("relay stop: %s" % e)
        try:
            worker = await asyncio.to_thread(_vision_request, "POST", "/v1/vision/stop", None, 15)
            worker_state = worker.get("state") if isinstance(worker, dict) else worker
        except Exception as e:
            problems.append("worker stop: %s" % e)
        cam = await _camera_streaming_state(False)
        _vision_held = False
        _vision_watchdog_cancel()
        if cam:
            problems.append("camera_streaming is still on")
        if problems:
            raise RuntimeError("Vision off had problems: %s" % "; ".join(problems))
        return (
            "Vision off: Echo video %s; worker %s; camera streaming off"
            % (video, worker_state or "stopped")
        )
    names = _parse_vision_classes(classes)
    play_attempted = False
    # Held before the switch is asked to turn on, so a failed, slow or unconfirmed switch-on is
    # still switched off by _vision_release (and by atexit if the process ends).
    _vision_held = True
    try:
        cam = await _camera_streaming_state(True)
        if not cam:
            raise RuntimeError("camera_streaming did not turn on")
        await asyncio.to_thread(_wait_rtsp_listening, HOST, RTSP_PORT, RTSP_READY_TIMEOUT_S)
        started = await asyncio.to_thread(
            _vision_request, "POST", "/v1/vision/start", _vision_start_body(names), VISION_START_TIMEOUT
        )
        state = started.get("state") if isinstance(started, dict) else None
        if state != "running":
            raise RuntimeError("Dell vision did not start (state=%s)" % state)
        play_attempted = True
        played = await _play_video_confirmed(VISION_STREAM, VISION_TITLE)
        await _vision_watchdog_ensure()
        watching = ", ".join(names) if names else "everything"
        return (
            f"{played}. Terminator vision is on, watching {watching}"
        )
    except Exception as e:
        errors = await _vision_release(stop_echo_video=play_attempted)
        if errors:
            raise RuntimeError(
                "Terminator vision failed: %s; cleanup had problems: %s" % (e, "; ".join(errors))
            ) from e
        raise


@mcp.tool()
async def generate_picture(prompt: str, message: str = "", seconds: int = 30) -> str:
    """Generate an image on the Dell (local diffusion model, ~5 s warm, up to ~40 s cold).
    On an Echo turn, show it full screen on the Echo. On a chat turn, send the image to the chat and do not use the Echo."""
    origin = _turn_origin("generate_picture")
    r = await asyncio.to_thread(_media, "POST", "/v1/images", {"prompt": prompt, "width": 960, "height": 480})
    path = await asyncio.to_thread(_save_generated, r["url"])
    if origin == "chat":
        return f"generated in {r.get('seconds')} s; sent to the chat\nMEDIA:{path}\nSAVED:{path}"
    shown = await _show_picture_checked(r["url"], message, "", seconds)
    return f"generated in {r.get('seconds')} s; {shown}\nSAVED:{path}"

_IMAGES_DIR = pathlib.Path.home() / ".hermes" / "images"
# Hermex / desktop native attachments (dashboard_*.png) live here, not under ~/.hermes/images/.
_ATTACH_DIR = pathlib.Path.home() / ".hermes" / "cache" / "images"
_UPLOAD_ROOTS = (_IMAGES_DIR, _ATTACH_DIR)
_IMAGE_MAX_DECODED = 10 * 1024 * 1024  # Dell image_base64 limit (10,485,760)
_UPLOAD_ALIASES = frozenset({
    "upload", "attached", "attachment", "this upload", "the upload",
})
_GENERATED_ALIASES = frozenset({
    "generated", "generated picture",
})
_LOOK_ALIASES = frozenset({
    "look", "capture", "snapshot", "look snapshot", "camera photo", "the look",
})
_LATEST_ALIASES = frozenset({
    "latest", "last", "last picture", "last photo", "the picture", "this picture",
    "that picture", "the photo", "this photo", "that photo", "picture", "photo",
})


def _pending_sessions(tool_name, now=None, db_path=None):
    """Unique (session_id, source) for in-window pending calls of this tool."""
    want = _tool_names(tool_name)
    cutoff = (now if now is not None else time.time()) - _ORIGIN_WINDOW_S
    db = pathlib.Path(db_path) if db_path else _state_db()
    if not db.is_file():
        raise RuntimeError(_ORIGIN_FAIL)
    uri = "file:{}?mode=ro".format(urllib.parse.quote(str(db.resolve()), safe="/"))
    try:
        con = sqlite3.connect(uri, uri=True, timeout=5)
    except sqlite3.Error:
        raise RuntimeError(_ORIGIN_FAIL)
    try:
        rows = con.execute(
            "SELECT m.session_id, m.tool_calls, s.source "
            "FROM messages m JOIN sessions s ON s.id = m.session_id "
            "WHERE m.role = 'assistant' AND m.active = 1 "
            "AND m.timestamp >= ? AND m.tool_calls IS NOT NULL AND length(m.tool_calls) > 2",
            (cutoff,),
        ).fetchall()
        found = []
        seen = set()
        for session_id, raw, source in rows:
            try:
                calls = json.loads(raw) if isinstance(raw, str) else raw
            except (ValueError, TypeError):
                continue
            if not isinstance(calls, list):
                continue
            for call in calls:
                if not isinstance(call, dict):
                    continue
                fn = call.get("function") if isinstance(call.get("function"), dict) else {}
                name = fn.get("name") or call.get("name") or ""
                if name not in want:
                    continue
                call_id = call.get("call_id") or call.get("id") or ""
                if not call_id:
                    continue
                done = con.execute(
                    "SELECT 1 FROM messages WHERE session_id = ? AND role = 'tool' "
                    "AND active = 1 AND tool_call_id = ? LIMIT 1",
                    (session_id, call_id),
                ).fetchone()
                if done:
                    continue
                key = (session_id, source)
                if key not in seen:
                    seen.add(key)
                    found.append(key)
        return found
    except sqlite3.Error:
        raise RuntimeError(_ORIGIN_FAIL)
    finally:
        con.close()


def _under_root(path: pathlib.Path, root: pathlib.Path) -> bool:
    try:
        resolved = path.resolve()
        root = root.resolve()
    except OSError:
        return False
    return str(resolved) == str(root) or str(resolved).startswith(str(root) + os.sep)


def _is_upload_path(path: pathlib.Path) -> bool:
    return any(_under_root(path, root) for root in _UPLOAD_ROOTS)


def _looks_like_user_text(value) -> str:
    """Extract plain text from a stored user message (string or native-vision parts)."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return ""
    if isinstance(value, str):
        raw = value
        stripped = raw.lstrip()
        if stripped.startswith("[") or stripped.startswith("{"):
            try:
                value = json.loads(raw)
            except (ValueError, TypeError):
                return raw
        else:
            return raw
    if isinstance(value, dict):
        value = [value]
    if isinstance(value, list):
        parts = []
        for part in value:
            if isinstance(part, str):
                parts.append(part)
                continue
            if not isinstance(part, dict):
                continue
            for key in ("text", "content"):
                if isinstance(part.get(key), str):
                    parts.append(part[key])
            image = part.get("image_url") or part.get("image") or part.get("url")
            if isinstance(image, dict):
                image = image.get("url") or image.get("path")
            if isinstance(image, str) and image.startswith("/") and not image.startswith("data:"):
                parts.append("image_url: " + image)
        return "\n".join(parts)
    return str(value)


def _iter_turn_image_refs(text: str):
    for raw in re.findall(r"@image:(\S+)", text):
        yield raw
    for raw in re.findall(r"image_url:\s*(\S+)", text):
        yield raw


def _resolve_upload_ref(raw: str) -> pathlib.Path:
    raw = (raw or "").strip().rstrip(".,;)]}\"'~")
    if raw.startswith("file://"):
        raw = urllib.parse.urlparse(raw).path
    p = pathlib.Path(raw).expanduser()
    if not p.is_absolute():
        matches = []
        for root in _UPLOAD_ROOTS:
            cand = root / p.name
            if cand.is_symlink():
                continue
            if cand.is_file() and _under_root(cand, root):
                matches.append(cand.resolve())
        uniq = []
        seen = set()
        for m in matches:
            if str(m) not in seen:
                seen.add(str(m))
                uniq.append(m)
        if len(uniq) == 1:
            return uniq[0]
        if len(uniq) > 1:
            raise RuntimeError("this turn has more than one uploaded photo with that name; say which path to use")
        p = _IMAGES_DIR / p.name
    if p.is_symlink():
        raise RuntimeError("the uploaded photo path cannot be a symlink")
    if not _is_upload_path(p):
        raise RuntimeError("the uploaded photo path is not under the images directory")
    return p


def _turn_upload_paths(tool_name, db_path=None):
    """This turn's uploaded photos from the pending session's latest user message.

    Hermex/desktop attachments are annotated as ``image_url: ~/.hermes/cache/images/...``.
    Some paths still use ``@image:`` under ~/.hermes/images/.
    """
    sessions = _pending_sessions(tool_name, db_path=db_path)
    if len(sessions) != 1:
        raise RuntimeError("cannot tell which chat turn the uploaded photo belongs to")
    session_id, _source = sessions[0]
    db = pathlib.Path(db_path) if db_path else _state_db()
    uri = "file:{}?mode=ro".format(urllib.parse.quote(str(db.resolve()), safe="/"))
    try:
        con = sqlite3.connect(uri, uri=True, timeout=5)
        row = con.execute(
            "SELECT content FROM messages WHERE session_id = ? AND role = 'user' AND active = 1 "
            "ORDER BY timestamp DESC, id DESC LIMIT 1",
            (session_id,),
        ).fetchone()
    except sqlite3.Error:
        raise RuntimeError("cannot read the uploaded photo for this turn")
    finally:
        try:
            con.close()
        except Exception:
            pass
    text = _looks_like_user_text(row[0] if row else "")
    paths = []
    seen = set()
    for raw in _iter_turn_image_refs(text):
        p = _resolve_upload_ref(raw)
        key = str(p.resolve()) if p.exists() else str(p)
        if key in seen:
            continue
        seen.add(key)
        paths.append(p)
    return paths


_SAVED_IMAGE_RE = re.compile(r"(?:MEDIA|SAVED):(/[^\s]+)")


def _parse_saved_images(text, *roots):
    """Unique saved image paths from a tool result, under the given roots. No folder scan."""
    if text is None:
        return []
    if isinstance(text, bytes):
        try:
            text = text.decode("utf-8")
        except UnicodeDecodeError:
            return []
    if not isinstance(text, str):
        text = str(text)
    found = []
    seen = set()
    for raw in _SAVED_IMAGE_RE.findall(text):
        raw = raw.strip().rstrip(".,;)]}\"'~")
        path = pathlib.Path(raw)
        if not path.is_absolute():
            continue
        if path.suffix.lower() not in {".jpg", ".jpeg", ".png"}:
            continue
        if not any(_under_root(path, root) for root in roots):
            continue
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        found.append(path)
    return found


def _parse_saved_generated_images(text):
    """Unique generated image paths from a generate_picture tool result."""
    return _parse_saved_images(text, CACHE_GENERATED)


def _session_id_for_tool(tool_name, db_path=None):
    sessions = _pending_sessions(tool_name, db_path=db_path)
    if len(sessions) != 1:
        raise RuntimeError("cannot tell which session the image belongs to")
    return sessions[0][0]


def _session_image_events(session_id, db_path=None):
    """Chronological image events in this session: upload, look snapshot, or generated picture."""
    db = pathlib.Path(db_path) if db_path else _state_db()
    if not db.is_file():
        raise RuntimeError("cannot read images for this session")
    uri = "file:{}?mode=ro".format(urllib.parse.quote(str(db.resolve()), safe="/"))
    picture_names = _tool_names("generate_picture")
    look_names = _tool_names("look")
    con = None
    try:
        con = sqlite3.connect(uri, uri=True, timeout=5)
        rows = con.execute(
            "SELECT role, tool_name, content, timestamp, id FROM messages "
            "WHERE session_id = ? AND active = 1 "
            "AND (role = 'user' OR (role = 'tool' AND tool_name IS NOT NULL)) "
            "ORDER BY timestamp ASC, id ASC",
            (session_id,),
        ).fetchall()
    except sqlite3.Error:
        raise RuntimeError("cannot read images for this session")
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    events = []
    for role, tool_name, content, ts, mid in rows:
        kind = None
        paths = []
        if role == "user":
            text = _looks_like_user_text(content)
            seen = set()
            for raw in _iter_turn_image_refs(text):
                try:
                    path = _resolve_upload_ref(raw)
                except RuntimeError:
                    continue
                key = str(path.resolve()) if path.exists() else str(path)
                if key in seen:
                    continue
                seen.add(key)
                paths.append(path)
            if paths:
                kind = "upload"
        elif tool_name in picture_names:
            paths = _parse_saved_images(content, CACHE_GENERATED)
            if paths:
                kind = "generated"
        elif tool_name in look_names:
            paths = _parse_saved_images(content, CACHE_LOOK)
            if paths:
                kind = "look"
        if kind:
            events.append((float(ts or 0), int(mid or 0), kind, paths))
    return events


def _pick_session_event(events, kind, missing, ambiguous):
    chosen = None
    for ev in events:
        if kind == "latest" or ev[2] == kind:
            chosen = ev
    if chosen is None:
        raise RuntimeError(missing)
    paths = chosen[3]
    if len(paths) > 1:
        raise RuntimeError(ambiguous)
    path = paths[0]
    if path.is_symlink():
        raise RuntimeError("the image path cannot be a symlink")
    if not path.is_file():
        raise RuntimeError("the image from this session is missing")
    return path


def _session_generated_image(tool_name="generate_video", db_path=None):
    """The picture generated in the same session as this call. No newest-file fallback."""
    events = _session_image_events(_session_id_for_tool(tool_name, db_path=db_path), db_path=db_path)
    return _pick_session_event(
        events, "generated",
        "no generated picture in this session",
        "this session has more than one saved path on the latest generated picture; say which path to use",
    )


def _session_upload_image(tool_name="generate_video", db_path=None):
    """The most recent uploaded photo in this session, not only the current turn."""
    events = _session_image_events(_session_id_for_tool(tool_name, db_path=db_path), db_path=db_path)
    return _pick_session_event(
        events, "upload",
        "no photo was uploaded in this session",
        "the latest upload in this session has more than one photo; say which path to use",
    )


def _session_look_image(tool_name="generate_video", db_path=None):
    """The most recent Echo camera snapshot saved in this session."""
    events = _session_image_events(_session_id_for_tool(tool_name, db_path=db_path), db_path=db_path)
    return _pick_session_event(
        events, "look",
        "no camera photo was taken in this session",
        "the latest camera photo in this session has more than one saved path; say which path to use",
    )


def _session_latest_image(tool_name="generate_video", db_path=None):
    """The most recent image in this session: upload, look snapshot, or generated picture."""
    events = _session_image_events(_session_id_for_tool(tool_name, db_path=db_path), db_path=db_path)
    return _pick_session_event(
        events, "latest",
        "no image is available in this session",
        "the latest image in this session has more than one path; say which path to use",
    )


_TEXT_ALIASES = frozenset({
    "text",
})
_NONE_ALIASES = frozenset({
    "none",
})
_NOT_AUTOMATIC = "The photo you can see in the chat is not passed to this tool automatically; you must name it in image."
_IMAGE_REQUIRED = (
    'image is required; pass image="text" for a prompt-only render, or '
    'image="upload"/"latest"/"look"/"generated" (or its path) for a first-frame image. ' + _NOT_AUTOMATIC
)


def _image_missing_error(tool_name="generate_video", db_path=None) -> RuntimeError:
    """The error for a new clip called without image. Names this turn's attached photo(s) if there are any.

    It only informs the model; it never picks an image. If the turn cannot be identified, the plain
    _IMAGE_REQUIRED text is used.
    """
    try:
        paths = _turn_upload_paths(tool_name, db_path=db_path)
    except RuntimeError:
        paths = []
    if not paths:
        return RuntimeError(_IMAGE_REQUIRED)
    named = ", ".join(str(p) for p in paths)
    which = "an attached photo" if len(paths) == 1 else f"{len(paths)} attached photos"
    return RuntimeError(
        f"image is required; this turn has {which} at {named}. Call again with image=\"upload\" "
        f"(or that exact path) to animate it, or image=\"text\" for a text-only clip. " + _NOT_AUTOMATIC
    )


def _video_image_key(image: str) -> str:
    return re.sub(r"\s+", " ", (image or "").strip()).lower()


def _resolve_video_image(image: str, tool_name="generate_video") -> pathlib.Path | None:
    """Resolve generate_video's first-frame source. image="text" returns None (text-only)."""
    raw = (image or "").strip()
    if not raw:
        raise _image_missing_error(tool_name)
    key = _video_image_key(image)
    if key in _TEXT_ALIASES:
        return None
    if key in _NONE_ALIASES:
        raise RuntimeError('image="none" is only for mode=extend or mode=restyle; a new clip needs "text" or a first-frame image')
    if key in _UPLOAD_ALIASES:
        return _session_upload_image(tool_name)
    if key in _LOOK_ALIASES:
        return _session_look_image(tool_name)
    if key in _LATEST_ALIASES:
        return _session_latest_image(tool_name)
    if key in _GENERATED_ALIASES:
        return _session_generated_image(tool_name)
    if raw.startswith("@image:"):
        raw = raw[len("@image:"):]
    path = pathlib.Path(raw).expanduser()
    if not path.is_absolute():
        raise RuntimeError('the first-frame image must be an absolute path, upload, generated, look, latest, or "text"')
    if path.is_symlink():
        raise RuntimeError("the first-frame image path cannot be a symlink")
    if not (_is_upload_path(path) or _under_root(path, CACHE_GENERATED) or _under_root(path, CACHE_LOOK)):
        raise RuntimeError("the first-frame image must be an uploaded photo, a generated picture, or a camera snapshot")
    if not path.is_file():
        raise RuntimeError("the first-frame image is missing")
    return path


_IMAGE_MAX_SOURCE = 40 * 1024 * 1024  # largest source file the normaliser will open
_IMAGE_JPEG_QUALITY = 95


def _normalize_image_bytes(data: bytes, what: str) -> tuple[bytes, str]:
    """Every image sent to the Dell goes through here. No fallbacks.

    Opens the bytes with Pillow (so MPO phone photos, WebP, GIF, BMP, TIFF and EXIF-rotated JPEGs all work),
    applies the EXIF orientation, keeps only the first frame, and re-encodes: PNG if it has alpha, otherwise
    JPEG at quality 95. EXIF (including GPS) is not copied. Returns (bytes, mime). Anything Pillow cannot
    open, or a result over the Dell's 10 MiB limit, raises a clear error.
    """
    if not data or len(data) < 16:
        raise RuntimeError(f"{what} is empty or not an image")
    if len(data) > _IMAGE_MAX_SOURCE:
        raise RuntimeError(f"{what} is larger than {_IMAGE_MAX_SOURCE // (1024 * 1024)} MiB")
    try:
        with Image.open(io.BytesIO(data)) as src:
            fmt = src.format or "unknown"
            src.seek(0)
            src.load()
            img = ImageOps.exif_transpose(src)
    except UnidentifiedImageError as exc:
        raise RuntimeError(
            f"{what} is not an image format the bridge can open (Pillow could not identify it); "
            "send a JPEG, PNG, WebP or similar"
        ) from exc
    except (OSError, ValueError, SyntaxError, Image.DecompressionBombError) as exc:
        raise RuntimeError(f"{what} could not be decoded: {exc}") from exc
    has_alpha = img.mode in ("RGBA", "LA", "PA", "RGBa", "La") or (
        img.mode == "P" and "transparency" in img.info
    )
    out = io.BytesIO()
    if has_alpha:
        img.convert("RGBA").save(out, format="PNG", optimize=True)
        mime = "image/png"
    else:
        img.convert("RGB").save(out, format="JPEG", quality=_IMAGE_JPEG_QUALITY)
        mime = "image/jpeg"
    result = out.getvalue()
    if len(result) > _IMAGE_MAX_DECODED:
        raise RuntimeError(f"{what} is larger than 10 MiB after re-encoding ({fmt}, {img.width}x{img.height})")
    _log("image", what, f"normalised {fmt} {img.width}x{img.height} -> {mime} {len(result)} bytes")
    return result, mime


def _image_base64(path: pathlib.Path, what: str = "the first-frame image") -> str:
    """Read an image file, normalise it (_normalize_image_bytes) and return raw standard base64. No fallbacks."""
    if path.is_symlink():
        raise RuntimeError(f"{what} path cannot be a symlink")
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise RuntimeError(f"{what} could not be read") from exc
    result, _mime = _normalize_image_bytes(data, what)
    return base64.b64encode(result).decode("ascii")


_TEXT_ONLY_REFUSED = (
    'This turn has an attached photo; pass image="upload" (or its path). '
    "Text-only render refused."
)
def _refuse_text_only_when_attached(image: str, tool_name="generate_video", db_path=None) -> None:
    """Error if this turn has an uploaded photo and generate_video was called with image="text".

    Detection matches the upload resolver: image_url: under ~/.hermes/cache/images/
    or @image: under ~/.hermes/images/ in the current turn's user message. If the turn
    cannot be identified, this does not block a text-only render.
    """
    if _video_image_key(image) not in _TEXT_ALIASES:
        return
    try:
        paths = _turn_upload_paths(tool_name, db_path=db_path)
    except RuntimeError:
        return
    if paths:
        raise RuntimeError(_TEXT_ONLY_REFUSED)



VIDEO_MODES = ("t2v", "i2v", "first_last", "extend", "restyle")
VIDEO_STYLES = ("anime", "claymation", "papercut", "cyberpunk", "painterly")
VIDEO_CONTROLS = ("canny", "depth", "pose")
VIDEO_SOURCE_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_VIDEO_PREVIOUS_ALIASES = frozenset({
    "previous", "last", "last video", "the last video", "the previous video",
})
_VIDEO_MAX_BYTES = 10 * 1024 * 1024  # Dell video_base64 limit
_QUEUE_FULL = (
    "The Dell video queue is full (HTTP 429). Tell Mo the queue is full and to try again later"
)


def _opt_choice(value, allowed, name):
    """Return a lowercased allowed value, or None if omitted. Unknown values fail loudly."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"{name} must be one of {', '.join(allowed)}")
    key = value.strip().lower()
    if not key:
        return None
    if key not in allowed:
        raise RuntimeError(f"{name} must be one of {', '.join(allowed)}, not {value!r}")
    return key


def _opt_int(value, name, lo, hi):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"{name} must be an integer from {lo} to {hi}")
    if value < lo or value > hi:
        raise RuntimeError(f"{name} must be an integer from {lo} to {hi}")
    return value


def _last_generated_video_id(session_id):
    """The 32-hex Dell media id of this session's latest finished generated video."""
    recs = [
        r for r in long_jobs.all_records()
        if r.get("kind") == "videos" and r.get("status") == "done"
        and (r.get("origin") or {}).get("session_id") == session_id
        and r.get("source_video_id")
    ]
    if not recs:
        raise RuntimeError(
            "no previous generated video in this session (or its gateway id was not stored)"
        )
    recs.sort(key=lambda r: r.get("finished_at") or r.get("submitted_at") or 0, reverse=True)
    return recs[0]["source_video_id"]


def _mp4_bytes(path: pathlib.Path) -> bytes:
    if path.is_symlink():
        raise RuntimeError("the source video path cannot be a symlink")
    if not path.is_absolute():
        raise RuntimeError("the source video must be an absolute path, previous, or a 32-hex gateway id")
    if not (_is_upload_path(path) or _under_root(path, CACHE_GENERATED)):
        raise RuntimeError("the source video must be a chat-attached mp4 or a clip generated in this chat")
    if path.suffix.lower() != ".mp4":
        raise RuntimeError("the source video must be an mp4 file")
    if not path.is_file():
        raise RuntimeError("the source video is missing")
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise RuntimeError("the source video could not be read") from exc
    if len(data) > _VIDEO_MAX_BYTES:
        raise RuntimeError("the source video is larger than 10 MiB; pass a shorter clip or a gateway id")
    if len(data) < 16:
        raise RuntimeError("the source video is empty or not an mp4")
    return data


def _resolve_video_source(source: str, session_id: str) -> dict:
    """Turn source=previous, a 32-hex id, a /media/<id>.mp4 URL, or an attached mp4 path into one Dell field."""
    raw = (source or "").strip()
    if not raw:
        raise RuntimeError(
            "source is required for extend and restyle: pass previous, a 32-hex gateway id, or an attached mp4 path"
        )
    key = re.sub(r"\s+", " ", raw).lower()
    if key in _VIDEO_PREVIOUS_ALIASES:
        return {"source_video_id": _last_generated_video_id(session_id)}
    from_url = _video_id_from_url(raw)
    if from_url:
        return {"source_video_id": from_url}
    if VIDEO_SOURCE_ID_RE.fullmatch(raw.lower()) and "/" not in raw and " " not in raw:
        return {"source_video_id": raw.lower()}
    return {"video_base64": base64.b64encode(_mp4_bytes(pathlib.Path(raw).expanduser())).decode("ascii")}


def _end_frame_path(end_image: str, tool_name="generate_video") -> pathlib.Path:
    raw = (end_image or "").strip()
    if not raw or _video_image_key(end_image) in _TEXT_ALIASES:
        raise RuntimeError('end_image is required for first_last; pass latest/upload/look/generated or a path, not "text"')
    path = _resolve_video_image(end_image, tool_name)
    if path is None:
        raise RuntimeError('end_image cannot be "text"; first_last needs a last-frame image')
    return path


def _video_request_body(prompt, image, *, mode=None, end_image=None, style=None, seconds=None,
                        extend_seconds=None, control=None, source=None, session_id=None) -> dict:
    """Build the POST /v1/videos body. Only fields valid for the mode are sent. No silent defaults."""
    if not (prompt or "").strip():
        raise RuntimeError("prompt is required; write a visual description (no silent default)")
    mode = _opt_choice(mode, VIDEO_MODES, "mode")
    style = _opt_choice(style, VIDEO_STYLES, "style")
    control = _opt_choice(control, VIDEO_CONTROLS, "control")
    seconds = _opt_int(seconds, "seconds", 1, 10)
    extend_seconds = _opt_int(extend_seconds, "extend_seconds", 1, 5)
    image_raw = (image or "").strip()
    end_raw = (end_image or "").strip()
    source_raw = (source or "").strip()

    if mode in (None, "t2v", "i2v", "first_last"):
        if not image_raw:
            raise _image_missing_error()
        if _video_image_key(image) in _NONE_ALIASES:
            raise RuntimeError(
                'image="none" is only for mode=extend or mode=restyle; for a new clip pass image="text" '
                'or a first-frame image (image="upload" for the photo attached in this turn)'
            )
        if source_raw:
            raise RuntimeError("source is only used for extend and restyle")
        if extend_seconds is not None:
            raise RuntimeError("extend_seconds is only used for mode=extend")
        if control is not None:
            raise RuntimeError("control is only used for mode=restyle")
        if mode != "first_last" and end_raw:
            raise RuntimeError("end_image is only used for mode=first_last")
        if mode == "t2v" and _video_image_key(image) not in _TEXT_ALIASES:
            raise RuntimeError('mode=t2v is text-only; pass image="text", or use mode=i2v to use this photo')
        if mode == "i2v" and _video_image_key(image) in _TEXT_ALIASES:
            raise RuntimeError("mode=i2v needs a first-frame image, not image=\"text\"")
        if mode == "first_last" and _video_image_key(image) in _TEXT_ALIASES:
            raise RuntimeError("mode=first_last needs a first-frame image, not image=\"text\"")
        _refuse_text_only_when_attached(image)
        frame = _resolve_video_image(image)
        body = {"prompt": prompt}
        if mode:
            body["mode"] = mode
        if seconds is not None:
            body["seconds"] = seconds
        if style is not None:
            body["style"] = style
        if frame is not None:
            body["image_base64"] = _image_base64(frame)
            _log("videos", "first-frame", frame.name)
        if mode == "first_last":
            end_path = _end_frame_path(end_image)
            body["end_image_base64"] = _image_base64(end_path, "the last-frame image")
            _log("videos", "last-frame", end_path.name)
        return body

    # extend / restyle: image must be passed explicitly as "none"
    if _video_image_key(image) not in _NONE_ALIASES:
        raise RuntimeError(
            f'mode={mode} does not use a first-frame image; pass image="none" and give the clip in source '
            "(previous, a gateway id, or an attached mp4)"
        )
    if end_raw:
        raise RuntimeError(f"mode={mode} does not use end_image")
    if style is not None:
        raise RuntimeError(f"style is not valid for mode={mode}")
    if seconds is not None:
        raise RuntimeError(f"seconds is not valid for mode={mode}")
    if mode == "extend" and control is not None:
        raise RuntimeError("control is only used for mode=restyle")
    if mode == "restyle" and extend_seconds is not None:
        raise RuntimeError("extend_seconds is only used for mode=extend")
    body = {"prompt": prompt, "mode": mode}
    body.update(_resolve_video_source(source, session_id or ""))
    if extend_seconds is not None:
        body["extend_seconds"] = extend_seconds
    if control is not None:
        body["control"] = control
    return body


@mcp.tool()
async def generate_video(
    prompt: Annotated[str, Field(description=(
        "Required. Sent to the Dell as written: no fixed instruction and no silent default. "
        'For a text-only render (image="text"), write one LTX-style paragraph covering subject, '
        "motion, camera, style, lighting and audio. When image is a first-frame source, write "
        "motion, camera and audio, and keep the photo's look unless the user asked to restyle. "
        "If the user gave no style or no motion, do not call this tool yet; ask one short question first."
    ))],
    image: Annotated[str, Field(description=(
        "Required, always. The photo you can see in the chat is NOT passed to this tool automatically: "
        'to animate a photo the user attached, pass image="upload" (or its exact path from the '
        '"[Image attached at: ...]" line). Other first-frame sources: image="latest" (the most recent '
        'upload, camera snapshot from look, or generated picture), image="look" (the latest camera '
        'snapshot), image="generated" (the latest generated picture), or an allowed absolute path. '
        'Pass image="text" for a prompt-only clip; it is refused if a photo is attached in this turn. '
        'For mode=extend or mode=restyle pass image="none" and give the clip in source. '
        "Nothing is filled in automatically."
    ))],
    mode: Annotated[str, Field(description=(
        "Optional. t2v, i2v, first_last, extend or restyle. Omit it for a new clip: the Dell "
        "defaults to i2v if a first-frame image is sent, otherwise t2v. Required for first_last, "
        "extend and restyle."
    ))] = None,
    end_image: Annotated[str, Field(description=(
        'Last-frame image for mode=first_last, using the same sources as image (not "text"). '
        "Not used for other modes."
    ))] = None,
    style: Annotated[str, Field(description=(
        "Optional built-in style preset for t2v, i2v and first_last only: anime, claymation, "
        "papercut, cyberpunk or painterly. Omit it for a realistic clip (no style). Not used "
        "for extend or restyle."
    ))] = None,
    seconds: Annotated[int, Field(description=(
        "Optional clip length in seconds, 1-10, for t2v, i2v and first_last only. Omit it to use "
        "the Dell default of 5. Not used for extend or restyle."
    ))] = None,
    source: Annotated[str, Field(description=(
        "The source clip for mode=extend or mode=restyle: previous (the last video generated in "
        "this session), a 32-hex Dell media id (or a /media/<id>.mp4 URL), or a chat-attached mp4 "
        "path of 10 MiB or less (sent as video_base64). Required for those modes."
    ))] = None,
    extend_seconds: Annotated[int, Field(description=(
        "How many seconds to add for mode=extend, 1-5. The source plus the extension must be at "
        "most 10 s. The Dell returns the source plus the new part."
    ))] = None,
    control: Annotated[str, Field(description=(
        "Restyle control for mode=restyle: canny, depth or pose. The output is as long as the source."
    ))] = None,
) -> str:
    """Generate a short video on the Dell (LTX-2.5).

    image is always required. A photo attached in the chat is NOT passed automatically: to animate it,
    pass image="upload" (or its path).

    New clip: mode t2v, i2v or first_last (or omit mode: the Dell defaults to i2v if a first-frame
    image is sent, otherwise t2v). image is "text" for text-only, or a first-frame source
    (upload/latest/look/generated or an allowed path). first_last also needs end_image. Optional
    seconds (1-10, Dell default 5) and style (anime, claymation, papercut, cyberpunk, painterly;
    omit for realistic). Only those fields are sent.

    Extend (make it longer) or restyle: pass mode, image="none", source (previous, a gateway id, or an
    attached mp4 of 10 MiB or less), and extend_seconds (1-5) or control (canny, depth, pose). Do not
    pass seconds or style. The prompt is always sent as written.

    On an Echo turn, play the result on the Echo when the Dell finishes. On a chat turn, send the
    clip to the chat. The render takes a few minutes (and may wait in the queue); this returns at
    once with the "working on it" message, including the queue position when the Dell reports one.
    A full queue (HTTP 429) is an error to tell Mo, not a silent retry. If the prompt is missing
    or blank, this fails. Occasional use only."""
    if not (prompt or "").strip():
        raise RuntimeError("prompt is required; write a visual description (no silent default)")
    origin = _job_origin("generate_video")
    body = _video_request_body(
        prompt, image, mode=mode, end_image=end_image, style=style, seconds=seconds,
        extend_seconds=extend_seconds, control=control, source=source,
        session_id=origin["session_id"],
    )
    try:
        j = await asyncio.to_thread(_media, "POST", "/v1/videos", body, 180)
    except DellMediaError as e:
        if e.code == 429:
            raise RuntimeError(f"{_QUEUE_FULL}: {e.msg}") from e
        raise
    job_id = j["job_id"]
    title = prompt[:60]

    async def play_on_echo(url):
        played = await _play_video_confirmed(url, title)
        rec = {"url": url, "title": title}
        global _last_video
        _last_video = rec
        _last_video_by_session[origin["session_id"]] = rec
        return played

    job, say = await _start_long_media(
        "videos", job_id, origin, "video", VIDEO_ETA_S / 60, play_on_echo, _queue_note(j)
    )
    return f"rendering the video (job {job['job_id']}), about {_minutes_words(VIDEO_ETA_S / 60)}.\n{say}"


@mcp.tool()
async def replay_last_video() -> str:
    """Play again the most recent video this conversation showed on the Echo
    (generated or YouTube). The Echo clears the screen when a clip ends;
    this is the way to watch it again. Fails if no video has been played in this
    conversation, or if that file has expired."""
    rec = _last_video_in_session("replay_last_video")
    if rec.get("local_video"):
        return await _play_local_video(rec["local_video"], rec.get("title") or "last video", "replay_last_video")
    url = rec["url"]
    await asyncio.to_thread(_last_video_check, url)
    played = await _play_video_confirmed(url, rec.get("title") or "last video")
    return played

@mcp.tool()
async def replay_video() -> str:
    """Same as replay_last_video: play again the last video in this conversation."""
    return await replay_last_video()


async def _capture_still():
    """One JPEG from the Echo camera over ESPHome. Kept in memory only; never written to disk."""
    async with echo() as (c, ents, _):
        if "camera" not in ents:
            raise RuntimeError("the Echo has no camera entity")
        got = asyncio.get_running_loop().create_future()
        def on(s):
            if isinstance(s, CameraState) and not got.done():
                got.set_result(s)
        c.subscribe_states(on)
        c.request_single_image()
        try:
            state = await asyncio.wait_for(got, LOOK_TIMEOUT)
        except asyncio.TimeoutError:
            raise RuntimeError(
                "camera timed out after 10s with no JPEG; privacy may be on or the lens cover may be closed"
            )
        data = bytes(state.data or b"")
        if len(data) < 4 or data[:2] != b"\xff\xd8":
            raise RuntimeError("camera did not return a JPEG")
        return data

def _describe_jpeg(jpeg, question):
    if not jpeg or jpeg[:2] != b"\xff\xd8":
        raise RuntimeError("camera did not return a JPEG")
    img, mime = _normalize_image_bytes(jpeg, "the camera photo")
    body = {
        "model": "Qwen3.8-Flash-Next-UD-IQ4_XS",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": question},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64," + base64.b64encode(img).decode()}},
            ],
        }],
        "chat_template_kwargs": {"enable_thinking": False},
    }
    req = urllib.request.Request(
        LLM + "/v1/chat/completions",
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Authorization": "Bearer " + os.environ["DELL_LLM_API_KEY"], "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=LLM_TIMEOUT_S) as r:
            data = json.load(r)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            msg = json.loads(raw).get("error") or raw
        except (ValueError, TypeError):
            msg = raw[:300]
        raise RuntimeError(f"Dell vision {e.code}: {msg}")
    msg = (data.get("choices") or [{}])[0].get("message") or {}
    content = msg.get("content")
    if isinstance(content, list):
        content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
    text = (content or "").strip()
    if not text:
        raise RuntimeError("Dell vision model returned no description")
    return text

@mcp.tool()
async def show_camera(entity: str, seconds: int = 30) -> str:
    """Show a camera's live view on the Echo screen by entity id, e.g. camera.front_door."""
    return await call("home_show_camera", entity=entity, seconds=seconds)

async def _look_vision_snapshot(origin: str) -> str:
    """Cut-out snapshot while terminator vision is on. Saved under the look cache. No camera still."""
    jpeg, labels = await asyncio.to_thread(_vision_snapshot)
    path = await asyncio.to_thread(_save_look_snapshot, jpeg)
    named = ", ".join(labels) if labels else "detected objects"
    text = "a true-colour cut-out of %s on a white background" % named
    if origin == "chat":
        return f"{text}\nSAVED:{path}"
    async with _lan_jpeg(jpeg) as (slot, url):
        shown = await _show_picture_checked(url, "", "", LOOK_SHOW_S)
        await _wait_echo_fetched(slot)
        return f"{text}\n\n{shown} for {LOOK_SHOW_S} seconds\nSAVED:{path}"


@mcp.tool()
async def look(question: str = "Describe what you see.") -> str:
    """Capture one photo from this Echo's camera and describe it with the Dell's local model.
    The green privacy light comes on briefly. Refused if privacy is on or the lens cover is closed.
    If terminator vision is on, do not use the camera still: take a true-colour cut-out of only the
    detected objects on a white background, with no labels, show it on the Echo, save it (SAVED: path),
    and report the object names. If that fails, this tool fails; it does not fall back to a normal look.
    On an Echo turn without vision, also show the photo on the Echo screen for about 20 seconds, and save it so
    generate_video can use it as a first frame in this conversation (SAVED: path). The on-screen
    copy is served only on the LAN from memory, under a one-time path, and is dropped after the
    Echo has fetched it, or after at most 30 seconds. If the photo cannot be shown on the Echo, this tool fails.
    On a chat turn without vision, return the description only: do not show it on the Echo, do not write the photo to disk, and do not attach it."""
    origin = _turn_origin("look")
    if _vision_is_on():
        return await _look_vision_snapshot(origin)
    jpeg = await _capture_still()
    if origin == "chat":
        return await asyncio.to_thread(_describe_jpeg, jpeg, question)
    path = await asyncio.to_thread(_save_look_snapshot, jpeg)
    async with _lan_jpeg(jpeg) as (slot, url):
        shown = await _show_picture_checked(url, "", "", LOOK_SHOW_S)
        await _wait_echo_fetched(slot)
        text = await asyncio.to_thread(_describe_jpeg, jpeg, question)
        return f"{text}\n\n{shown} for {LOOK_SHOW_S} seconds\nSAVED:{path}"


@mcp.tool()
async def intercom_call(device: str) -> str:
    """Start an intercom call from the Echo to another device by name."""
    return await call("intercom_call", device=device)

@mcp.tool()
async def pause_timer(id: str) -> str:
    """Pause a running Echo timer by id."""
    return await call("timer_pause", id=id)

@mcp.tool()
async def resume_timer(id: str) -> str:
    """Resume a paused Echo timer by id."""
    return await call("timer_resume", id=id)

@mcp.tool()
async def add_time_to_timer(id: str, duration: str) -> str:
    """Add time to an Echo timer, e.g. duration '5 minutes'."""
    return await call("timer_add", id=id, duration=duration)

@mcp.tool()
async def snooze_alarm(how_long: str = "") -> str:
    """Snooze the alarm ringing on the Echo, e.g. 'for 10 minutes'; empty uses the default snooze."""
    return await call("alarm_snooze_for", True, sentence=how_long)

RADIO = "https://de1.api.radio-browser.info/json/stations/search"

def _radio_search(query: str, by: str, country: str):
    q = {"codec": "MP3", "hidebroken": "true", "order": "clickcount", "reverse": "true", "limit": "6",
         ("tag" if by == "genre" else "name"): query}
    if country:
        q["countrycode"] = country
    req = urllib.request.Request(RADIO + "?" + urllib.parse.urlencode(q), headers={"User-Agent": "echo-hermes-bridge/1"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return [{"name": s["name"].strip(), "url": s["url_resolved"], "country": s["countrycode"], "tags": s["tags"][:60]}
                for s in json.load(r)]


_LOG_CAP = 4000
_HERMES_DB = pathlib.Path.home() / ".hermes" / "state.db"
_MCP_STDERR = pathlib.Path.home() / ".hermes" / "logs" / "mcp-stderr.log"
_UNTRUSTED_RE = re.compile(
    r"<untrusted_tool_result[^>]*>.*?</untrusted_tool_result>", re.S | re.I)
_RESULT_RE = re.compile(r'"result"\s*:\s*"((?:\\.|[^"\\])*)"')
_REDACT_RE = re.compile(
    r"(?is)"
    r"(bearer\s+[a-z0-9._\-+=/]+)"
    r"|(?:api[_-]?key|password|secret|token|authorization|noise[_-]?psk)\s*[:=]\s*\S+"
    r"|(sk-[a-z0-9]{10,})"
    r"|(eyJ[a-zA-Z0-9_\-]{20,}\.[a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+)"
    r"|(-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----)"
)
_URL_QUERY_RE = re.compile(r"(https?://[^\s?]+)\?[^\s]*")
_ECHO_KEEP_RE = re.compile(
    r"video:|picture:|music:|timer:|alarm:|pipeline| heard |replying |error",
    re.I,
)

def _redact(text):
    if not text:
        return ""
    text = _URL_QUERY_RE.sub(r"\1?…", str(text))
    text = _REDACT_RE.sub("[redacted]", text)
    return text.replace(str(SSH_KEY), "ECHO_SSH_KEY")

def _mentions(text, about):
    if not about:
        return True
    return re.search(r"(?i)(?<![A-Za-z0-9])" + re.escape(about) + r"(?![A-Za-z0-9])", text or "") is not None

def _short(text, n=240):
    text = re.sub(r"\s+", " ", (text or "")).strip()
    return text if len(text) <= n else text[: n - 1] + "…"

def _tool_result_text(content):
    raw = content or ""
    rm = _RESULT_RE.search(raw)
    if rm:
        try:
            return json.loads('"' + rm.group(1) + '"')
        except json.JSONDecodeError:
            return bytes(rm.group(1), "utf-8").decode("unicode_escape")
    m = _UNTRUSTED_RE.search(raw)
    if not m:
        return raw
    inner = re.sub(r"<[^>]+>", "", m.group(0))
    preamble = "The following content was retrieved"
    if preamble in inner:
        inner = inner.split(preamble, 1)[-1]
        # drop the rest of the trust-warning paragraph
        inner = re.sub(r"^.*?(?:\n\n|\n)(?=[{\"A-Za-z])", "", inner, count=1, flags=re.S)
        if "can issue instructions." in inner:
            inner = inner.split("can issue instructions.", 1)[-1]
    return inner.strip() or raw

def _parse_tool_calls(raw):
    if not raw:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return [("unparsed", _short(raw, 160))]
    out = []
    for tc in raw if isinstance(raw, list) else [raw]:
        fn = (tc.get("function") or {}) if isinstance(tc, dict) else {}
        name = fn.get("name") or (tc.get("name") if isinstance(tc, dict) else "") or "tool"
        name = re.sub(r"^mcp__echo__", "", name)
        args = fn.get("arguments") or (tc.get("arguments") if isinstance(tc, dict) else "") or ""
        if not isinstance(args, str):
            args = json.dumps(args, ensure_ascii=False)
        out.append((name, _short(args, 200)))
    return out

def _fmt_hm(ts):
    return time.strftime("%H:%M:%S", time.localtime(float(ts)))

def _digest_turns(since, about):
    if not _HERMES_DB.is_file():
        raise RuntimeError(f"session store not found: {_HERMES_DB}")
    uri = f"file:{_HERMES_DB}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=5)
    try:
        con.row_factory = sqlite3.Row
        rows = list(con.execute(
            "SELECT session_id, role, content, tool_calls, tool_name, timestamp "
            "FROM messages WHERE timestamp >= ? AND role != 'session_meta' "
            "ORDER BY timestamp, id",
            (since,),
        ))
    finally:
        con.close()
    lines, buf, sid = [], [], None

    def flush():
        nonlocal buf
        if not buf:
            return
        text = "\n".join(buf)
        if _mentions(text, about):
            lines.append(text)
        buf = []

    for r in rows:
        if r["session_id"] != sid:
            flush()
            sid = r["session_id"]
            buf.append(f"session {sid}")
        ts = _fmt_hm(r["timestamp"])
        role = r["role"]
        if role == "user":
            buf.append(f"{ts} user: {_short(_redact(r['content'] or ''), 300)}")
        elif role == "assistant":
            calls = _parse_tool_calls(r["tool_calls"])
            if calls:
                for name, args in calls:
                    buf.append(f"{ts} tool call {name}({args})")
            text = (r["content"] or "").strip()
            if text:
                buf.append(f"{ts} glados: {_short(_redact(text), 300)}")
            if not calls and not text:
                buf.append(f"{ts} glados: [no tool call, empty reply]")
            if not calls and text:
                buf.append(f"{ts} (no tool call)")
        elif role == "tool":
            name = re.sub(r"^mcp__(?:echo__)?", "", r["tool_name"] or "tool")
            if name == "check_my_log" or name.endswith("check_my_log"):
                buf.append(f"{ts} tool result {name}: [omitted so this digest is not recursive]")
                continue
            res = _short(_redact(_tool_result_text(r["content"] or "")), 300)
            low = res.lower()
            kind = "error" if ("error" in low or "failed" in low or "runtimeerror" in low) else "result"
            buf.append(f"{ts} tool {kind} {name}: {res}")
        else:
            buf.append(f"{ts} {role}: {_short(_redact(r['content'] or ''), 200)}")
    flush()
    return lines

def _digest_mcp_stderr(since, about):
    if not _MCP_STDERR.is_file():
        return [f"bridge mcp-stderr: missing ({_MCP_STDERR})"]
    out = []
    for line in _MCP_STDERR.read_text(errors="replace").splitlines():
        # "2026-11-13 20:12:47,373 ..."
        try:
            ts = time.mktime(time.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f"))
        except ValueError:
            try:
                ts = time.mktime(time.strptime(line[:19], "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                ts = None
        if ts is not None and ts < since:
            continue
        if "echo-bridge" not in line and "MCP server 'echo'" not in line:
            continue
        if not _mentions(line, about):
            continue
        out.append(_short(_redact(line), 300))
    return out

def _digest_echo_log(minutes, about):
    remote = (
        "echo UP:$(cut -d' ' -f1 /proc/uptime); "
        "tail -n 8000 /data/techo5-linux/techo5.log"
    )
    raw = _echo_ssh(remote, timeout=15)
    lines = raw.splitlines()
    if not lines or not lines[0].startswith("UP:"):
        raise RuntimeError("Echo log fetch did not return uptime")
    up = float(lines[0][3:].strip() or "0")
    cut = up - max(minutes, 0) * 60
    ts_re = re.compile(r"\[(\d+(?:\.\d+)?)\]")
    now = time.time()
    boot = now - up
    out = []
    for ln in lines[1:]:
        m = ts_re.search(ln)
        if not m or float(m.group(1)) < cut:
            continue
        if not _ECHO_KEEP_RE.search(ln):
            continue
        if not _mentions(ln, about):
            continue
        wall = time.strftime("%H:%M:%S", time.localtime(boot + float(m.group(1))))
        body = ts_re.sub("", ln, count=1)
        body = re.sub(r"^\s*[IWE]\s*", "", body).strip()
        out.append(_short(_redact(f"{wall} {body}"), 300))
    return out

def _cap_sections(header, sections, about=""):
    """Keep the most useful newest lines, cap the whole digest at _LOG_CAP characters."""
    bags = [[title, list(rows)] for title, rows in sections]

    def render(note=None):
        parts = []
        if note:
            parts.append(note)
        parts.append(header)
        for title, rows in bags:
            parts.append(title)
            parts.extend(rows if rows else ["(none)"])
        return "\n".join(parts).strip() + "\n"

    text = render()
    if len(text) <= _LOG_CAP:
        return text
    note = "[truncated to the newest %d characters]" % _LOG_CAP
    while True:
        text = render(note)
        if len(text) <= _LOG_CAP:
            return text
        # Drop a line that does not mention `about` first (oldest such), then the oldest remaining.
        def drop(prefer_unrelated):
            if prefer_unrelated and about:
                for _, rows in bags:
                    for i, ln in enumerate(rows):
                        if not _mentions(ln, about):
                            rows.pop(i)
                            return True
                return False
            # Prefer dropping later check_my_log investigations so earlier
            # action turns (the failure being looked up) stay in the digest.
            for title, rows in bags:
                if title.startswith("Turns:"):
                    for i in range(len(rows) - 1, -1, -1):
                        if "tool call check_my_log" in rows[i] or "tool call mcp__echo__check_my_log" in rows[i]:
                            rows.pop(i)
                            return True
            for _, rows in bags:
                if rows:
                    rows.pop(0)
                    return True
            return False
        if not drop(True) and not drop(False):
            return text[: _LOG_CAP - 1] + "…"


# ---------------------------------------------------------------------------------------------
# Other Hermes agents on the LAN (Mo approved 2026-11-14 16:45: "Status and asking them questions").
# Fixed allowlist; each key comes from the env (config.yaml mcp_servers.echo.env <- ~/.hermes/.env)
# and is only ever sent as a Bearer header to that peer. Never logged, printed or returned.
HERMES_PEERS = {
    "dell": {"label": "the Dell", "url": os.environ["HERMES_PEER_DELL_URL"], "key_env": "HERMES_PEER_DELL_KEY"},
    "imac": {"label": "the iMac", "url": os.environ["HERMES_PEER_IMAC_URL"], "key_env": "HERMES_PEER_IMAC_KEY"},
    "macmini": {"label": "the Mac mini", "url": os.environ["HERMES_PEER_MACMINI_URL"], "key_env": "HERMES_PEER_MACMINI_KEY"},
}
_HERMES_PEER_ALIASES = {
    "dell": "dell", "dellbot": "dell", "dellhermes": "dell",
    "imac": "imac", "minecraft": "imac", "minecraftbot": "imac", "minecraftagent": "imac",
    "imacbot": "imac", "imachermes": "imac", "sakura": "imac", "macsakura": "imac",
    "macmini": "macmini", "mini": "macmini", "macminibot": "macmini", "macminihermes": "macmini",
}
PEER_MESSAGE_PREFIX = "Message from GlaDOS (Mo's Echo voice assistant), asked on Mo's behalf: "
# How long ask_hermes_agent waits for a peer's answer, by where the request came from (_turn_origin):
# "echo" is an Echo voice turn (api_server); the Echo stops waiting for a reply at 60 s, so stay under it.
# "chat" is the Hermex desktop / local chat, which can wait for a slow peer (the iMac took ~4.5 min).
# An origin that is neither fails closed via _turn_origin; there is no default budget.
PEER_QUESTION_BUDGET_S = {"echo": 50, "chat": 300}
PEER_POLL_S = 2
PEER_HTTP_TIMEOUT_S = 10
_PEER_BOT_CHAT_TITLE = "Bot Chat"  # the session `hermes peer run` uses on a peer
_PEER_TERMINAL = frozenset({"completed", "failed", "cancelled", "interrupted"})
_peer_clock = time.monotonic
_peer_sleep = asyncio.sleep


def _hermes_peer(name):
    """Allowlisted peer for a spoken name (Dell, iMac / Minecraft bot, Mac mini). Unknown names are refused."""
    raw = re.sub(r"[^a-z0-9]", "", (name or "").lower())
    raw = re.sub(r"^the", "", raw)
    for suffix in ("agent", "bot", "hermes"):
        if raw not in _HERMES_PEER_ALIASES and raw.endswith(suffix) and raw[: -len(suffix)] in _HERMES_PEER_ALIASES:
            raw = raw[: -len(suffix)]
    peer_id = _HERMES_PEER_ALIASES.get(raw)
    if not peer_id:
        raise RuntimeError(
            "I can only ask the Dell, the iMac (the Minecraft bot) or the Mac mini, not %r" % (name,)
        )
    return peer_id, HERMES_PEERS[peer_id]


def _hermes_peer_key(peer):
    """The peer's key from the env. Missing (or an unsubstituted ${...}) fails loudly; no fallback."""
    key = (os.environ.get(peer["key_env"]) or "").strip()
    if not key or key.startswith("${"):
        raise RuntimeError("I don't have a key for %s yet" % peer["label"])
    return key


def _scrub(text, key):
    text = str(text)
    return text.replace(key, "<key>") if key else text


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A peer's Bearer key must never follow a redirect anywhere."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirect refused (key not forwarded)", headers, fp)


_peer_opener = urllib.request.build_opener(_NoRedirect)


def _peer_http(method, url, key, body=None, timeout=PEER_HTTP_TIMEOUT_S, headers=None):
    """One JSON request to an allowlisted peer. Errors are raised with the key scrubbed out."""
    h = {"Authorization": "Bearer " + key, "Content-Type": "application/json", "User-Agent": "glados-echo-bridge"}
    if headers:
        h.update(headers)
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with _peer_opener.open(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8", "replace")[:200]
        except Exception:
            detail = ""
        raise RuntimeError(_scrub("HTTP %s %s" % (e.code, detail or e.reason), key)) from None
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise RuntimeError(_scrub("not reachable (%s)" % (getattr(e, "reason", None) or e), key)) from None
    try:
        out = json.loads(raw)
    except ValueError:
        raise RuntimeError(_scrub("answered with something that is not JSON: %s" % raw[:120], key)) from None
    if not isinstance(out, dict):
        raise RuntimeError("answered with a JSON value that is not an object")
    return out


def _london_clock(iso):
    """'HH:MM' London time from an ISO timestamp, or the raw text if it cannot be parsed."""
    import datetime, zoneinfo
    try:
        t = datetime.datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=datetime.timezone.utc)
        return t.astimezone(zoneinfo.ZoneInfo("Europe/London")).strftime("%H:%M")
    except (TypeError, ValueError):
        return str(iso)


def _peer_status_summary(label, d):
    """Spoken summary of GET /health/detailed."""
    status = str(d.get("status") or "unknown")
    up = "up" if status == "ok" else "answering but its status is %s" % status
    busy = d.get("gateway_busy")
    busy_s = "busy" if busy is True else "idle" if busy is False else "busy state unknown"
    agents = d.get("active_agents")
    try:
        n = int(agents)
        agents_s = "%d active agent%s" % (n, "" if n == 1 else "s")
    except (TypeError, ValueError):
        agents_s = "active agents unknown"
    hb = d.get("last_heartbeat")
    hb_s = ("last heartbeat %s London time" % _london_clock(hb)) if hb else "no heartbeat reported"
    ver = d.get("version")
    return "%s Hermes is %s%s, %s, %s, %s." % (
        label[0].upper() + label[1:], up, (" (version %s)" % ver) if ver else "", busy_s, agents_s, hb_s)


def _peer_bot_chat(base, key):
    """The peer's canonical Bot Chat session id (created if missing), as `hermes peer run` does."""
    q = urllib.parse.urlencode({"limit": 200, "title": _PEER_BOT_CHAT_TITLE, "include_hidden": 1})
    listing = _peer_http("GET", "%s/api/sessions?%s" % (base, q), key)
    for s in listing.get("data") or []:
        if isinstance(s, dict) and (s.get("title") or "").strip() == _PEER_BOT_CHAT_TITLE and s.get("id"):
            return str(s["id"])
    created = _peer_http("POST", base + "/api/sessions", key,
                         body={"title": _PEER_BOT_CHAT_TITLE, "source": "bot_peer_dm"})
    s = created.get("session") if isinstance(created.get("session"), dict) else created
    sid = str(s.get("id") or s.get("session_id") or "")
    if not sid:
        raise RuntimeError("did not return a session id for its Bot Chat")
    return sid


@mcp.tool()
async def ask_hermes_agent(
    name: Annotated[str, Field(description=(
        "Which Hermes bot: 'dell', 'imac' (also the Minecraft bot) or 'macmini'. Spoken names like "
        "Dell, iMac, Minecraft bot or Mac mini are accepted."
    ))],
    question: Annotated[str, Field(description=(
        "Leave empty for a status check. Otherwise the question to ask that bot, in plain words."
    ))] = "",
) -> str:
    """Check on, or ask a question of, one of Mo's other Hermes bots on the home network:
    the Dell, the iMac (the Minecraft bot) or the Mac mini. Only these three.
    With no question: its status (up or down, busy or idle, active agents, last heartbeat).
    With a question: sends it to that bot's Bot Chat, prefixed as coming from GlaDOS on Mo's behalf,
    waits for its answer and returns it: up to 300 seconds (5 minutes) when asked from the Hermex desktop
    chat, up to 50 seconds when asked by voice on the Echo (the Echo stops waiting at 60 seconds).
    If it takes longer, it says so; the question keeps running there and must not be resent. Questions should be read-only; do not ask another
    bot to change, delete, push or restart anything unless Mo asked for exactly that.
    Fails with "I don't have a key for ..." if that bot's key is not set up yet."""
    peer_id, peer = _hermes_peer(name)
    label = peer["label"]
    key = _hermes_peer_key(peer)
    base = peer["url"]
    q = (question or "").strip()
    if not q:
        try:
            d = await asyncio.to_thread(_peer_http, "GET", base + "/health/detailed", key)
        except RuntimeError as e:
            _log("ask-hermes-agent", "%s status failed: %s" % (peer_id, _scrub(e, key)))
            if "not reachable" in str(e):
                return "%s Hermes is down or not reachable: %s" % (label[0].upper() + label[1:], _scrub(e, key))
            raise RuntimeError("%s status check failed: %s" % (label, _scrub(e, key))) from None
        _log("ask-hermes-agent", "%s status ok" % peer_id)
        return _peer_status_summary(label, d)
    budget = PEER_QUESTION_BUDGET_S[_turn_origin("ask_hermes_agent")]
    try:
        sid = await asyncio.to_thread(_peer_bot_chat, base, key)
        started = await asyncio.to_thread(
            _peer_http, "POST", base + "/v1/runs", key,
            {"input": PEER_MESSAGE_PREFIX + q, "session_id": sid}, PEER_HTTP_TIMEOUT_S,
            {"Idempotency-Key": "glados-" + secrets.token_hex(12)},
        )
    except RuntimeError as e:
        _log("ask-hermes-agent", "%s question not sent: %s" % (peer_id, _scrub(e, key)))
        raise RuntimeError("I couldn't send that to %s: %s" % (label, _scrub(e, key))) from None
    run_id = str(started.get("run_id") or "")
    if not run_id:
        raise RuntimeError("%s did not return a run id" % label)
    _log("ask-hermes-agent", "%s run %s started (budget %ds)" % (peer_id, run_id, budget))
    deadline = _peer_clock() + budget
    while True:
        try:
            st = await asyncio.to_thread(
                _peer_http, "GET", "%s/v1/runs/%s" % (base, urllib.parse.quote(run_id, safe="")), key)
        except RuntimeError as e:
            _log("ask-hermes-agent", "%s run %s poll failed: %s" % (peer_id, run_id, _scrub(e, key)))
            raise RuntimeError("I asked %s, but lost track of the answer: %s" % (label, _scrub(e, key))) from None
        status = st.get("status")
        if status == "completed":
            out = (st.get("output") or "").strip()
            _log("ask-hermes-agent", "%s run %s completed (%d chars)" % (peer_id, run_id, len(out)))
            if not out:
                return "%s finished but gave no answer." % (label[0].upper() + label[1:])
            return "%s says: %s" % (label[0].upper() + label[1:], _scrub(out, key))
        if status in _PEER_TERMINAL:
            err = _scrub(st.get("error") or st.get("turn_exit_reason") or "no reason given", key)
            _log("ask-hermes-agent", "%s run %s %s: %s" % (peer_id, run_id, status, err))
            raise RuntimeError("%s could not answer (%s): %s" % (label, status, err))
        if _peer_clock() >= deadline:
            _log("ask-hermes-agent", "%s run %s still %s after %ds" % (peer_id, run_id, status, budget))
            return ("%s hasn't answered within %d seconds. The question is still running in its Bot Chat "
                    "and will be answered there; I won't send it again."
                    % (label[0].upper() + label[1:], budget))
        await _peer_sleep(PEER_POLL_S)


@mcp.tool()
async def check_my_log(minutes: int = 15, about: str = "") -> str:
    """Read-only: look up why a recent action failed or did not happen.
    Returns a compact digest of your recent turns (what the user said, which tools you called and with what arguments, what those tools returned, and your replies), plus relevant echo-bridge log lines and Echo device log lines (video, picture, music, timer, pipeline error, heard, replying).
    minutes is how far back to look, default 15. If the failure may be older than that, pass a larger minutes (for example 60). about, if set, keeps turns and log lines that mention that word — use the specific subject (cat, youtube, picture), not a generic word like video.
    This does not play, show, stop, or change anything on the Echo. When asked why something failed, answer only from what this tool returns."""
    if not isinstance(minutes, int) or isinstance(minutes, bool) or minutes < 1 or minutes > 1440:
        raise ValueError("minutes must be an integer from 1 to 1440")
    about = (about or "").strip()
    since = time.time() - minutes * 60
    header = f"Last {minutes} minute" + ("s" if minutes != 1 else "")
    if about:
        header += f", about {about!r}"
    header += " (Europe/London):"

    def collect():
        turns = _digest_turns(since, about)
        bridge = _digest_mcp_stderr(since, about)
        echo_lines = _digest_echo_log(minutes, about)
        return turns, bridge, echo_lines

    turns, bridge, echo_lines = await asyncio.to_thread(collect)
    return _cap_sections(header, [
        ("Turns:", turns or ["(no matching turns)"]),
        ("Bridge mcp-stderr:", bridge or ["(none)"]),
        ("Echo log:", echo_lines or ["(none)"]),
    ], about)


@mcp.tool()
async def find_radio(query: str, by: str = "name", country: str = "GB") -> str:

    """Search internet radio stations (Radio Browser) by station name or by genre. country is a 2-letter code, '' for anywhere. Returns names and stream URLs to play with play_audio_url."""
    if by not in ("name", "genre"):
        raise ValueError("by must be 'name' or 'genre'")
    return json.dumps(await asyncio.to_thread(_radio_search, query, by, country))

@mcp.tool()
async def play_radio(station: str, country: str = "GB") -> str:
    """Find a radio station by name (most popular match) and play it on the Echo."""
    hits = await asyncio.to_thread(_radio_search, station, "name", country)
    if not hits:
        raise RuntimeError(f"no station called {station!r} found in {country or 'any country'}")
    async with echo() as (c, ents, _):
        c.media_player_command(ents["speaker"].key, media_url=hits[0]["url"])
    return "playing " + hits[0]["name"]


# ---- Free podcasts (iTunes Search API + the show's public RSS) and LibriVox audiobooks
# (archive.org librivoxaudio collection). Outbound HTTPS only; nothing listens. The Echo speaker
# decodes MP3 (go-mp3; no AAC decoder in its firmware), so only MP3 episodes/chapters are played,
# by handing the original public URL to the speaker. Anything else fails with a clear error.
PODCAST_SEARCH_URL = "https://itunes.apple.com/search"
ARCHIVE_SEARCH_URL = "https://archive.org/advancedsearch.php"
ARCHIVE_METADATA_URL = "https://archive.org/metadata/"
ARCHIVE_DOWNLOAD_URL = "https://archive.org/download/"
LIBRIVOX_MP3_FORMAT = "64Kbps MP3"
_AUDIO_HTTP_UA = "Mozilla/5.0 (X11; Linux x86_64) echo-hermes-bridge"
_AUDIO_HTTP_TIMEOUT = 20
_FEED_MAX_BYTES = 40 * 1024 * 1024
_ITUNES_NS = "{http://www.itunes.com/dtds/podcast-1.0.dtd}"


def _audio_http_get(url, max_bytes=_FEED_MAX_BYTES, headers=None):
    req = urllib.request.Request(url, headers={"User-Agent": _AUDIO_HTTP_UA, **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=_AUDIO_HTTP_TIMEOUT) as r:
            data = r.read(max_bytes + 1)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} {e.reason} from {urllib.parse.urlsplit(url).netloc}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"cannot reach {urllib.parse.urlsplit(url).netloc}: {e.reason}") from e
    if len(data) > max_bytes:
        raise RuntimeError(f"response from {urllib.parse.urlsplit(url).netloc} is larger than {max_bytes} bytes")
    return data


def _audio_http_json(url):
    raw = _audio_http_get(url, max_bytes=8 * 1024 * 1024)
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise RuntimeError(f"{urllib.parse.urlsplit(url).netloc} did not return JSON") from e


def _audio_seconds(value):
    """'1:05:06', '65:06', '3906', '3906.5' -> seconds; anything else -> None."""
    s = str(value or "").strip()
    if not s:
        return None
    try:
        if ":" in s:
            secs = 0.0
            for part in s.split(":"):
                secs = secs * 60 + float(part)
            return int(secs)
        return int(float(s))
    except ValueError:
        return None


def _is_mp3_bytes(head: bytes) -> bool:
    if head[:3] == b"ID3":
        return True
    # MPEG audio frame sync with a non-zero layer (ADTS AAC has layer bits 00).
    return len(head) >= 2 and head[0] == 0xFF and (head[1] & 0xE0) == 0xE0 and (head[1] & 0x06) != 0


def _check_mp3_url(url):
    """Fetch the first bytes as the Echo would (following redirects) and require MP3 audio."""
    head = _audio_http_get(url, max_bytes=1 << 20, headers={"Range": "bytes=0-15"})[:16]
    if not _is_mp3_bytes(head):
        raise RuntimeError(
            "that audio is not MP3 (the Echo speaker plays MP3 only); first bytes from %s were not MP3"
            % urllib.parse.urlsplit(url).netloc
        )


def _words(text):
    return [w for w in re.findall(r"[a-z0-9]+", (text or "").lower()) if w]


def _podcast_find(query):
    q = (query or "").strip()
    if not q:
        raise RuntimeError("podcast name is empty")
    u = urllib.parse.urlsplit(q)
    if u.scheme in ("http", "https") and u.hostname:
        return {"name": "", "author": "", "feed": q}
    params = urllib.parse.urlencode({"media": "podcast", "entity": "podcast", "limit": 10, "term": q})
    data = _audio_http_json(f"{PODCAST_SEARCH_URL}?{params}")
    hits = [r for r in (data.get("results") or []) if r.get("feedUrl")]
    if not hits:
        raise RuntimeError(f"no podcast found for {q!r}")
    r = hits[0]
    return {"name": (r.get("collectionName") or "").strip(), "author": (r.get("artistName") or "").strip(),
            "feed": r["feedUrl"]}


def _parse_podcast_feed(raw: bytes):
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        raise RuntimeError(f"the podcast feed is not valid RSS: {e}") from e
    channel = root.find("channel")
    if channel is None:
        raise RuntimeError("the podcast feed has no RSS channel")
    show = (channel.findtext("title") or "").strip()
    author = (channel.findtext(_ITUNES_NS + "author") or "").strip()
    episodes = []
    for item in channel.findall("item"):
        enc = item.find("enclosure")
        if enc is None or not enc.get("url"):
            continue
        episodes.append({
            "title": (item.findtext("title") or "").strip() or "untitled episode",
            "url": enc.get("url").strip(),
            "type": (enc.get("type") or "").strip().lower(),
            "duration": _audio_seconds(item.findtext(_ITUNES_NS + "duration")),
            "published": (item.findtext("pubDate") or "").strip(),
        })
    return show, author, episodes


def _pick_episode(episodes, episode):
    """Feed order is newest first. '' or 'latest' -> newest; otherwise every word must be in the title."""
    if not episodes:
        raise RuntimeError("that podcast feed has no playable episodes")
    want = (episode or "").strip()
    if not want or want.lower() in ("latest", "newest", "last", "most recent"):
        return episodes[0]
    words = _words(want)
    for ep in episodes:
        title_words = set(_words(ep["title"]))
        if all(w in title_words for w in words):
            return ep
    recent = "; ".join(e["title"] for e in episodes[:3])
    raise RuntimeError(f"no episode matching {want!r}; the newest are: {recent}")


def _podcast_resolve(query, episode=""):
    found = _podcast_find(query)
    show, author, episodes = _parse_podcast_feed(_audio_http_get(found["feed"]))
    ep = _pick_episode(episodes, episode)
    if ep["type"] and ep["type"] not in ("audio/mpeg", "audio/mp3", "audio/mpeg3", "audio/x-mpeg"):
        raise RuntimeError(
            f"that episode is {ep['type']}, not MP3; the Echo speaker plays MP3 only"
        )
    _check_mp3_url(ep["url"])
    return {
        "show": found["name"] or show or "podcast",
        "author": found["author"] or author,
        "episode": ep["title"],
        "url": ep["url"],
        "duration": ep["duration"],
        "published": ep["published"],
    }


def _archive_query(words):
    terms = " AND ".join(words)
    return f"collection:(librivoxaudio) AND (title:({terms}) OR creator:({terms}))"


def _audiobook_find(query):
    words = _words(query)
    if not words:
        raise RuntimeError("audiobook title is empty")
    params = urllib.parse.urlencode([
        ("q", _archive_query(words)), ("fl[]", "identifier"), ("fl[]", "title"), ("fl[]", "creator"),
        ("sort[]", "downloads desc"), ("rows", "5"), ("output", "json"),
    ])
    data = _audio_http_json(f"{ARCHIVE_SEARCH_URL}?{params}")
    docs = (data.get("response") or {}).get("docs") or []
    if not docs:
        raise RuntimeError(f"no LibriVox audiobook found for {query!r}")
    d = docs[0]
    creator = d.get("creator") or ""
    if isinstance(creator, list):
        creator = ", ".join(map(str, creator))
    return {"id": d["identifier"], "title": (d.get("title") or d["identifier"]).strip(), "author": str(creator).strip()}


def _audiobook_chapters(meta):
    files = [f for f in (meta.get("files") or []) if f.get("format") == LIBRIVOX_MP3_FORMAT and f.get("name")]
    files.sort(key=lambda f: f["name"])
    return files


def _audiobook_resolve(query, chapter=1):
    if isinstance(chapter, bool) or not isinstance(chapter, int) or chapter < 1:
        raise ValueError("chapter must be a whole number from 1")
    book = _audiobook_find(query)
    meta = _audio_http_json(ARCHIVE_METADATA_URL + urllib.parse.quote(book["id"]))
    chapters = _audiobook_chapters(meta)
    if not chapters:
        raise RuntimeError(f"{book['title']} has no {LIBRIVOX_MP3_FORMAT} files on archive.org")
    if chapter > len(chapters):
        raise RuntimeError(f"{book['title']} has {len(chapters)} chapters; there is no chapter {chapter}")
    f = chapters[chapter - 1]
    url = ARCHIVE_DOWNLOAD_URL + urllib.parse.quote(book["id"]) + "/" + urllib.parse.quote(f["name"])
    _check_mp3_url(url)
    return {
        "title": book["title"], "author": book["author"], "chapter": chapter, "chapters": len(chapters),
        "chapter_title": (f.get("title") or f["name"]).strip(), "url": url,
        "duration": _audio_seconds(f.get("length")),
    }


@mcp.tool()
async def long_job_status(
    job: Annotated[str, Field(description='Job id from a long job (video, song, ...); empty or "latest" for the '
                                          'newest; "list" for the last 10.')] = "",
) -> str:
    """Status of long-running jobs (generated videos, songs, 3D scenes): running with the time left, done (with
    files and how Mo was told), or failed with the reason. Call it every time Mo asks about a job's progress,
    completion or failure, before answering, even if you think you know: never answer that from memory."""
    recs = await asyncio.to_thread(long_jobs.all_records)
    if job == "list":
        return "\n".join(long_jobs.describe(r) for r in recs[:10]) or "No long jobs yet."
    if not recs:
        raise RuntimeError("no long jobs have been started")
    rec = recs[0] if job in ("", "latest") else await asyncio.to_thread(long_jobs.load, long_jobs.check_job_id(job))
    return long_jobs.describe(rec)


@mcp.tool()
async def show_job_result(
    job: Annotated[str, Field(description='Job id of a finished long job; empty or "latest" for the newest one '
                                          'with a result.')] = "",
) -> str:
    """Show a finished long job's result where Mo asked: on the Echo it plays the job's video on the screen
    (stop it with stop_video); in the chat it returns MEDIA: lines, include every one in your reply unchanged,
    each on its own line. Only when Mo asks to see it."""
    origin = _turn_origin("show_job_result")
    recs = await asyncio.to_thread(long_jobs.all_records)
    if job in ("", "latest"):
        done = [r for r in recs if r["status"] == "done" and r["media"]]
        if not done:
            raise RuntimeError("no finished long job has a result to show")
        rec = done[0]
    else:
        rec = await asyncio.to_thread(long_jobs.load, long_jobs.check_job_id(job))
        if rec["status"] != "done":
            raise RuntimeError(long_jobs.describe(rec))
    if origin == "chat":
        return f"{rec['label']} (job {rec['job_id']}): {rec['summary']}\n{_media_lines(rec['media'])}"
    if rec.get("kind") == "vision":
        raise RuntimeError("Vision results are not shown on the Echo; ask in the chat")
    if not rec["echo_video"]:
        raise RuntimeError(f"{rec['label']} job {rec['job_id']} has no video to show on the Echo")
    played = await _play_local_video(rec["echo_video"], f"{rec['label']}"[:60], "show_job_result")
    return f"{played}: {rec['label']} (job {rec['job_id']})"


# ---- 3D scenes: a thin handoff to the 3D bot (Hermes profile "splat" on this gateway, /p/splat/). GlaDOS keeps
# nothing about scenes: create_splat records a long job (for the instant "working on it" and the done notice),
# starts the 3D bot's run with Mo's request word for word, and hermes_handoff.py (its own systemd user unit, so it
# outlives this process) relays the 3D bot's reply unchanged into the long job: its text as the summary and its
# MEDIA: files (screenshot, preview video, .ply) as the media. Mo can also talk to the 3D bot directly.
SPLAT_RUNS_URL = os.environ["HERMES_API_URL"].rstrip("/") + "/p/splat/v1/runs"
SPLAT_KEY_FILE = pathlib.Path.home() / ".hermes" / "profiles" / "splat" / ".env"
SPLAT_ETA_MIN = 10
HANDOFF = pathlib.Path(__file__).resolve().parent / "hermes_handoff.py"


def _start_handoff_relay(job_id, run_id):
    """Launch the relay as a transient systemd user unit (survives bridge exits and gateway restarts)."""
    cmd = ["systemd-run", "--user", "--collect", "--quiet", f"--unit=splat-handoff-{job_id}",
           "--property=Nice=10", "/usr/bin/python3", str(HANDOFF), "follow", job_id, run_id,
           "--runs-url", SPLAT_RUNS_URL, "--key-file", str(SPLAT_KEY_FILE)]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        raise RuntimeError(f"could not start the reply relay: {(r.stderr or r.stdout).strip()[-200:]}")


@mcp.tool()
async def create_splat(
    request: Annotated[str, Field(description="Mo's request about a 3D scene, in his words: a new scene, or a change "
                                              "to one he made (\"make the hut bigger\").")],
) -> str:
    """Hand Mo's request to make or change a 3D scene (a pixel-art Gaussian splat, a 3D model, a diorama) to the
    3D bot, word for word. It takes a few minutes, so this returns at once and already gives the "working on it"
    message to say: reply with exactly that right away and call no other tool. When the 3D bot answers, its reply
    is relayed to Mo unchanged (spoken notice on the Echo; in the chat its text plus the screenshot, preview video
    and .ply). show_job_result shows a finished one again (on the Echo it plays the preview video)."""
    origin = _job_origin("create_splat")
    text = (request or "").strip()
    if not text:
        raise ValueError("request is required: what Mo asked for, in his words")
    job = await asyncio.to_thread(long_jobs.new_job, "splat", "3D scene", origin, SPLAT_ETA_MIN, "3D bot handoff")
    jid = job["job_id"]
    try:
        run_id = await asyncio.to_thread(hermes_handoff.start_run, SPLAT_RUNS_URL, SPLAT_KEY_FILE, text)
        await asyncio.to_thread(_start_handoff_relay, jid, run_id)
    except Exception as e:
        _log("splat", jid, "ERROR", e)
        if (await asyncio.to_thread(long_jobs.load, jid))["status"] == "running":
            await asyncio.to_thread(long_jobs.fail, jid, f"could not hand the request to the 3D bot: {e}")
        raise RuntimeError(f"could not hand the request to the 3D bot: {e}") from None
    _log("splat", jid, "handed to the 3D bot, run", run_id)
    say = await _acknowledge(origin, "Your 3D scene", SPLAT_ETA_MIN)
    return f"3D scene job {jid} handed to the 3D bot.\n{say}"


# ---- Video clips: a thin handoff to Vision (Hermes profile "vision" on this gateway, /p/vision/). GlaDOS keeps
# nothing about annotation: ask_vision records a long job (for the instant "working on it" and the done notice),
# starts Vision's run with Mo's request word for word (including any attachment path), and hermes_handoff.py
# (its own systemd user unit, so it outlives this process) relays Vision's reply unchanged into the long job:
# its text as the summary (including Viewer: lines) and its MEDIA: files as the media. Not connected to the Echo:
# an Echo turn is refused and no job is started. Mo can also talk to Vision directly.
VISION_RUNS_URL = os.environ["HERMES_API_URL"].rstrip("/") + "/p/vision/v1/runs"
VISION_KEY_FILE = pathlib.Path.home() / ".hermes" / "profiles" / "vision" / ".env"
VISION_ETA_MIN = 5


def _start_vision_handoff_relay(job_id, run_id):
    """Launch the Vision reply relay as a transient systemd user unit (survives bridge exits and gateway restarts)."""
    cmd = ["systemd-run", "--user", "--collect", "--quiet", f"--unit=vision-handoff-{job_id}",
           "--property=Nice=10", "/usr/bin/python3", str(HANDOFF), "follow", job_id, run_id,
           "--runs-url", VISION_RUNS_URL, "--key-file", str(VISION_KEY_FILE)]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        raise RuntimeError(f"could not start the reply relay: {(r.stderr or r.stdout).strip()[-200:]}")


@mcp.tool()
async def ask_vision(
    request: Annotated[str, Field(description="Mo's request about a video clip, in his words: annotate, segment, "
                                              "track, pose, depth-estimate or describe it, including any "
                                              "attachment path or URL.")],
) -> str:
    """Hand Mo's request to annotate, segment, track, pose, depth-estimate or describe a video clip to Vision,
    word for word, including any attachment path. It takes a few minutes, so this returns at once and already
    gives the "working on it" message to say: reply with exactly that right away and call no other tool. When
    Vision answers, its reply is relayed to Mo unchanged (in the chat its text, including every Viewer: and
    MEDIA: line). This is not on the Echo: refused on an Echo turn. show_job_result shows a finished one again
    in the chat. Mo can also talk to Vision directly."""
    origin = _job_origin("ask_vision")
    if origin["kind"] == "echo":
        raise RuntimeError("Vision is not connected to the Echo; ask in the chat")
    text = (request or "").strip()
    if not text:
        raise ValueError("request is required: what Mo asked for, in his words, including any attachment path")
    job = await asyncio.to_thread(long_jobs.new_job, "vision", "video annotation", origin, VISION_ETA_MIN,
                                  "Vision handoff")
    jid = job["job_id"]
    try:
        run_id = await asyncio.to_thread(hermes_handoff.start_run, VISION_RUNS_URL, VISION_KEY_FILE, text)
        await asyncio.to_thread(_start_vision_handoff_relay, jid, run_id)
    except Exception as e:
        _log("vision", jid, "ERROR", e)
        if (await asyncio.to_thread(long_jobs.load, jid))["status"] == "running":
            await asyncio.to_thread(long_jobs.fail, jid, f"could not hand the request to Vision: {e}")
        raise RuntimeError(f"could not hand the request to Vision: {e}") from None
    _log("vision", jid, "handed to Vision, run", run_id)
    say = await _acknowledge(origin, "Your video annotation", VISION_ETA_MIN)
    return f"video annotation job {jid} handed to Vision.\n{say}"


# ---- Audio: a thin handoff to Synth (Hermes profile "synth" on this gateway, /p/synth/). GlaDOS keeps
# nothing about generation: ask_synth records a long job (for the instant "working on it" and the done notice),
# starts Synth's run with Mo's request word for word (including any attachment path), and hermes_handoff.py
# (its own systemd user unit, so it outlives this process) relays Synth's reply unchanged into the long job:
# its text as the summary (including Viewer: lines) and its MEDIA: files as the media. Not connected to the Echo:
# an Echo turn is refused (a short line is spoken: songs are in the chat with Synth now) and no job is started.
# Mo can also talk to Synth directly.
SYNTH_RUNS_URL = os.environ["HERMES_API_URL"].rstrip("/") + "/p/synth/v1/runs"
SYNTH_KEY_FILE = pathlib.Path.home() / ".hermes" / "profiles" / "synth" / ".env"
SYNTH_ETA_MIN = 5
_SYNTH_ECHO_REFUSAL = "Songs are in the chat with Synth now."


def _start_synth_handoff_relay(job_id, run_id):
    """Launch the Synth reply relay as a transient systemd user unit (survives bridge exits and gateway restarts)."""
    cmd = ["systemd-run", "--user", "--collect", "--quiet", f"--unit=synth-handoff-{job_id}",
           "--property=Nice=10", "/usr/bin/python3", str(HANDOFF), "follow", job_id, run_id,
           "--runs-url", SYNTH_RUNS_URL, "--key-file", str(SYNTH_KEY_FILE)]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        raise RuntimeError(f"could not start the reply relay: {(r.stderr or r.stdout).strip()[-200:]}")


@mcp.tool()
async def ask_synth(
    request: Annotated[str, Field(description="Mo's request for Synth (songs, speech, cloned or designed voices, sound "
                                              "effects, stems, images, LTX-2.5 video clips, slideshows/presentations, "
                                              "storyboards), in his words, including any attachment path.")],
) -> str:
    """Hand Mo's request to Synth, his media generation bot, word for word, including any attachment path: songs,
    speech, cloned or designed voices, sound effects, stems, and (since 20 Nov) images, LTX-2.5 video clips,
    slideshows/presentations with narration and music, and storyboards (shot list, keyframes, contact sheet). Use it
    when Mo asks for Synth or for a presentation, storyboard or multi-step media piece; your own generate_picture and
    generate_video stay for quick single pictures and clips. It can take a few minutes, so this returns at once and already gives the
    "working on it" message to say: reply with exactly that right away and call no other tool. When Synth
    answers, its reply is relayed to Mo unchanged (in the chat its text, including every Viewer: and MEDIA: line).
    This is not on the Echo: refused on an Echo turn, which already speaks that songs are in the chat with Synth.
    show_job_result shows a finished one again in the chat. Mo can also talk to Synth directly."""
    origin = _job_origin("ask_synth")
    if origin["kind"] == "echo":
        # Spoken here because an Echo voice turn can be cut before GlaDOS can relay a raised error.
        # No job is started. Mo asked for this short line; the handoff spec only raised RuntimeError.
        await _speak_on_echo(_SYNTH_ECHO_REFUSAL)
        return (
            f'The Echo has already said aloud: "{_SYNTH_ECHO_REFUSAL}" '
            'Reply with just "On it." and call no other tool.'
        )
    text = (request or "").strip()
    if not text:
        raise ValueError("request is required: what Mo asked for, in his words, including any attachment path")
    job = await asyncio.to_thread(long_jobs.new_job, "synth", "audio", origin, SYNTH_ETA_MIN, "Synth handoff")
    jid = job["job_id"]
    try:
        run_id = await asyncio.to_thread(hermes_handoff.start_run, SYNTH_RUNS_URL, SYNTH_KEY_FILE, text)
        await asyncio.to_thread(_start_synth_handoff_relay, jid, run_id)
    except Exception as e:
        _log("synth", jid, "ERROR", e)
        if (await asyncio.to_thread(long_jobs.load, jid))["status"] == "running":
            await asyncio.to_thread(long_jobs.fail, jid, f"could not hand the request to Synth: {e}")
        raise RuntimeError(f"could not hand the request to Synth: {e}") from None
    _log("synth", jid, "handed to Synth, run", run_id)
    say = await _acknowledge(origin, "Your audio", SYNTH_ETA_MIN)
    return f"audio job {jid} handed to Synth.\n{say}"


@mcp.tool()
async def play_podcast(query: str, episode: str = "") -> str:
    """Play a free podcast episode on the Echo speaker.
    query is the podcast's name (looked up in the Apple Podcasts directory) or its RSS feed URL.
    episode is empty for the newest episode, or words from the episode title to pick an older one.
    MP3 episodes only (the Echo speaker cannot play AAC/M4A). Stop it with stop_ringing."""
    info = await asyncio.to_thread(_podcast_resolve, query, episode)
    played = await _play_audio_confirmed(info["url"])
    _log("podcast", info["show"], info["episode"], played)
    by = f" by {info['author']}" if info["author"] else ""
    return f"{played}: {info['show']}{by}, episode {info['episode']!r}, {_duration_words(info['duration'])}"


@mcp.tool()
async def play_audiobook(query: str, chapter: int = 1) -> str:
    """Play a free public-domain audiobook (LibriVox, via archive.org) on the Echo speaker.
    query is the book title and/or author. chapter is the chapter (section) number, default 1;
    each call plays one chapter, so ask for the next number to continue. The most downloaded
    LibriVox recording that matches is used. Stop it with stop_ringing."""
    info = await asyncio.to_thread(_audiobook_resolve, query, chapter)
    played = await _play_audio_confirmed(info["url"])
    _log("audiobook", info["title"], info["chapter"], played)
    by = f" by {info['author']}" if info["author"] else ""
    return (
        f"{played}: {info['title']}{by} (LibriVox), chapter {info['chapter']} of {info['chapters']}: "
        f"{info['chapter_title']}, {_duration_words(info['duration'])}"
    )


if __name__ == "__main__":
    mcp.run()
