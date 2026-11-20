"""LAN-only video relay for the Echo: serves a finished local MP4 (a splat preview, a job result, a replay)
as a complete VOD HLS playlist. ffmpeg remuxes it with -c copy (#EXT-X-ENDLIST, every segment kept), so the
Echo plays it from the start; ffmpeg then exits 0 and the files stay served until idle or stop.
Served on RELAY_BIND only (never 0.0.0.0). Local files only: there is no network source, no transcode and
no refresh (the Twitch live/VOD relay was removed on 2026-11-20).

Lifecycle (one relay per machine, across every bridge process):
- start() first stops any running relay (this process's directly; another bridge process's through its
  control URL, whose secret is in the 0600 owner file), waits for the port to be free, removes stale
  /tmp/video-relay-* dirs, and only then binds.
- The relay stops itself when the Echo has fetched nothing for IDLE_STOP_S. stop_video still stops it at once.
"""
from __future__ import annotations

import atexit
import fcntl
import json
import os
import secrets
import shutil
import signal
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote
import urllib.request

RELAY_BIND = os.environ["BRIDGE_BIND"]
RELAY_PORT = 8644
_PLAYLIST = "index.m3u8"
_WAIT_PLAYLIST_S = 45
_FFMPEG_LOGLEVEL = "error"
# The Echo's HLS player re-fetches the playlist every ~2 s while playing; 60 s with no
# fetch means playback ended or was stopped on the device.
IDLE_STOP_S = 60
_WATCHDOG_TICK_S = 5
_FOREIGN_STOP_WAIT_S = 15
_TMP_DIR = Path("/tmp")
_WORKDIR_PREFIX = "video-relay-"
_OWNER_FILE = _TMP_DIR / "video-relay.owner.json"
_START_LOCK_FILE = _TMP_DIR / "video-relay.lock"

_lock = threading.Lock()
_state = None  # RelayState | None


def _ffmpeg_bin() -> str:
    path = shutil.which("ffmpeg")
    if not path:
        raise RuntimeError("ffmpeg is not installed on this machine (needed for the video relay)")
    return path


class RelayState:
    def __init__(self, bind: str, port: int, token: str, workdir: Path):
        self.bind = bind
        self.port = port
        self.token = token
        self.workdir = workdir
        self.ctl = secrets.token_urlsafe(24)
        self.served_at = None  # monotonic time start() handed the URL out
        self.last_fetch = None  # monotonic time of the last file served
        self.watchdog = None
        self.httpd = None
        self.http_thread = None
        self.ffmpeg = None
        self.supervisor = None
        self.stop_event = threading.Event()
        self.kind = ""
        self.title = ""
        self.source = ""
        self.transcode = False
        self.file_complete = False
        self.error = None
        self.started = time.monotonic()

    @property
    def url(self) -> str:
        return f"http://{self.bind}:{self.port}/{self.token}/{_PLAYLIST}"

    @property
    def playlist_path(self) -> Path:
        return self.workdir / _PLAYLIST


class _RelayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    def _safe_file(self) -> Path | None:
        raw = unquote(self.path.split("?", 1)[0])
        # /<token>/<filename>
        parts = [p for p in raw.split("/") if p]
        if len(parts) != 2 or parts[0] != self.server.relay.token:
            return None
        name = parts[1]
        if name in (".", "..") or "/" in name or "\\" in name or not name:
            return None
        path = (self.server.relay.workdir / name).resolve()
        try:
            path.relative_to(self.server.relay.workdir.resolve())
        except ValueError:
            return None
        return path if path.is_file() else None

    def do_HEAD(self):
        self._send(body=False)

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, BrokenPipeError, TimeoutError):
            return

    def do_GET(self):
        self._send(body=True)

    def do_POST(self):
        relay = self.server.relay
        parts = [p for p in unquote(self.path.split("?", 1)[0]).split("/") if p]
        if parts != [relay.ctl, "stop"]:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()
        threading.Thread(
            target=_stop_if_current, args=(relay, self.server.relay_log, "relay stop requested by another bridge process"),
            name="video-relay-ctl-stop", daemon=True,
        ).start()

    def _send(self, body: bool):
        path = self._safe_file()
        if path is None:
            self.send_error(404)
            return
        suffix = path.suffix.lower()
        if suffix in (".m3u8", ".m3u"):
            ctype = "application/vnd.apple.mpegurl"
        elif suffix in (".ts", ".m2ts"):
            ctype = "video/mp2t"
        else:
            self.send_error(404)
            return
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.server.relay.last_fetch = time.monotonic()
        if body:
            self.wfile.write(data)


# Measured on this machine (i7-2620M, 2C/4T) 2026-11-15 with sharpkorners 1080p source:
# ~1.1 cores, keeps realtime (speed ~1.1x on a live input), output 1280x720 H.264 High + AAC.
def _ffmpeg_file_cmd(source: str, playlist: Path) -> list[str]:
    """Remux a local H.264 MP4 into a complete VOD HLS playlist (no re-encode)."""
    return [
        _ffmpeg_bin(),
        "-hide_banner", "-nostdin", "-loglevel", _FFMPEG_LOGLEVEL,
        "-protocol_whitelist", "file",
        "-i", source,
        "-map", "0:v:0", "-map", "0:a:0?",
        "-c", "copy",
        "-f", "hls",
        "-hls_time", "2",
        "-hls_list_size", "0",
        "-hls_playlist_type", "vod",
        "-hls_allow_cache", "0",
        str(playlist),
    ]


def _spawn_ffmpeg(source: str, playlist: Path, log, transcode: bool = False, kind: str = ""):
    if kind != "file":
        raise RuntimeError("the video relay serves local files only, not %r" % (kind,))
    cmd = _ffmpeg_file_cmd(source, playlist)
    log("ffmpeg start hls vod copy %s -> %s" % (Path(source).name, playlist.name))
    return __import__("subprocess").Popen(
        cmd,
        stdin=__import__("subprocess").DEVNULL,
        stdout=__import__("subprocess").DEVNULL,
        stderr=__import__("subprocess").PIPE,
        start_new_session=True,
    )


def _supervisor(state: RelayState, refresh, log):
    source = state.source
    while not state.stop_event.is_set():
        try:
            proc = _spawn_ffmpeg(source, state.playlist_path, log, state.transcode, state.kind)
        except Exception as exc:
            state.error = str(exc)
            log("ffmpeg spawn failed: %s" % exc)
            return
        with _lock:
            if _state is state:
                state.ffmpeg = proc
        stderr_tail = []

        def _drain():
            try:
                raw = proc.stderr.read() if proc.stderr else b""
            except Exception:
                return
            text = raw.decode("utf-8", "replace").strip()
            if text:
                stderr_tail.append(text[-1000:])

        t = threading.Thread(target=_drain, name="video-ffmpeg-stderr", daemon=True)
        t.start()
        while not state.stop_event.is_set():
            rc = proc.poll()
            if rc is not None:
                t.join(timeout=1)
                break
            state.stop_event.wait(0.5)
        else:
            _kill_proc(proc)
            t.join(timeout=1)
            return
        if state.stop_event.is_set():
            return
        if state.kind == "file":
            if proc.returncode == 0:
                log("file playlist complete; serving until idle or stop")
                state.file_complete = True
            else:
                state.error = "relay ffmpeg exited (%s) on %s: %s" % (
                    proc.returncode, Path(source).name, (stderr_tail[-1:] or [""])[0][:300])
            return
        state.error = "relay ffmpeg exited (%s): %s" % (proc.returncode, (stderr_tail[-1:] or [""])[0][:300])
        return


def _kill_proc(proc):
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.terminate()
        except OSError:
            pass
    try:
        proc.wait(timeout=5)
    except Exception:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        try:
            proc.wait(timeout=2)
        except Exception:
            pass


def _playlist_ready(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    return "#EXTINF" in text and ".ts" in text


def current_url() -> str | None:
    with _lock:
        return None if _state is None else _state.url


def is_relay_url(url: str) -> bool:
    cur = current_url()
    return bool(cur and url and (url == cur or url.startswith(cur.rsplit("/", 1)[0] + "/")))


def _stop_locked(log):
    global _state
    state = _state
    _state = None
    if state is None:
        return
    state.stop_event.set()
    _kill_proc(state.ffmpeg)
    if state.httpd is not None:
        try:
            state.httpd.shutdown()
        except Exception:
            pass
        try:
            state.httpd.server_close()
        except Exception:
            pass
    if state.http_thread is not None:
        state.http_thread.join(timeout=3)
    if state.supervisor is not None:
        state.supervisor.join(timeout=6)
    if state.watchdog is not None and state.watchdog is not threading.current_thread():
        state.watchdog.join(timeout=_WATCHDOG_TICK_S + 1)
    if state.ffmpeg is not None and state.ffmpeg.poll() is None:
        raise RuntimeError("video relay ffmpeg pid %s did not exit" % state.ffmpeg.pid)
    _clear_owner(state)
    shutil.rmtree(state.workdir, ignore_errors=True)
    log("relay stopped")


def _stop_if_current(state: RelayState, log, reason: str):
    with _lock:
        if _state is not state:
            return
        log(reason)
        _stop_locked(log)


def _watchdog(state: RelayState, log):
    """Stop the relay when the Echo has fetched nothing for IDLE_STOP_S since the URL was handed out."""
    while not state.stop_event.wait(_WATCHDOG_TICK_S):
        if state.served_at is None:
            continue
        idle = time.monotonic() - max(state.served_at, state.last_fetch or state.served_at)
        if idle >= IDLE_STOP_S:
            _stop_if_current(state, log, "relay idle %ds (no Echo fetches); stopping" % idle)
            return


def _write_owner(state: RelayState):
    data = json.dumps({"pid": os.getpid(), "bind": state.bind, "port": state.port,
                       "ctl": state.ctl, "workdir": str(state.workdir)})
    tmp = _OWNER_FILE.with_name(_OWNER_FILE.name + ".%d" % os.getpid())
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(data)
    os.replace(tmp, _OWNER_FILE)


def _read_owner() -> dict | None:
    try:
        return json.loads(_OWNER_FILE.read_text())
    except FileNotFoundError:
        return None


def _clear_owner(state: RelayState):
    owner = _read_owner()
    if owner and owner.get("pid") == os.getpid() and owner.get("ctl") == state.ctl:
        _OWNER_FILE.unlink()


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _stop_foreign(log):
    """Stop a relay owned by another bridge process (via its control URL) and wait until it is gone."""
    owner = _read_owner()
    if owner is None:
        return
    pid = int(owner["pid"])
    if pid == os.getpid() or not _pid_alive(pid):
        _OWNER_FILE.unlink()
        return
    url = "http://%s:%s/%s/stop" % (owner["bind"], owner["port"], owner["ctl"])
    log("stopping the video relay of bridge pid %s" % pid)
    req = urllib.request.Request(url, data=b"", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            if r.status != 200:
                raise RuntimeError("HTTP %s" % r.status)
    except Exception as exc:
        raise RuntimeError("could not stop the video relay of bridge pid %s: %s" % (pid, exc)) from exc
    deadline = time.monotonic() + _FOREIGN_STOP_WAIT_S
    while _OWNER_FILE.exists():
        if time.monotonic() > deadline:
            raise RuntimeError("video relay of bridge pid %s did not stop within %ds" % (pid, _FOREIGN_STOP_WAIT_S))
        time.sleep(0.2)


def _clean_stale_dirs(log):
    """Remove every /tmp/video-relay-* dir of this user. Only called once no relay is running."""
    uid = os.getuid()
    for d in _TMP_DIR.glob(_WORKDIR_PREFIX + "*"):
        if d.is_dir() and not d.is_symlink() and d.stat().st_uid == uid:
            shutil.rmtree(d)
            log("removed stale relay dir %s" % d.name)


def stop(log=None):
    log = log or (lambda *_: None)
    with _lock:
        _stop_locked(log)


def start(source: str, *, kind: str, title: str = "", transcode: bool = False, refresh=None, log=None,
          bind: str = RELAY_BIND, port: int = RELAY_PORT) -> str:
    """Start (or replace) the relay. Returns the LAN-only playlist URL. Never binds 0.0.0.0.
    Local MP4 files only (kind="file"), remuxed with -c copy."""
    if bind in ("0.0.0.0", "::", "", "127.0.0.1"):
        raise RuntimeError("video relay bind must be the LAN address, not %r" % (bind,))
    if kind != "file":
        raise RuntimeError("the video relay serves local files only, not %r" % (kind,))
    if transcode or refresh:
        raise RuntimeError("a file relay is copy-only and has no refresh")
    if not source or not Path(source).is_absolute() or not Path(source).is_file():
        raise RuntimeError("relay file is missing: %r" % (source,))
    log = log or (lambda *_: None)
    _ffmpeg_bin()
    with open(_START_LOCK_FILE, "a") as lockf:
        fcntl.flock(lockf, fcntl.LOCK_EX)
        try:
            return _start_locked(source, kind=kind, title=title, transcode=transcode, refresh=refresh,
                                 log=log, bind=bind, port=port)
        finally:
            fcntl.flock(lockf, fcntl.LOCK_UN)


def _start_locked(source, *, kind, title, transcode, refresh, log, bind, port) -> str:
    global _state
    # Stop whatever relay is running (here or in another bridge process) before binding.
    stop(log)
    _stop_foreign(log)
    _clean_stale_dirs(log)
    token = secrets.token_urlsafe(24)
    workdir = Path(tempfile.mkdtemp(prefix=_WORKDIR_PREFIX, dir=_TMP_DIR))
    try:
        os.chmod(workdir, 0o700)
    except OSError:
        pass
    state = RelayState(bind, port, token, workdir)
    state.kind = kind
    state.title = title
    state.source = source
    state.transcode = bool(transcode)

    class _Server(ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True

    try:
        httpd = _Server((bind, port), _RelayHandler)
    except OSError as exc:
        shutil.rmtree(workdir, ignore_errors=True)
        raise RuntimeError(f"cannot bind video relay on {bind}:{port}: {exc}") from exc
    httpd.relay = state
    httpd.relay_log = log
    # Belt and braces: the socket must be on the LAN address.
    bound_ip, bound_port = httpd.server_address[:2]
    if bound_ip != bind or bound_port != port:
        httpd.server_close()
        shutil.rmtree(workdir, ignore_errors=True)
        raise RuntimeError(f"video relay bound {bound_ip}:{bound_port}, expected {bind}:{port}")
    state.httpd = httpd

    with _lock:
        if _state is not None:
            raise RuntimeError("a video relay started while this one was binding")
        _state = state
        _write_owner(state)
        state.http_thread = threading.Thread(target=httpd.serve_forever, name="video-relay-http", daemon=True)
        state.supervisor = threading.Thread(
            target=_supervisor, args=(state, refresh, log), name="video-relay-ffmpeg", daemon=True)
        state.watchdog = threading.Thread(
            target=_watchdog, args=(state, log), name="video-relay-watchdog", daemon=True)
        state.http_thread.start()
        state.supervisor.start()
        state.watchdog.start()

    deadline = time.monotonic() + _WAIT_PLAYLIST_S
    while time.monotonic() < deadline:
        ready = state.file_complete if kind == "file" else _playlist_ready(state.playlist_path)
        if ready and _playlist_ready(state.playlist_path):
            state.served_at = time.monotonic()
            log("relay ready %s (stops after %ds with no Echo fetches)" % (state.url, IDLE_STOP_S))
            return state.url
        if state.error:
            err = state.error
            stop(log)
            raise RuntimeError(err)
        if state.stop_event.is_set():
            stop(log)
            raise RuntimeError("video relay stopped before the playlist was ready")
        time.sleep(0.2)
    stop(log)
    raise RuntimeError(f"video relay did not produce a playlist within {_WAIT_PLAYLIST_S}s")


atexit.register(stop)
