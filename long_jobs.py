"""Generic long-running job registry for the Echo bridge: origin, ETA, result, and notify-on-done.

A job is one JSON file in JOBS_DIR (~/.hermes/long-jobs/<job_id>.json):
  job_id, kind, label, origin {kind: echo|chat, session_id, source, deliverable}, eta_min, submitted_at,
  status (running|done|failed), summary, media [absolute file paths], echo_video (one of media, an .mp4 to
  offer on the Echo), error, finished_at, notified {how, at} | null, detail (free text for the producer).

Producers:
  * the bridge's Dell media jobs (generate_video) via new_job()/finish()/fail();
  * any other local agent (e.g. a future splat agent): a bridge tool creates the job with the turn's origin
    and hands the job_id to the agent, which reports back with
        python3 long_jobs.py finish <job_id> --summary "..." --media /abs/a.png /abs/b.mp4 [--echo-video /abs/b.mp4]
        python3 long_jobs.py fail <job_id> --error "..."
The hermes-gateway echo MCP that owns JOBS_DIR/.notifier.lock sends each finished job's notice
exactly once (spoken on the Echo, or queued for the origin chat) and records how in "notified".
hermes-dashboard also starts this MCP for its Chat tab, but must not run the notifier: a
dashboard-owned lock survived gateway restarts and kept sending notices from stale code.
"""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import pathlib
import re
import secrets
import sys
import time

JOBS_DIR = pathlib.Path.home() / ".hermes" / "long-jobs"
POLL_S = 10
STATUSES = ("running", "done", "failed")
_ID_RE = re.compile(r"^[0-9a-f]{16}$")

_notifier_lock_fd = None
_notifier_task = None
_GATEWAY_CGROUP = "hermes-gateway.service"
_DASHBOARD_CGROUP = "hermes-dashboard.service"


def argv_after_python_c(cmdline: bytes) -> list[str]:
    """Argv tokens after a `python -c <script>` body, or the full argv if there is no -c."""
    argv = [a.decode("utf-8", "replace") for a in cmdline.split(b"\0") if a]
    try:
        i = argv.index("-c")
        return argv[i + 2:]
    except ValueError:
        return argv


def process_is_hermes_gateway(cgroup: str, cmdline: bytes):
    """True if this process is hermes-gateway, False if it is hermes-dashboard, else None (keep walking).

    The systemd cgroup is the production signal. The hermes launcher is `python -c <bootstrap>
    gateway run` or `... dashboard ...`; the bootstrap script itself can mention those words, so
    only the tokens after -c are used.
    """
    cgroup = cgroup or ""
    if _GATEWAY_CGROUP in cgroup:
        return True
    if _DASHBOARD_CGROUP in cgroup:
        return False
    tokens = argv_after_python_c(cmdline)
    if "dashboard" in tokens:
        return False
    if "gateway" in tokens and "run" in tokens:
        return True
    return None


def _read_proc(pid: int):
    cgroup = pathlib.Path(f"/proc/{pid}/cgroup").read_text()
    cmdline = pathlib.Path(f"/proc/{pid}/cmdline").read_bytes()
    stat = pathlib.Path(f"/proc/{pid}/stat").read_text()
    ppid = int(stat[stat.rfind(")") + 2:].split()[1])
    return cgroup, cmdline, ppid


def launched_by_hermes_gateway(start_pid: int | None = None, *, _read=None) -> bool:
    """True when this MCP was started by hermes-gateway, not hermes-dashboard.

    Both services launch the same echo MCP from config.yaml. Only the gateway's process may
    own .notifier.lock, so a dashboard-spawned MCP cannot keep sending notices from stale
    code after a gateway restart. If the parent cannot be identified, this is False
    (do not start the notifier).
    """
    read = _read or _read_proc
    pid = os.getpid() if start_pid is None else start_pid
    seen: set[int] = set()
    for _ in range(8):
        if not pid or pid <= 1 or pid in seen:
            return False
        seen.add(pid)
        try:
            cgroup, cmdline, ppid = read(pid)
        except (OSError, ValueError, IndexError):
            return False
        verdict = process_is_hermes_gateway(cgroup, cmdline)
        if verdict is not None:
            return verdict
        pid = ppid
    return False



def check_job_id(job_id: str) -> str:
    if not isinstance(job_id, str) or not _ID_RE.match(job_id):
        raise ValueError(f"not a long-job id: {job_id!r}")
    return job_id


def _path(job_id: str) -> pathlib.Path:
    return JOBS_DIR / f"{check_job_id(job_id)}.json"


class _Lock:
    """Machine-wide lock for read-modify-write of job files."""
    def __enter__(self):
        JOBS_DIR.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(str(JOBS_DIR / ".jobs.lock"), os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        os.close(self.fd)


def _save(rec: dict) -> None:
    p = _path(rec["job_id"])
    tmp = p.with_name(f".{p.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(rec, indent=1))
    os.replace(tmp, p)


def load(job_id: str) -> dict:
    p = _path(job_id)
    if not p.is_file():
        raise RuntimeError(f"no long job {job_id} (nothing at {p})")
    return json.loads(p.read_text())


def _check_origin(origin: dict) -> None:
    if not isinstance(origin, dict) or origin.get("kind") not in ("echo", "chat") or not origin.get("session_id") \
            or not isinstance(origin.get("deliverable"), bool):
        raise ValueError(f"origin must be {{kind: echo|chat, session_id, source, deliverable}}, got {origin!r}")


def new_job(kind: str, label: str, origin: dict, eta_min: float, detail: str = "") -> dict:
    """Record a started job. label is how Mo hears it ("song", "video", "3D scene")."""
    _check_origin(origin)
    if not kind or not label:
        raise ValueError("kind and label are required")
    if not isinstance(eta_min, (int, float)) or isinstance(eta_min, bool) or eta_min <= 0:
        raise ValueError(f"eta_min must be a positive number of minutes, got {eta_min!r}")
    rec = {"job_id": secrets.token_hex(8), "kind": kind, "label": label, "origin": origin, "eta_min": eta_min,
           "submitted_at": time.time(), "status": "running", "summary": "", "media": [], "echo_video": "",
           "error": "", "finished_at": None, "notified": None, "detail": detail}
    with _Lock():
        _save(rec)
    return rec


def _update(job_id: str, **changes) -> dict:
    with _Lock():
        rec = load(job_id)
        if rec["status"] != "running":
            raise RuntimeError(f"long job {job_id} is already {rec['status']}")
        rec.update(changes, finished_at=time.time())
        _save(rec)
    return rec


def finish(job_id: str, summary: str = "", media=(), echo_video: str = "", source_video_id: str = "") -> dict:
    """Mark a job done. media: absolute paths of existing files to deliver; echo_video: one of them (.mp4).
    source_video_id: optional 32-hex Dell /media/<id>.mp4 id, so a later extend/restyle can pass source=previous."""
    media = [str(m) for m in media]
    for m in media:
        if not os.path.isabs(m) or not os.path.isfile(m):
            raise RuntimeError(f"result file is missing or not an absolute path: {m}")
    if echo_video and echo_video not in media:
        raise ValueError("echo_video must be one of the media paths")
    if echo_video and not echo_video.lower().endswith(".mp4"):
        raise ValueError("echo_video must be an .mp4 file")
    changes = dict(status="done", summary=summary, media=media, echo_video=echo_video)
    if source_video_id:
        if not isinstance(source_video_id, str) or not re.fullmatch(r"[0-9a-f]{32}", source_video_id):
            raise ValueError(f"source_video_id must be a 32-hex Dell media id, got {source_video_id!r}")
        changes["source_video_id"] = source_video_id
    return _update(job_id, **changes)


def fail(job_id: str, error: str, media=()) -> dict:
    """Mark a job failed. error is the text to relay (the bot's reply, or why there was none).
    media: existing files named in that reply, so Viewer/MEDIA can still be delivered."""
    if not error:
        raise ValueError("a failed job needs its error text")
    media = [str(m) for m in media]
    for m in media:
        if not os.path.isabs(m) or not os.path.isfile(m):
            raise RuntimeError(f"result file is missing or not an absolute path: {m}")
    return _update(job_id, status="failed", error=str(error), media=media)


def all_records() -> list[dict]:
    if not JOBS_DIR.is_dir():
        return []
    recs = [json.loads(p.read_text()) for p in JOBS_DIR.glob("*.json") if _ID_RE.match(p.stem)]
    return sorted(recs, key=lambda r: r["submitted_at"], reverse=True)


def pending_notices() -> list[dict]:
    return [r for r in all_records() if r["status"] != "running" and r["notified"] is None]


def mark_notified(job_id: str, how: str) -> None:
    with _Lock():
        rec = load(job_id)
        rec["notified"] = {"how": how, "at": time.time()}
        _save(rec)


def describe(rec: dict) -> str:
    age = (time.time() - rec["submitted_at"]) / 60
    head = f"{rec['label']} job {rec['job_id']} ({rec['kind']}, started {age:.0f} min ago)"
    if rec["status"] == "running":
        left = rec["eta_min"] - age
        return f"{head} is running; expected in about {max(1, round(left))} min" if left > 0 else \
            f"{head} is running, {round(-left)} min past its estimate of {rec['eta_min']:g} min"
    if rec["status"] == "failed":
        return f"{head} FAILED: {rec['error']}"
    files = "".join(f"\n  {m}" for m in rec["media"])
    told = f" Mo was told: {rec['notified']['how']}." if rec["notified"] else " Notice not sent yet."
    return f"{head} is done. {rec['summary']}{told}" + (f"\nFiles:{files}" if files else "")


def _notifier_lock_acquire() -> bool:
    global _notifier_lock_fd
    if _notifier_lock_fd is not None:
        return True
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(JOBS_DIR / ".notifier.lock"), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return False
    os.ftruncate(fd, 0)
    os.write(fd, b"%d\n" % os.getpid())
    _notifier_lock_fd = fd
    return True


async def send_notices(log, notify) -> None:
    """Send each finished job's notice once via notify(rec) -> how. A failed send is logged and retried."""
    for rec in await asyncio.to_thread(pending_notices):
        jid = rec["job_id"]
        try:
            how = await notify(rec)
        except Exception as e:
            log("long-job", jid, "ERROR sending the completion notice (will retry): %s" % e)
            continue
        await asyncio.to_thread(mark_notified, jid, how)
        log("long-job", jid, "completion notice sent:", how)


async def notifier_loop(log, notify, poll_s=POLL_S) -> None:
    owned = False
    while True:
        try:
            if not owned:
                owned = await asyncio.to_thread(_notifier_lock_acquire)
                if owned:
                    log("long-job", "notifier: this process (pid %d) owns %s" % (os.getpid(), JOBS_DIR / ".notifier.lock"))
            if owned:
                await send_notices(log, notify)
        except Exception as e:
            log("long-job", "notifier ERROR: %s" % e)
        await asyncio.sleep(poll_s)


def ensure_notifier(log, notify):
    global _notifier_task
    if _notifier_task is not None and not _notifier_task.done():
        return _notifier_task
    _notifier_task = asyncio.get_running_loop().create_task(notifier_loop(log, notify), name="long-job-notifier")
    return _notifier_task


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Report a long job's result to the Echo bridge.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("finish")
    f.add_argument("job_id")
    f.add_argument("--summary", default="")
    f.add_argument("--media", nargs="*", default=[])
    f.add_argument("--echo-video", default="")
    x = sub.add_parser("fail")
    x.add_argument("job_id")
    x.add_argument("--error", required=True)
    s = sub.add_parser("show")
    s.add_argument("job_id", nargs="?")
    a = ap.parse_args(argv)
    if a.cmd == "finish":
        rec = finish(a.job_id, a.summary, a.media, a.echo_video)
    elif a.cmd == "fail":
        rec = fail(a.job_id, a.error)
    else:
        recs = [load(a.job_id)] if a.job_id else all_records()[:10]
        print("\n".join(describe(r) for r in recs) or "no long jobs")
        return 0
    print(describe(rec))
    return 0


if __name__ == "__main__":
    sys.exit(main())
