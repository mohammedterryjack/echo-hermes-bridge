#!/usr/bin/env python3
"""Is oldmac idle enough to restart the Hermes gateway or dashboard? Prints the evidence, then IDLE or BUSY: <reasons>.
Read-only and stateless: every run probes live sources afresh and nothing is remembered between runs, so a failed
probe makes only that run BUSY ("probe failed"), never a later one. Never prints keys.
Rules: Hermes live work 0 (live /health/detailed, not the gateway_state.json snapshot), no live turn lease, no
Echo voice for 3 min, no video or camera stream on the Echo, Dell vision off, no Hermex desktop/TUI message for
10 min, no video/RTSP relay connection, no open splat job. Run it again right before restarting."""
import datetime, json, os, re, sqlite3, socket, subprocess, time, urllib.parse, urllib.request
from pathlib import Path

H = Path.home() / ".hermes"
now = time.time()
out, busy = [], []


def env_key(name, path=H / ".env"):
    m = re.search(rf"^{name}=(.*)$", path.read_text(), re.M)
    if not m:
        raise RuntimeError(f"{name} missing in {path}")
    return m.group(1).strip().strip("'\"")


def probe(label, fn):
    try:
        fn()
    except Exception as e:  # a failed probe is BUSY for this run only
        out.append(f"{label}: PROBE FAILED ({type(e).__name__}: {str(e)[:120]})")
        busy.append(f"{label} probe failed")


def hermes_live():
    req = urllib.request.Request(os.environ["HERMES_API_URL"].rstrip("/") + "/health/detailed",
                                 headers={"Authorization": "Bearer " + env_key("API_SERVER_KEY")})
    d = json.load(urllib.request.urlopen(req, timeout=6))
    runs = d["api_server"]["active_runs"]
    bq = d["readiness"]["checks"]["background_queues"]
    aa, gb = d.get("active_agents"), d.get("gateway_busy")
    out.append(f"hermes live: active_runs={runs} active_api_runs={bq.get('active_api_runs')} active_agents={aa} "
               f"gateway_busy={gb} process_completions={bq.get('process_completions')}")
    if runs or bq.get("active_api_runs") or aa or gb:
        busy.append("Hermes has live work")
    try:  # the snapshot file is shown for comparison only; it can lag or keep a leftover
        st = json.loads((H / "gateway_state.json").read_text())
        out.append(f"(gateway_state.json snapshot, informational: active_agents={st.get('active_agents')} "
                   f"active_work={st.get('active_work')})")
    except Exception:
        pass


def sessions():
    c = sqlite3.connect(f"file:{H}/state.db?mode=ro", uri=True, timeout=5)
    leases = [r for r in c.execute("select conversation_id, holder, expires_at from session_turn_leases") if r[2] > now]
    out.append(f"live turn leases: {len(leases)}")
    if leases:
        busy.append(f"{len(leases)} live turn lease(s)")
    last = {s: t for s, t in c.execute("select s.source, max(m.timestamp) from messages m join sessions s "
                                        "on s.id = m.session_id group by s.source") if t}
    for s, t in sorted(last.items()):
        out.append(f"last {s} message {datetime.datetime.fromtimestamp(t):%H:%M:%S} ({(now - t) / 60:.1f} min ago)")
    if now - last.get("api_server", 0) < 180:
        busy.append("Echo/API message under 3 min ago")
    desk = max([t for s, t in last.items() if s in ("gui", "desktop", "tui", "local", "cli")] or [0])
    if now - desk < 600:
        busy.append("Hermex desktop/TUI message under 10 min ago")
    # a session whose last row is user/tool with no live lease is a leftover (e.g. a killed probe), not live work
    for sid, src, role in c.execute("""select s.id, s.source, m.role from messages m join sessions s on s.id = m.session_id
            where m.id in (select max(id) from messages group by session_id) and m.timestamp > ?""", (now - 1800,)):
        if role in ("user", "tool"):
            out.append(f"(ignored: {src} session {sid[:20]} ends on a {role} row but holds no live lease)")


def echo_voice():
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "ConnectTimeout=8", "-i",
                        os.path.expanduser(os.environ["ECHO_SSH_KEY"]), "root@" + os.environ["ECHO_HOST"],
                        "cut -d' ' -f1 /proc/uptime; grep -E 'heard slot|replying slot|sent audio|reply played' "
                        "/data/techo5-linux/techo5.log | tail -n1"], capture_output=True, text=True, timeout=20)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip()[-120:] or f"ssh exit {r.returncode}")
    lines = r.stdout.strip().splitlines()
    up = float(lines[0])
    m = re.search(r"\[([0-9.]+)\]", lines[1]) if len(lines) > 1 else None
    age = up - float(m.group(1)) if m else None
    out.append(f"Echo last voice event: {'none in log' if age is None else f'{age:.0f} s ago'}")
    if age is not None and age < 180:
        busy.append("Echo voice under 3 min ago")


def echo_screen():
    code = ("import asyncio, pathlib\nfrom aioesphomeapi import APIClient\nasync def m():\n"
            " c = APIClient('" + os.environ["ECHO_HOST"] + "', 6053, None, noise_psk=pathlib.Path('" +
            os.path.expanduser(os.environ["ECHO_KEY_FILE"]) + "').read_text().strip())\n"
            " await c.connect(login=True); ents, _ = await c.list_entities_services(); keys = {e.key: e.object_id for e in ents}; st = {}\n"
            " c.subscribe_states(lambda s: st.__setitem__(keys.get(s.key, ''), s)); await asyncio.sleep(2); await c.disconnect()\n"
            " print(st['video_state'].state, st['camera_streaming'].state)\nasyncio.run(m())\n")
    r = subprocess.run([str(H / "tools/uv-0.13.5-linux-x64/uv"), "run", "-q", "--with", "aioesphomeapi", "python", "-c", code],
                       capture_output=True, text=True, timeout=90, cwd="/tmp")
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip()[-120:])
    video, cam = r.stdout.split()[-2:]
    out.append(f"Echo video_state={video} camera_streaming={cam}")
    if video != "idle":
        busy.append(f"Echo video {video}")
    if cam != "False":
        busy.append("Echo camera streaming")


def vision():
    req = urllib.request.Request("http://" + urllib.parse.urlsplit(os.environ["DELL_MEDIA_URL"]).hostname + ":8092/v1/vision/status",
                                 headers={"Authorization": "Bearer " + env_key("DELL_LLM_API_KEY")})
    st = json.load(urllib.request.urlopen(req, timeout=6)).get("state")
    out.append(f"Dell vision state={st}")
    if st != "idle":
        busy.append(f"vision {st}")


def relay():
    ss = subprocess.run(["ss", "-tnH", "state", "established", "( sport = :8644 or sport = :8554 )"],
                        capture_output=True, text=True, check=True).stdout.strip()
    ff = subprocess.run(["pgrep", "-f", "ffmpeg.*protocol_whitelist"], capture_output=True, text=True).stdout.split()
    n = len(ss.splitlines()) if ss else 0
    out.append(f"relay/RTSP connections={n} relay ffmpeg={ff or 'none'}")
    if n or ff:
        busy.append("video/RTSP relay in use")


def splat():
    with socket.socket(socket.AF_UNIX) as c:
        c.settimeout(10)
        c.connect(str(H / "splat-agent/worker.sock"))
        c.sendall(b'{"op": "status"}\n')
        buf = b""
        while b"\n" not in buf:
            b = c.recv(4096)
            if not b:
                break
            buf += b
    st = json.loads(buf)["result"]["open"]
    out.append(f"splat job open: {st['job_id'] if st else 'none'}")
    if st:
        busy.append(f"splat job {st['job_id']} open")


for label, fn in (("hermes", hermes_live), ("sessions", sessions), ("echo voice", echo_voice), ("echo screen", echo_screen),
                  ("vision", vision), ("relay", relay), ("splat", splat)):
    probe(label, fn)
print(f"{datetime.datetime.now():%H:%M:%S} London")
print("\n".join(out))
print("IDLE" if not busy else "BUSY: " + "; ".join(busy))
