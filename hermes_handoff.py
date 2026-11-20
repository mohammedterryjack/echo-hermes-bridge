#!/usr/bin/env python3
"""Hand a request to another Hermes bot on this machine and relay its reply into a long job, unchanged.

GlaDOS's create_splat uses it for the 3D bot (Hermes profile "splat"): the bridge records the long job, starts the
bot's run with Mo's request (start_run), and launches this file as its own systemd user unit (follow), so the
relay outlives the bridge process and gateway restarts. When the run ends:
  * a completed run with a reply -> long_jobs.finish(summary = the reply without its MEDIA lines, media = those
    files if they exist, echo_video = the .mp4 among them). A reply with no files is still finished, as written;
  * a failed/cancelled/interrupted run that still has a reply -> long_jobs.fail(error = that reply, media = its
    files if they exist), so the notice can relay it verbatim with any Viewer: and MEDIA: lines;
  * no reply -> long_jobs.fail(error = the run's status and error).
Mo cancelling the long job (long_jobs.py fail <id> --error ...) stops the bot's run. The key is read from the bot
profile's .env on this machine, never put on a command line, and never printed.
Usage: hermes_handoff.py follow <long_job_id> <run_id> --runs-url URL --key-file PATH
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import long_jobs  # noqa: E402

TERMINAL = ("completed", "failed", "cancelled", "interrupted")
POLL_S = float(os.environ.get("HERMES_HANDOFF_POLL_S", "10"))
MAX_LOST = 6  # consecutive unreachable polls (one minute at the default poll) before the job is failed


def read_key(key_file) -> str:
    m = re.search(r"^API_SERVER_KEY=(.*)$", pathlib.Path(key_file).read_text(), re.M)
    k = m.group(1).strip().strip("'\"") if m else ""
    if not k:
        raise RuntimeError(f"API_SERVER_KEY is not set in {key_file}")
    return k


def http(method, url, key, body=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        text = e.read().decode(errors="replace").replace(key, "<key>")[:200]
        raise RuntimeError(f"HTTP {e.code} from {url}: {text}") from None
    except (urllib.error.URLError, OSError) as e:
        raise RuntimeError(f"{url} is not reachable: {e}") from None


def start_run(runs_url, key_file, text) -> str:
    """Start the bot's run with this input; returns its run id. Fails loudly."""
    r = http("POST", runs_url, read_key(key_file), {"input": text})
    if not r.get("run_id"):
        raise RuntimeError(f"the bot's gateway gave no run id: {json.dumps(r)[:200]}")
    return str(r["run_id"])


def split_reply(text):
    """(reply without MEDIA: lines, [absolute paths from MEDIA: lines])"""
    media, keep = [], []
    for line in (text or "").splitlines():
        s = line.strip()
        if s.startswith("MEDIA:"):
            media.append(s[len("MEDIA:"):].strip())
        else:
            keep.append(line)
    return "\n".join(keep).strip(), media


def settle(job_id, status, output, error=""):
    """Record the run's end: the bot's reply as written (and its files), or failed if there is no reply."""
    summary, media = split_reply(output)
    if media:
        missing = [m for m in media if not (os.path.isabs(m) and os.path.isfile(m))]
        if missing:
            return long_jobs.fail(job_id, f"the bot's reply names files that do not exist: {', '.join(missing)}. "
                                          f"Its reply: {summary}"[:1500])
    if status == "completed" and (summary or media):
        mp4 = next((m for m in media if m.lower().endswith(".mp4")), "")
        return long_jobs.finish(job_id, summary, media, mp4)
    if summary or media:
        err = summary or (
            f"the bot's run ended ({status})" + (f": {error}" if error else "")
        )
        return long_jobs.fail(job_id, err[:1500], media)
    return long_jobs.fail(job_id, f"the bot's run ended ({status}) without a reply" + (f": {error}" if error else ""))


def follow(job_id, run_id, runs_url, key_file, log=print):
    key = read_key(key_file)
    url = f"{runs_url}/{run_id}"
    lost = 0
    while True:
        rec = long_jobs.load(job_id)
        if rec["status"] != "running":  # cancelled (failed from outside): stop the bot's run
            try:
                log("long job", job_id, "is", rec["status"], "- stopping run", run_id, http("POST", url + "/stop", key, {}))
            except RuntimeError as e:
                log("STOP FAILED", run_id, e)
            return rec
        try:
            st = http("GET", url, key)
            lost = 0
        except RuntimeError as e:
            lost += 1
            log("poll failed", lost, e)
            if lost >= MAX_LOST:
                return long_jobs.fail(job_id, f"lost track of the bot's run {run_id}: {e}")
            time.sleep(POLL_S)
            continue
        if st.get("status") in TERMINAL:
            log("run", run_id, st["status"])
            return settle(job_id, st["status"], st.get("output") or "", st.get("error") or "")
        time.sleep(POLL_S)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("follow")
    f.add_argument("job_id")
    f.add_argument("run_id")
    f.add_argument("--runs-url", required=True)
    f.add_argument("--key-file", required=True)
    a = ap.parse_args(argv)
    log = lambda *x: print(time.strftime("%H:%M:%S"), *x, flush=True)
    try:
        rec = follow(long_jobs.check_job_id(a.job_id), a.run_id, a.runs_url, a.key_file, log)
    except Exception as e:  # the relay itself broke: say so in the job, loudly
        log("ERROR", e)
        try:
            long_jobs.fail(a.job_id, f"the relay of the bot's reply broke: {e}")
        except Exception as e2:
            log("could not fail the job:", e2)
        return 1
    log(long_jobs.describe(rec))
    return 0


if __name__ == "__main__":
    sys.exit(main())
