# echo-hermes-bridge (Echo Show MCP server for Hermes)

An MCP server (`FastMCP("echo")`) that gives a [Hermes](https://github.com/NousResearch/hermes-agent) agent the
tools of an Amazon Echo Show running custom Linux firmware with an ESPHome API, on the home LAN only: timers and
alarms, volume, audio/radio/podcasts, pictures and video on the screen, the camera, intercom, and hand-offs to the
other local bots (3D, Vision, Synth) with long-job tracking and notify-on-done.

## Tools

Timers and alarms: `start_timer`, `pause_timer`, `resume_timer`, `add_time_to_timer`, `cancel_timer`,
`list_timers_and_alarms`, `set_alarm`, `set_reminder`, `snooze_alarm`, `delete_alarm`, `stop_ringing`.
Audio: `set_volume`, `play_audio_url`, `find_radio`, `play_radio`, `play_podcast`, `play_audiobook`.
Screen and video: `show_on_screen`, `show_message`, `show_picture`, `play_video`, `stop_video`, `play_youtube`,
`replay_last_video`, `replay_video`, `generate_picture`, `generate_video`.
Camera: `show_camera`, `look`, `glados_vision`, `intercom_call`.
Other agents and jobs: `ask_hermes_agent`, `create_splat`, `ask_vision`, `ask_synth`, `long_job_status`,
`show_job_result`, `check_my_log`.

## Files

- `bridge.py`: the MCP server (a `uv run --script` file; dependencies are pinned in its header).
- `long_jobs.py`: long-running job registry (`~/.hermes/long-jobs/`) with a CLI for producers to report back.
- `hermes_handoff.py`: relays a request to another local Hermes profile's run and back into a long job.
- `video_relay.py`: LAN-only HLS relay that plays a finished local MP4 on the Echo.
- `idlecheck.py`: read-only check that the machine is idle enough to restart the Hermes gateway.
- `test_*.py`: unit tests (`python -m unittest`); a few read files from a live Hermes install and fail elsewhere.

## Run

```yaml
# Hermes config.yaml
mcp_servers:
  echo:
    command: uv
    args: [run, -q, --script, /home/<you>/Code/echo-hermes-bridge/bridge.py]
    env: { ... }   # see .env.example
```

## Required environment

See `.env.example`. Everything is required and the bridge fails loudly if a variable is missing: `ECHO_HOST`,
`ECHO_KEY_FILE` (path to the ESPHome noise key), `ECHO_SSH_KEY` (path to the Echo SSH identity, used only as a
file path, never read or printed), `ECHO_TTS`, `ECHO_TTS_VOICE`, `BRIDGE_BIND`, `HERMES_API_URL`,
`DELL_MEDIA_URL`, `DELL_LLM_API_KEY`, and `HERMES_PEER_*_URL` / `HERMES_PEER_*_KEY` for `ask_hermes_agent`.
No keys, cookies or addresses from the author's network are in this repo.
