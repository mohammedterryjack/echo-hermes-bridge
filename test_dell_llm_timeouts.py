"""Dell LLM timeouts: a proxy hold must be a wait, not an error."""
from __future__ import annotations

import inspect
import os
import unittest
from pathlib import Path

# Match the other bridge tests: set dummy env before importing bridge.
os.environ.setdefault("ECHO_HOST", "192.0.2.248")
os.environ.setdefault("BRIDGE_BIND", "192.0.2.230")
os.environ.setdefault("HERMES_API_URL", "http://192.0.2.230:8642")
os.environ.setdefault("HERMES_PEER_DELL_URL", "http://192.0.2.148:8642")
os.environ.setdefault("HERMES_PEER_IMAC_URL", "http://imac.local:8642")
os.environ.setdefault("HERMES_PEER_MACMINI_URL", "http://192.0.2.215:8642")
os.environ.setdefault("ECHO_TTS", "192.0.2.215:10200")
os.environ.setdefault("ECHO_TTS_VOICE", "en_US-glados-medium")
os.environ.setdefault("ECHO_KEY_FILE", str(Path.home() / "Code/echo-hermes-bridge/echo-esphome.key"))
os.environ.setdefault("ECHO_SSH_KEY", str(Path.home() / ".ssh/techo5_echo"))
os.environ.setdefault("DELL_MEDIA_URL", "http://192.0.2.148:8091")
os.environ.setdefault("DELL_LLM_API_KEY", "test-key-not-used-on-the-network")

import bridge  # noqa: E402


def _dell_llm_timeout(path: Path) -> float:
    # Tiny parse of the dell-llm block so this test does not need PyYAML.
    text = path.read_text()
    start = text.find("  dell-llm:\n")
    if start < 0:
        raise AssertionError(f"no dell-llm provider in {path}")
    nxt = text.find("\n  ", start + 1)
    # next top-level-ish key after the block; take a window
    block = text[start:start + 800]
    for line in block.splitlines():
        if line.strip().startswith("request_timeout_seconds:"):
            return float(line.split(":", 1)[1].strip())
    raise AssertionError(f"no request_timeout_seconds in {path}")


class DellLlmTimeoutTests(unittest.TestCase):
    def test_bridge_llm_call_waits_at_least_180s(self):
        self.assertGreaterEqual(bridge.LLM_TIMEOUT_S, 180)
        self.assertIn(":8090", bridge.LLM)
        src = inspect.getsource(bridge._describe_jpeg)
        self.assertIn("timeout=LLM_TIMEOUT_S", src)
        self.assertIn("/v1/chat/completions", src)
        self.assertNotIn("timeout=15", src)

    def test_hermes_dell_llm_request_timeout_is_at_least_180s(self):
        home = Path.home() / ".hermes"
        for path in (
            home / "config.yaml",
            home / "profiles" / "splat" / "config.yaml",
            home / "profiles" / "vision" / "config.yaml",
        ):
            with self.subTest(path=str(path)):
                self.assertGreaterEqual(_dell_llm_timeout(path), 180)
                self.assertEqual(_dell_llm_timeout(path), 1800)


if __name__ == "__main__":
    unittest.main()
