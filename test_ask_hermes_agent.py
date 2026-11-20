"""Unit tests for ask_hermes_agent. Mocks only: never contacts another Hermes agent."""
from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_tmp = tempfile.TemporaryDirectory(prefix="ask-hermes-test-")
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

SECRET = "sk-peer-SECRET-abc123-never-print"
PEER_ENV = ("HERMES_PEER_DELL_KEY", "HERMES_PEER_IMAC_KEY", "HERMES_PEER_MACMINI_KEY")


class Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.saved = {k: os.environ.pop(k, None) for k in PEER_ENV}
        self.logs = []
        self._log = patch.object(bridge, "_log", lambda *a: self.logs.append(" ".join(map(str, a))))
        self._log.start()

    def tearDown(self):
        self._log.stop()
        for k, v in self.saved.items():
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v

    def assertNoSecret(self, *texts):
        for t in list(texts) + self.logs:
            self.assertNotIn(SECRET, str(t))


class AllowlistTests(Base):
    def test_fixed_allowlist(self):
        self.assertEqual(set(bridge.HERMES_PEERS), {"dell", "imac", "macmini"})
        self.assertEqual(bridge.HERMES_PEERS["dell"]["url"], "http://192.0.2.148:8642")
        self.assertEqual(bridge.HERMES_PEERS["imac"]["url"], "http://imac.local:8642")
        self.assertEqual(bridge.HERMES_PEERS["macmini"]["url"], "http://192.0.2.215:8642")

    def test_spoken_names(self):
        for spoken, want in [("Dell", "dell"), ("the Dell", "dell"), ("iMac", "imac"), ("i mac", "imac"),
                             ("Minecraft bot", "imac"), ("the Minecraft agent", "imac"), ("Mac mini", "macmini"),
                             ("mac-mini", "macmini"), ("Mac Mini bot", "macmini")]:
            self.assertEqual(bridge._hermes_peer(spoken)[0], want, spoken)

    async def test_unknown_name_rejected_without_network(self):
        calls = []
        with patch.object(bridge, "_peer_http", lambda *a, **k: calls.append(a)):
            for bad in ("macbook", "http://evil:8642", "", "oldmac"):
                with self.assertRaisesRegex(RuntimeError, "I can only ask the Dell, the iMac"):
                    await bridge.ask_hermes_agent(bad)
        self.assertEqual(calls, [])


class KeyTests(Base):
    async def test_missing_key_fails_loudly(self):
        calls = []
        with patch.object(bridge, "_peer_http", lambda *a, **k: calls.append(a)):
            with self.assertRaisesRegex(RuntimeError, "I don't have a key for the iMac yet"):
                await bridge.ask_hermes_agent("imac")
            os.environ["HERMES_PEER_DELL_KEY"] = "${HERMES_PEER_DELL_KEY}"
            with self.assertRaisesRegex(RuntimeError, "I don't have a key for the Dell yet"):
                await bridge.ask_hermes_agent("dell", "how are you?")
        self.assertEqual(calls, [])

    async def test_key_only_in_bearer_header_never_in_output_or_logs(self):
        os.environ["HERMES_PEER_MACMINI_KEY"] = SECRET
        seen = []

        class Resp:
            def __init__(self, body):
                self.body = body
            def __enter__(self):
                return self
            def __exit__(self, *a):
                return False
            def read(self):
                return self.body

        def fake_open(req, timeout=None):
            seen.append((req.full_url, req.get_header("Authorization")))
            return Resp(b'{"status": "ok", "gateway_busy": false, "active_agents": 0, '
                        b'"last_heartbeat": "2026-11-14T16:50:00Z", "version": "0.23.1"}')

        with patch.object(bridge._peer_opener, "open", fake_open):
            out = await bridge.ask_hermes_agent("macmini")
        self.assertEqual(seen, [("http://192.0.2.215:8642/health/detailed", "Bearer " + SECRET)])
        self.assertNoSecret(out)

    async def test_errors_scrub_the_key(self):
        os.environ["HERMES_PEER_DELL_KEY"] = SECRET
        import urllib.error

        def fake_open(req, timeout=None):
            raise urllib.error.URLError("refused, header was Bearer %s" % SECRET)

        with patch.object(bridge._peer_opener, "open", fake_open):
            out = await bridge.ask_hermes_agent("dell")
        self.assertIn("down or not reachable", out)
        self.assertIn("<key>", out)
        self.assertNoSecret(out)

        def fake_open_q(req, timeout=None):
            raise urllib.error.URLError("boom %s" % SECRET)

        with patch.object(bridge._peer_opener, "open", fake_open_q):
            with self.assertRaises(RuntimeError) as cm:
                await bridge.ask_hermes_agent("dell", "status?")
        self.assertNoSecret(str(cm.exception))

    def test_redirects_are_refused(self):
        import urllib.error, urllib.request
        h = bridge._NoRedirect()
        req = urllib.request.Request("http://192.0.2.148:8642/health/detailed")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            h.redirect_request(req, None, 302, "Found", {}, "http://elsewhere/")
        cm.exception.close()


class StatusTests(Base):
    async def test_status_summary(self):
        os.environ["HERMES_PEER_DELL_KEY"] = SECRET
        calls = []

        def http(method, url, key, *a, **k):
            calls.append((method, url))
            return {"status": "ok", "gateway_busy": True, "active_agents": 2,
                    "last_heartbeat": "2026-11-14T16:50:12+00:00", "version": "0.23.1"}

        with patch.object(bridge, "_peer_http", http):
            out = await bridge.ask_hermes_agent("Dell")
        self.assertEqual(calls, [("GET", "http://192.0.2.148:8642/health/detailed")])
        self.assertEqual(out, "The Dell Hermes is up (version 0.23.1), busy, 2 active agents, "
                              "last heartbeat 16:50 London time.")

    def test_summary_idle_and_odd_values(self):
        out = bridge._peer_status_summary("the Mac mini", {"status": "degraded", "gateway_busy": False,
                                                           "active_agents": 1})
        self.assertIn("answering but its status is degraded", out)
        self.assertIn("idle", out)
        self.assertIn("1 active agent,", out)
        self.assertIn("no heartbeat reported", out)

    async def test_http_error_on_status_is_not_reported_as_down(self):
        os.environ["HERMES_PEER_DELL_KEY"] = SECRET

        def http(*a, **k):
            raise RuntimeError("HTTP 401 invalid key")

        with patch.object(bridge, "_peer_http", http):
            with self.assertRaisesRegex(RuntimeError, "the Dell status check failed: HTTP 401"):
                await bridge.ask_hermes_agent("dell")


class QuestionTests(Base):
    def setUp(self):
        super().setUp()
        os.environ["HERMES_PEER_IMAC_KEY"] = SECRET
        self.calls = []
        self.polls = []
        self._sleep = patch.object(bridge, "_peer_sleep", self.fake_sleep)
        self._sleep.start()
        self.clock = [1000.0]
        self._mono = patch.object(bridge, "_peer_clock", lambda: self.clock[0])
        self._mono.start()
        self.origin = "chat"
        self.origin_asked = []
        self._origin = patch.object(bridge, "_turn_origin", self.fake_origin)
        self._origin.start()

    def tearDown(self):
        self._sleep.stop()
        self._mono.stop()
        self._origin.stop()
        super().tearDown()

    def fake_origin(self, tool_name):
        self.origin_asked.append(tool_name)
        if self.origin is None:
            raise RuntimeError(bridge._ORIGIN_FAIL)
        return self.origin

    async def fake_sleep(self, s):
        self.clock[0] += s

    def http_factory(self, statuses, bot_chat_exists=True):
        statuses = list(statuses)

        def http(method, url, key, body=None, timeout=None, headers=None):
            self.assertEqual(key, SECRET)
            self.calls.append((method, url, body, headers))
            if url.startswith("http://imac.local:8642/api/sessions?"):
                return {"data": [{"id": "sess-1", "title": "Bot Chat"}] if bot_chat_exists else []}
            if url == "http://imac.local:8642/api/sessions":
                return {"object": "hermes.session", "session": {"id": "sess-new"}}
            if url == "http://imac.local:8642/v1/runs":
                return {"run_id": "run_42", "status": "queued", "replayed": False}
            if url == "http://imac.local:8642/v1/runs/run_42":
                self.polls.append(self.clock[0])
                return statuses.pop(0) if len(statuses) > 1 else statuses[0]
            raise AssertionError(url)
        return http

    async def test_run_polling_returns_answer(self):
        http = self.http_factory([{"status": "running"}, {"status": "running"},
                                  {"status": "completed", "output": "  All good, 3 tests left.  "}])
        with patch.object(bridge, "_peer_http", http):
            out = await bridge.ask_hermes_agent("Minecraft bot", "How is the bot fix going?")
        self.assertEqual(out, "The iMac says: All good, 3 tests left.")
        post = [c for c in self.calls if c[0] == "POST"][0]
        self.assertEqual(post[1], "http://imac.local:8642/v1/runs")
        self.assertEqual(post[2], {"input": "Message from GlaDOS (Mo's Echo voice assistant), asked on Mo's "
                                            "behalf: How is the bot fix going?", "session_id": "sess-1"})
        self.assertTrue(post[3]["Idempotency-Key"].startswith("glados-"))
        self.assertEqual(len(self.polls), 3)
        self.assertNoSecret(out)

    async def test_creates_bot_chat_when_missing(self):
        http = self.http_factory([{"status": "completed", "output": "hi"}], bot_chat_exists=False)
        with patch.object(bridge, "_peer_http", http):
            await bridge.ask_hermes_agent("imac", "hello?")
        create = [c for c in self.calls if c[1] == "http://imac.local:8642/api/sessions"][0]
        self.assertEqual(create[2], {"title": "Bot Chat", "source": "bot_peer_dm"})
        run = [c for c in self.calls if c[1].endswith("/v1/runs")][0]
        self.assertEqual(run[2]["session_id"], "sess-new")

    async def test_timeout_says_so_and_does_not_resend(self):
        http = self.http_factory([{"status": "running"}])
        with patch.object(bridge, "_peer_http", http):
            out = await bridge.ask_hermes_agent("imac", "long question")
        self.assertIn("hasn't answered within 300 seconds", out)
        self.assertIn("won't send it again", out)
        self.assertEqual(len([c for c in self.calls if c[1].endswith("/v1/runs")]), 1)
        self.assertGreaterEqual(self.clock[0] - 1000.0, 300)
        self.assertLessEqual(self.clock[0] - 1000.0, 300 + bridge.PEER_POLL_S)
        self.assertTrue(any("still running" in x for x in self.logs))
        self.assertEqual(self.origin_asked, ["ask_hermes_agent"])

    async def test_chat_waits_past_90s_for_a_slow_answer(self):
        http = self.http_factory([{"status": "running"}] * 120 + [{"status": "completed", "output": "Fixing ViaProxy."}])
        with patch.object(bridge, "_peer_http", http):
            out = await bridge.ask_hermes_agent("imac", "what are you doing?")
        self.assertEqual(out, "The iMac says: Fixing ViaProxy.")
        self.assertGreater(self.clock[0] - 1000.0, 200)

    async def test_echo_voice_budget_stays_under_the_echo_60s_cutoff(self):
        self.origin = "echo"
        http = self.http_factory([{"status": "running"}])
        with patch.object(bridge, "_peer_http", http):
            out = await bridge.ask_hermes_agent("imac", "long question")
        self.assertIn("hasn't answered within 50 seconds", out)
        self.assertGreaterEqual(self.clock[0] - 1000.0, 50)
        self.assertLess(self.clock[0] - 1000.0, 60)
        self.assertTrue(any("budget 50s" in x for x in self.logs))

    async def test_unknown_origin_fails_closed_before_sending(self):
        self.origin = None
        http = self.http_factory([{"status": "completed", "output": "x"}])
        with patch.object(bridge, "_peer_http", http):
            with self.assertRaisesRegex(RuntimeError, "cannot tell whether this request came from"):
                await bridge.ask_hermes_agent("imac", "q")
        self.assertEqual(self.calls, [])

    async def test_status_check_needs_no_origin(self):
        self.origin = None
        with patch.object(bridge, "_peer_http", lambda *a, **k: {"status": "ok", "gateway_busy": False}):
            out = await bridge.ask_hermes_agent("imac")
        self.assertIn("Hermes is up", out)
        self.assertEqual(self.origin_asked, [])

    async def test_failed_run_is_an_error(self):
        http = self.http_factory([{"status": "failed", "error": "model overloaded"}])
        with patch.object(bridge, "_peer_http", http):
            with self.assertRaisesRegex(RuntimeError, r"the iMac could not answer \(failed\): model overloaded"):
                await bridge.ask_hermes_agent("imac", "q")

    async def test_completed_without_output(self):
        http = self.http_factory([{"status": "completed", "output": ""}])
        with patch.object(bridge, "_peer_http", http):
            out = await bridge.ask_hermes_agent("imac", "q")
        self.assertEqual(out, "The iMac finished but gave no answer.")


class BudgetTableTests(unittest.TestCase):
    def test_one_explicit_budget_per_origin(self):
        self.assertEqual(bridge.PEER_QUESTION_BUDGET_S, {"echo": 50, "chat": 300})
        self.assertLess(bridge.PEER_QUESTION_BUDGET_S["echo"], 60)

    def test_sources_map_to_origins(self):
        self.assertEqual(bridge._classify_session("20261115_094539_4b7ffd", "gui"), "chat")
        self.assertEqual(bridge._classify_session("x", "tui"), "chat")
        self.assertEqual(bridge._classify_session("x", "cli"), "chat")
        self.assertEqual(bridge._classify_session("api-abc", "api_server"), "echo")
        self.assertIsNone(bridge._classify_session("x", "bot_peer_dm"))

    def test_docstring_states_both_budgets(self):
        doc = bridge.ask_hermes_agent.__doc__ if hasattr(bridge.ask_hermes_agent, "__doc__") else ""
        src = Path(bridge.__file__).read_text()
        self.assertIn("up to 300 seconds (5 minutes) when asked from the Hermex desktop", src)
        self.assertIn("up to 50 seconds when asked by voice on the Echo", src)
        self.assertNotIn("waits up to about 90 seconds", src)


class DocsTests(unittest.TestCase):
    def test_docstring_and_soul(self):
        doc = Path(bridge.__file__).read_text()
        self.assertIn("async def ask_hermes_agent(", doc)
        self.assertIn("Questions should be read-only", doc)
        soul = Path.home().joinpath(".hermes/SOUL.md").read_text()
        self.assertIn("call ask_hermes_agent with its name and no question", soul)
        self.assertIn("call ask_hermes_agent with its name and the question in plain words", soul)
        self.assertIn("Keep questions read-only", soul)


if __name__ == "__main__":
    unittest.main(verbosity=2)
