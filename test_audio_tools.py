"""Unit tests for the podcast/audiobook tools. Mocks only:
never contacts Apple, archive.org, or the Echo."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

_tmp = tempfile.TemporaryDirectory(prefix="audio-test-")
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

SOURCE_1080 = {"format_id": "1080p__source_", "url": "https://hls.example/1080.m3u8", "protocol": "m3u8_native",
               "vcodec": "avc1.640028", "acodec": "mp4a.40.2", "height": 1080, "fps": 20.0, "tbr": 3766}
AUDIO_ONLY = {"format_id": "audio_only", "url": "https://hls.example/a.m3u8", "protocol": "m3u8_native",
              "vcodec": "none", "acodec": "mp4a.40.2", "height": None, "tbr": 160}
H264_720 = {"format_id": "720p30", "url": "https://hls.example/720.m3u8", "protocol": "m3u8_native",
            "vcodec": "avc1.4D401F", "acodec": "mp4a.40.2", "height": 720, "fps": 30.0, "tbr": 2500}
H264_480 = {"format_id": "480p30", "url": "https://hls.example/480.m3u8", "protocol": "m3u8_native",
            "vcodec": "avc1.4D401E", "acodec": "mp4a.40.2", "height": 480, "fps": 30.0, "tbr": 1400}
HEVC_720 = {"format_id": "720p60hevc", "url": "https://hls.example/hevc.m3u8", "protocol": "m3u8_native",
            "vcodec": "hvc1.1.2.L120", "acodec": "mp4a.40.2", "height": 720, "fps": 60.0, "tbr": 2000}


def _fake_ytdlp(info=None, exc=None, seen=None):
    class YDL:
        def __init__(self, opts):
            if seen is not None:
                seen.append(opts)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def extract_info(self, target, download=False):
            if exc:
                raise exc
            return info
    return types.SimpleNamespace(YoutubeDL=YDL)


FEED = b"""<?xml version="1.0"?>
<rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" version="2.0"><channel>
<title>Show</title><itunes:author>Host</itunes:author>
<item><title>3. Newest One</title><enclosure url="https://cdn/3.mp3" type="audio/mpeg"/><itunes:duration>01:02:03</itunes:duration></item>
<item><title>2. Nelson at Trafalgar</title><enclosure url="https://cdn/2.m4a" type="audio/x-m4a"/><itunes:duration>600</itunes:duration></item>
<item><title>1. First</title><enclosure url="https://cdn/1.mp3" type="audio/mpeg"/></item>
<item><title>no audio</title></item>
</channel></rss>"""
ID3 = b"ID3\x04\x00\x00\x00\x00\x00\x00rest"
ADTS = b"\xff\xf1\x50\x80\x00\x1f\xfc"


class AudioHelperTests(unittest.TestCase):
    def test_seconds(self):
        self.assertEqual(bridge._audio_seconds("01:02:03"), 3723)
        self.assertEqual(bridge._audio_seconds("65:06"), 3906)
        self.assertEqual(bridge._audio_seconds("3906.4"), 3906)
        self.assertIsNone(bridge._audio_seconds(""))
        self.assertIsNone(bridge._audio_seconds("soon"))

    def test_mp3_sniff(self):
        self.assertTrue(bridge._is_mp3_bytes(ID3))
        self.assertTrue(bridge._is_mp3_bytes(b"\xff\xfb\x90\x64"))
        self.assertFalse(bridge._is_mp3_bytes(ADTS))
        self.assertFalse(bridge._is_mp3_bytes(b"<html>"))

    def test_feed_parse_and_pick(self):
        show, author, eps = bridge._parse_podcast_feed(FEED)
        self.assertEqual((show, author, len(eps)), ("Show", "Host", 3))
        self.assertEqual(bridge._pick_episode(eps, "")["url"], "https://cdn/3.mp3")
        self.assertEqual(bridge._pick_episode(eps, "latest")["url"], "https://cdn/3.mp3")
        self.assertEqual(bridge._pick_episode(eps, "first")["url"], "https://cdn/1.mp3")
        with self.assertRaisesRegex(RuntimeError, "no episode matching"):
            bridge._pick_episode(eps, "waterloo")

    def test_bad_feed(self):
        with self.assertRaisesRegex(RuntimeError, "not valid RSS"):
            bridge._parse_podcast_feed(b"<rss><channel>")

    def test_archive_query_is_librivox_only(self):
        q = bridge._archive_query(["sherlock", "holmes"])
        self.assertTrue(q.startswith("collection:(librivoxaudio) AND "))
        self.assertIn("title:(sherlock AND holmes)", q)


class PodcastResolveTests(unittest.TestCase):
    def _get(self, routes):
        def fake(url, max_bytes=None, headers=None):
            for prefix, body in routes:
                if url.startswith(prefix):
                    if isinstance(body, Exception):
                        raise body
                    return body
            raise AssertionError("unexpected URL " + url)
        return patch.object(bridge, "_audio_http_get", fake)

    def test_newest_mp3(self):
        itunes = json.dumps({"results": [{"collectionName": "Show", "artistName": "Host",
                                          "feedUrl": "https://feed/rss"}]}).encode()
        with self._get([("https://itunes.apple.com/search?", itunes), ("https://feed/rss", FEED),
                        ("https://cdn/3.mp3", ID3)]):
            got = bridge._podcast_resolve("show")
        self.assertEqual(got["url"], "https://cdn/3.mp3")
        self.assertEqual(got["duration"], 3723)
        self.assertEqual(got["show"], "Show")

    def test_feed_url_query_skips_search(self):
        with self._get([("https://feed/rss", FEED), ("https://cdn/1.mp3", ID3)]):
            got = bridge._podcast_resolve("https://feed/rss", "first")
        self.assertEqual(got["url"], "https://cdn/1.mp3")

    def test_m4a_episode_refused(self):
        with self._get([("https://feed/rss", FEED)]):
            with self.assertRaisesRegex(RuntimeError, "not MP3"):
                bridge._podcast_resolve("https://feed/rss", "nelson")

    def test_mp3_typed_but_aac_bytes_refused(self):
        with self._get([("https://feed/rss", FEED), ("https://cdn/3.mp3", ADTS)]):
            with self.assertRaisesRegex(RuntimeError, "not MP3"):
                bridge._podcast_resolve("https://feed/rss")

    def test_no_podcast(self):
        with self._get([("https://itunes.apple.com/search?", b'{"results": []}')]):
            with self.assertRaisesRegex(RuntimeError, "no podcast found"):
                bridge._podcast_resolve("nothing")


class AudiobookResolveTests(unittest.TestCase):
    META = {"files": [
        {"name": "book_02_x_64kb.mp3", "format": "64Kbps MP3", "title": "Two", "length": "10:00"},
        {"name": "book_01_x_64kb.mp3", "format": "64Kbps MP3", "title": "One", "length": "3906.1"},
        {"name": "book_01_x.ogg", "format": "Ogg Vorbis"},
    ]}
    SEARCH = {"response": {"docs": [{"identifier": "book_librivox", "title": "Book", "creator": "Author"}]}}

    def _patch(self, meta=None, search=None, head=ID3):
        def fake_json(url):
            if url.startswith(bridge.ARCHIVE_SEARCH_URL):
                return search if search is not None else self.SEARCH
            if url.startswith(bridge.ARCHIVE_METADATA_URL):
                return meta if meta is not None else self.META
            raise AssertionError(url)
        heads = []

        def fake_get(url, max_bytes=None, headers=None):
            heads.append(url)
            return head
        return patch.object(bridge, "_audio_http_json", fake_json), patch.object(bridge, "_audio_http_get", fake_get), heads

    def test_chapter_one(self):
        pj, pg, heads = self._patch()
        with pj, pg:
            got = bridge._audiobook_resolve("book", 1)
        self.assertEqual(got["url"], "https://archive.org/download/book_librivox/book_01_x_64kb.mp3")
        self.assertEqual((got["chapter"], got["chapters"], got["chapter_title"], got["duration"]), (1, 2, "One", 3906))
        self.assertEqual(heads, [got["url"]])

    def test_chapter_out_of_range(self):
        pj, pg, _ = self._patch()
        with pj, pg, self.assertRaisesRegex(RuntimeError, "has 2 chapters"):
            bridge._audiobook_resolve("book", 3)

    def test_bad_chapter(self):
        for bad in (0, -1, True, "2"):
            with self.assertRaises(ValueError):
                bridge._audiobook_resolve("book", bad)

    def test_not_found(self):
        pj, pg, _ = self._patch(search={"response": {"docs": []}})
        with pj, pg, self.assertRaisesRegex(RuntimeError, "no LibriVox audiobook"):
            bridge._audiobook_resolve("zzz", 1)

    def test_no_mp3_files(self):
        pj, pg, _ = self._patch(meta={"files": [{"name": "a.ogg", "format": "Ogg Vorbis"}]})
        with pj, pg, self.assertRaisesRegex(RuntimeError, "no 64Kbps MP3"):
            bridge._audiobook_resolve("book", 1)


class AudioToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_play_podcast_plays_url_on_speaker(self):
        info = {"show": "Show", "author": "Host", "episode": "3. Newest", "url": "https://cdn/3.mp3",
                "duration": 3723, "published": ""}
        played = []

        async def fake_play(url):
            played.append(url)
            return "playing on the Echo speaker (speaker=PLAYING)"
        with patch.object(bridge, "_podcast_resolve", return_value=info), \
             patch.object(bridge, "_play_audio_confirmed", fake_play), patch.object(bridge, "_log"):
            out = await bridge.play_podcast("show")
        self.assertEqual(played, ["https://cdn/3.mp3"])
        self.assertIn("speaker=PLAYING", out)
        self.assertIn("1 hour 2 minutes 3 seconds", out)

    async def test_play_audiobook_reports_chapter(self):
        info = {"title": "Book", "author": "Author", "chapter": 1, "chapters": 12, "chapter_title": "One",
                "url": "https://archive.org/download/b/1.mp3", "duration": 60}

        async def fake_play(url):
            return "playing on the Echo speaker (speaker=PLAYING)"
        with patch.object(bridge, "_audiobook_resolve", return_value=info), \
             patch.object(bridge, "_play_audio_confirmed", fake_play), patch.object(bridge, "_log"):
            out = await bridge.play_audiobook("book")
        self.assertIn("chapter 1 of 12: One", out)

    async def test_resolve_error_never_touches_speaker(self):
        async def fake_play(url):
            raise AssertionError("must not play")
        with patch.object(bridge, "_podcast_resolve", side_effect=RuntimeError("that episode is audio/x-m4a, not MP3")), \
             patch.object(bridge, "_play_audio_confirmed", fake_play):
            with self.assertRaisesRegex(RuntimeError, "not MP3"):
                await bridge.play_podcast("show")


if __name__ == "__main__":
    unittest.main(verbosity=2)
