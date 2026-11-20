"""Offline tests for bridge._normalize_image_bytes / _image_base64 (every image sent to the Dell)."""
from __future__ import annotations

import base64
import io
import os
import tempfile
import unittest
from pathlib import Path

from PIL import Image

_tmp = tempfile.TemporaryDirectory(prefix="img-norm-test-")
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


def _open(b64: str) -> Image.Image:
    im = Image.open(io.BytesIO(base64.b64decode(b64)))
    im.load()
    return im


class NormaliseTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(dir=_tmp.name))

    def test_mpo_becomes_single_frame_jpeg(self):
        p = self.dir / "phone.jpg"
        Image.new("RGB", (64, 48), (255, 0, 0)).save(
            p, format="MPO", save_all=True, append_images=[Image.new("RGB", (64, 48), (0, 0, 255))])
        self.assertEqual(Image.open(p).format, "MPO")
        im = _open(bridge._image_base64(p))
        self.assertEqual(im.format, "JPEG")
        self.assertEqual(getattr(im, "n_frames", 1), 1)
        self.assertEqual(im.size, (64, 48))
        self.assertGreater(im.getpixel((5, 5))[0], 200)  # first (red) frame

    def test_exif_orientation_applied_and_dropped(self):
        p = self.dir / "rot.jpg"
        ex = Image.Exif()
        ex[0x0112] = 6
        Image.new("RGB", (80, 40), (0, 255, 0)).save(p, format="JPEG", exif=ex.tobytes())
        im = _open(bridge._image_base64(p))
        self.assertEqual(im.format, "JPEG")
        self.assertEqual(im.size, (40, 80))
        self.assertIsNone(im.getexif().get(0x0112))

    def test_alpha_png_stays_png_with_alpha(self):
        p = self.dir / "a.png"
        Image.new("RGBA", (32, 32), (255, 0, 255, 0)).save(p)
        im = _open(bridge._image_base64(p))
        self.assertEqual(im.format, "PNG")
        self.assertEqual(im.mode, "RGBA")
        self.assertEqual(im.getpixel((0, 0))[3], 0)

    def test_opaque_png_becomes_jpeg(self):
        p = self.dir / "o.png"
        Image.new("RGB", (32, 32), (10, 20, 30)).save(p)
        self.assertEqual(_open(bridge._image_base64(p)).format, "JPEG")

    def test_unreadable_formats_fail_loudly(self):
        p = self.dir / "junk.jpg"
        p.write_bytes(b"\xff\xd8 not really a jpeg " * 4)
        with self.assertRaisesRegex(RuntimeError, "could not be decoded|cannot open|could not identify"):
            bridge._image_base64(p)
        q = self.dir / "text.png"
        q.write_bytes(b"plain text, not an image at all")
        with self.assertRaisesRegex(RuntimeError, "not an image format the bridge can open"):
            bridge._image_base64(q)

    def test_camera_bytes_normalised_for_llm(self):
        buf = io.BytesIO()
        Image.new("RGB", (8, 8), (1, 2, 3)).save(buf, format="JPEG")
        data, mime = bridge._normalize_image_bytes(buf.getvalue(), "the camera photo")
        self.assertEqual(mime, "image/jpeg")
        self.assertEqual(Image.open(io.BytesIO(data)).format, "JPEG")


if __name__ == "__main__":
    unittest.main()
