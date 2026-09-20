from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse, urlunparse

from video_media_catalog.source_silver import _spark_input_uri


def test_spark_input_uri_decodes_percent_encoded_file_paths(tmp_path: Path) -> None:
    path = tmp_path / "sha256=abc" / "sha256:deadbeef.ndjson"
    path.parent.mkdir(parents=True)
    path.write_text('{"value":"ok"}\n', encoding="utf-8")
    parsed = urlparse(path.as_uri())
    encoded_path = parsed.path.replace("=", "%3D").replace(":", "%3A")
    encoded = urlunparse((parsed.scheme, parsed.netloc, encoded_path, "", "", ""))
    assert _spark_input_uri(encoded) == str(path)
