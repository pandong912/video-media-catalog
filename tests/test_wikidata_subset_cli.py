from __future__ import annotations

import hashlib

from video_media_catalog.object_store import S3Location
from video_media_catalog.wikidata_subset_cli import _publish_s3_part


class _MissingObject(Exception):
    def __init__(self) -> None:
        self.response = {
            "Error": {"Code": "NoSuchKey"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }


class _NonSeekableBody:
    def __init__(self, value: bytes) -> None:
        self._value = value
        self._offset = 0
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._value) - self._offset
        result = self._value[self._offset : self._offset + size]
        self._offset += len(result)
        return result

    def close(self) -> None:
        self.closed = True


class _S3:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.published = False
        self.put_request = None

    def get_object(self, **_request):
        return {
            "Body": _NonSeekableBody(self.payload),
            "ContentLength": len(self.payload),
        }

    def head_object(self, **request):
        if not self.published or "VersionId" not in request:
            raise _MissingObject
        return {
            "ContentLength": len(self.payload),
            "Metadata": {"sha256": hashlib.sha256(self.payload).hexdigest()},
            "ETag": '"published-etag"',
            "VersionId": "published-version",
        }

    def put_object(self, **request):
        body = request["Body"]
        assert hasattr(body, "seek")
        assert body.seekable()
        assert body.read() == self.payload
        self.put_request = request
        self.published = True
        return {"VersionId": "published-version"}


def test_publish_spools_non_seekable_s3_body_before_conditional_put() -> None:
    payload = b"BZh" + (b"catalog" * 1024)
    s3 = _S3(payload)

    result = _publish_s3_part(
        s3=s3,
        source=S3Location("catalog", "temporary/output/part-00000.bz2"),
        source_version="temporary-version",
        destination=S3Location("catalog", "raw/subset.json.bz2"),
        config_digest="sha256:" + ("1" * 64),
        dump_sha256="2" * 64,
        max_bytes=len(payload),
    )

    assert result.uri == "s3://catalog/raw/subset.json.bz2"
    assert result.object_version == "published-version"
    assert s3.put_request["IfNoneMatch"] == "*"
    assert s3.put_request["ContentLength"] == len(payload)
