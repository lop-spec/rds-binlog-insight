from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import app.oss_store as oss_store
from app.config import Settings
from app.oss_store import OssArchive, OssArchiveError


def _settings() -> Settings:
    return Settings(
        db_instance_id="rm-test000001",
        retention_days=60,
        oss_enabled=True,
        oss_bucket="example-binlog-bucket",
        oss_region_id="cn-hangzhou",
        oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
        oss_prefix="mysql-binlog/rm-test000001/",
        oss_auth_mode="ecs_ram_role",
        oss_retention_days=60,
    )


class MissingObject(Exception):
    status = 404


class RequestError(Exception):
    status = -2  # what oss2 reports for a TLS EOF


class AccessDenied(Exception):
    status = 403


class FakeBucket:
    def __init__(self, *, fail_part: dict[int, int] | None = None, fatal_part: int = 0):
        self.objects: dict[str, dict] = {}
        self.uploads: dict[str, dict] = {}
        self.fail_part = dict(fail_part or {})
        self.fatal_part = fatal_part
        self.put_calls = 0
        self.part_calls: list[int] = []
        self.aborted: list[str] = []
        self.completed: list[dict] = []

    def head_object(self, key):
        if key not in self.objects:
            raise MissingObject()
        value = self.objects[key]
        return SimpleNamespace(headers=value["headers"], content_length=len(value["body"]), etag="etag")

    def put_object_from_file(self, key, filename, headers=None):
        self.put_calls += 1
        self.objects[key] = {"body": Path(filename).read_bytes(), "headers": dict(headers or {})}

    def init_multipart_upload(self, key, headers=None):
        upload_id = f"upload-{len(self.uploads) + 1}"
        self.uploads[upload_id] = {"key": key, "headers": dict(headers or {}), "parts": {}}
        return SimpleNamespace(upload_id=upload_id)

    def upload_part(self, key, upload_id, number, data):
        self.part_calls.append(number)
        if number == self.fatal_part:
            raise AccessDenied("denied")  # not retryable
        if self.fail_part.get(number, 0) > 0:
            self.fail_part[number] -= 1
            data.read()
            raise RequestError("EOF occurred in violation of protocol")
        self.uploads[upload_id]["parts"][number] = data.read()
        return SimpleNamespace(etag=f"e{number}", crc=number)

    def complete_multipart_upload(self, key, upload_id, parts, headers=None):
        upload = self.uploads.pop(upload_id)
        self.completed.append({"parts": [(p.part_number, p.size, p.part_crc) for p in parts], "headers": dict(headers or {})})
        body = b"".join(upload["parts"][p.part_number] for p in parts)
        self.objects[key] = {"body": body, "headers": upload["headers"]}

    def abort_multipart_upload(self, key, upload_id):
        self.aborted.append(upload_id)
        self.uploads.pop(upload_id, None)


class OssMultipartTest(unittest.TestCase):
    def _upload(self, bucket: FakeBucket, body: bytes):
        archive = OssArchive(_settings(), bucket=bucket, upload_retry_delay_seconds=0)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.binlog"
            path.write_bytes(body)
            key = archive.prefix + "raw-binlog/v1/x/y.binlog"

            def verify():
                try:
                    head = bucket.head_object(key)
                except MissingObject:
                    return None
                return {"oss_key": key, "size_bytes": head.content_length}

            headers = {"x-oss-forbid-overwrite": "true", "x-oss-meta-sha256": "abc"}
            result = archive._put_object_from_file_verified(
                key, path, headers, verify=verify, missing_message="missing", missing_code="MISSING")
            return key, result

    def test_large_file_goes_up_in_parts_with_headers_on_initiate_and_complete(self):
        bucket = FakeBucket()
        body = bytes(range(256)) * 41  # 10,496 bytes -> parts of 4,096
        with patch.object(oss_store, "OSS_MULTIPART_THRESHOLD_BYTES", 1024), \
                patch.object(oss_store, "OSS_MULTIPART_PART_BYTES", 4096):
            key, result = self._upload(bucket, body)
        self.assertEqual(bucket.put_calls, 0)
        self.assertEqual(bucket.objects[key]["body"], body)
        self.assertEqual(bucket.objects[key]["headers"]["x-oss-meta-sha256"], "abc")
        self.assertEqual(bucket.completed[0]["headers"], {"x-oss-forbid-overwrite": "true"})
        self.assertEqual(bucket.completed[0]["parts"], [(1, 4096, 1), (2, 4096, 2), (3, 2304, 3)])
        self.assertEqual(result["size_bytes"], len(body))

    def test_transient_part_failure_retries_only_that_part(self):
        bucket = FakeBucket(fail_part={2: 2})
        body = b"x" * 10_000
        with patch.object(oss_store, "OSS_MULTIPART_THRESHOLD_BYTES", 1024), \
                patch.object(oss_store, "OSS_MULTIPART_PART_BYTES", 4096):
            key, _ = self._upload(bucket, body)
        self.assertEqual(bucket.part_calls, [1, 2, 2, 2, 3])
        self.assertEqual(bucket.objects[key]["body"], body)
        self.assertEqual(bucket.aborted, [])

    def test_permanent_part_failure_aborts_and_is_reported(self):
        bucket = FakeBucket(fatal_part=2)
        with patch.object(oss_store, "OSS_MULTIPART_THRESHOLD_BYTES", 1024), \
                patch.object(oss_store, "OSS_MULTIPART_PART_BYTES", 4096):
            with self.assertRaises(OssArchiveError) as caught:
                self._upload(bucket, b"y" * 10_000)
        self.assertEqual(caught.exception.code, "OSS_UPLOAD_FAILED")
        self.assertEqual(bucket.aborted, ["upload-1"])
        self.assertEqual(bucket.uploads, {})
        self.assertEqual(bucket.objects, {})

    def test_small_file_keeps_single_put(self):
        bucket = FakeBucket()
        key, _ = self._upload(bucket, b"small")
        self.assertEqual(bucket.put_calls, 1)
        self.assertEqual(bucket.part_calls, [])

    def test_threshold_is_below_the_put_object_cap(self):
        self.assertLess(oss_store.OSS_MULTIPART_THRESHOLD_BYTES, 5 * 1024**3)
        self.assertLessEqual(23 * 1024**3 // oss_store.OSS_MULTIPART_PART_BYTES + 1, 10_000)


if __name__ == "__main__":
    unittest.main()
