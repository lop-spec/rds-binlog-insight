"""Check OSS's unchanged CRC64 algorithm and report its implementation."""
from __future__ import annotations

import importlib
import json
import logging
import time
from functools import lru_cache

import oss2

LOGGER = logging.getLogger(__name__)


def crc_backend() -> str:
    implementation = importlib.import_module("crcmod.crcmod")
    active = getattr(implementation, "_usingExtension", None)
    return "native" if active is True else "python" if active is False else "unknown"


@lru_cache(maxsize=1)
def report_crc_backend() -> str:
    backend = crc_backend()
    if backend == "native":
        LOGGER.info("OSS_CRC64_BACKEND backend=native checksum=enabled")
    else:
        LOGGER.warning(
            "OSS_CRC64_BACKEND backend=%s checksum=enabled "
            "reason=crcmod_C_extension_not_active_or_unrecognized", backend,
        )
    return backend


def verify_crc_runtime(*, require_native: bool = False) -> dict:
    backend = report_crc_backend()
    if require_native:
        if backend != "native":
            raise RuntimeError("OSS CRC64 native extension required; refusing slow fallback")
        importlib.import_module("crcmod._crcfunext")
    vectors = [(b"", 0), (b"123456789", 0x995DC9BBDF1939FA),
               (b"x" * (1024 * 1024), 8714314512667677816)]
    started = time.perf_counter()
    for data, expected in vectors:
        single = oss2.utils.Crc64()
        single(data)
        streamed = oss2.utils.Crc64()
        for offset in range(0, len(data), 4096):
            streamed(data[offset:offset + 4096])
        if single.crc != expected or streamed.crc != expected:
            raise RuntimeError("OSS CRC64 whole/streamed checksum vector mismatch")
    return {"backend": backend, "checksumEnabled": True, "vectors": len(vectors),
            "verificationSeconds": time.perf_counter() - started}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--require-native", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(verify_crc_runtime(require_native=args.require_native)), flush=True)
