"""Restore from locally staged v2 objects; no OSS credentials or live database required.

python -m app.restore_frames MANIFEST ZSTD LITE_INDEX NEW_DIRECTORY
"""
import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path

from oss2.utils import Crc64
from .raw_frames import validate, fail


def verified_file(path, expected):
    h, crc, size = hashlib.sha256(), Crc64(), 0
    with path.open('rb') as stream:
        while data := stream.read(1 << 20):
            h.update(data); crc(data); size += len(data)
    if (size,h.hexdigest(),str(crc.crc)) != (expected['size_bytes'],expected['sha256'],expected['crc64']):
        fail('staged object differs from manifest')


def restore(manifest, packed, index, dest):
    import pyarrow as pa
    import shutil
    # Obtain the immutable manifest object by its full content-addressed name.
    data = manifest.read_bytes()
    if not manifest.name.endswith('.'+hashlib.sha256(data).hexdigest()+'.manifest.json'):
        fail('manifest digest mismatch')
    m=json.loads(data); raw=m['raw']; validate(raw)
    verified_file(packed,raw['stored']); verified_file(index,m['index'])
    if dest.exists(): raise FileExistsError(dest)
    codec = pa.Codec('zstd')
    with tempfile.TemporaryDirectory(dir=dest.parent) as scratch:
        root=Path(scratch)/'verified'; root.mkdir()
        plain=root/'source.binlog'
        with packed.open('rb') as src, plain.open('xb') as out:
            for i,(_,length,sha) in enumerate(raw['frames']):
                n=min(raw['block'],raw['size_bytes']-i*raw['block'])
                decoded=codec.decompress(src.read(length),decompressed_size=n,asbytes=True)
                if hashlib.sha256(decoded).hexdigest()!=sha: fail('invalid decoded frame')
                out.write(decoded)
        verified_file(plain,raw)
        shutil.copyfile(index,root/'source.lite.json.gz'); verified_file(root/'source.lite.json.gz',m['index'])
        (root/'manifest.json').write_bytes(data)
        # Create the destination exclusively before publishing. Consumers require READY.
        dest.mkdir()
        for f in root.iterdir(): os.replace(f,dest/f.name)
        (dest/'READY').touch(exist_ok=False)
    return raw['size_bytes']


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('manifest','packed','index','dest'): p.add_argument(name,type=Path)
    a=p.parse_args(); print('verified plaintext bytes:',restore(a.manifest,a.packed,a.index,a.dest))
