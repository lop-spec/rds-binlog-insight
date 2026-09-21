"""Single-file OSS roundtrip probe; no serving metadata or source-data writes.

Writes immutable original+sidecar objects at their final content-addressed keys.
The serving collector can reuse them; the original local file is not removed.
"""
from __future__ import annotations
import dataclasses
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
import app
if os.environ.get('RAW_PROBE_MODULES'):
    app.__path__.insert(0,os.environ['RAW_PROBE_MODULES'])
from app.config import Settings
from app.metadata import MetadataStore
from app.oss_store import OssArchive
from app.parser_bridge import checksum_file
from app.raw_binlog import RawBinlogStore
from app.raw_binlog_query import Budget, read_object
from app.rds_api import RemoteBinlog


def run(path: Path, metadata_path: Path):
    source=sqlite3.connect(f'file:{metadata_path}?mode=ro',uri=True)
    source.row_factory=sqlite3.Row
    record=dict(source.execute('SELECT * FROM binlog_files WHERE id=?',(path.stem,)).fetchone())
    settings=Settings.from_mapping(json.loads(source.execute('SELECT value_json FROM app_settings WHERE singleton=1').fetchone()[0]))
    source.close()
    settings=dataclasses.replace(settings,db_instance_id=record['instance_id'])
    started=time.monotonic()
    print(json.dumps({'stage':'verify-local','bytes':path.stat().st_size}),flush=True)
    checksum=checksum_file(path)
    assert checksum.size_bytes==record['file_size']
    assert not record['checksum_crc64'] or checksum.crc64==record['checksum_crc64']
    assert not record['local_sha256'] or checksum.sha256==record['local_sha256']
    item=RemoteBinlog(**{f.name:record[f.name] for f in dataclasses.fields(RemoteBinlog) if f.name in record})
    credential=None
    if settings.oss_auth_mode=='access_key':
        from app.credentials import load_credential
        credential=load_credential(settings.credential_target)
    archive=OssArchive(settings,credential=credential)
    with tempfile.TemporaryDirectory(prefix='raw-archive-benchmark-') as directory:
        meta=MetadataStore(Path(directory)/'metadata.sqlite3')
        file_id,_=meta.upsert_remote(settings,item)
        assert file_id==record['id']
        meta.set_file_state(file_id,'downloaded',local_sha256=checksum.sha256)
        store=RawBinlogStore(meta)
        print(json.dumps({'stage':'scan-and-upload','checksum_seconds':round(time.monotonic()-started,3)}),flush=True)
        stage=time.monotonic()
        descriptor=store.archive(archive,path,file_id,item,'mysql')
        store.verify(archive,descriptor)
        budget=Budget()
        try:
            index=read_object(archive,descriptor['index'],budget)
            assert index['size']==checksum.size_bytes
        finally:
            budget.close()
        assert path.exists()
        print(json.dumps({'ok':True,'bytes':checksum.size_bytes,'scan_and_upload_seconds':round(time.monotonic()-stage,3),
                          'scan_seconds':descriptor['scan_seconds'],'total_seconds':round(time.monotonic()-started,3),
                          'header_events':index['events'],'tables':len(index['tables']),
                          'transaction_ranges':len(index['regions']),
                          'sidecar_bytes':descriptor['index']['size_bytes']}),flush=True)

if __name__=='__main__':
    run(Path(sys.argv[1]),Path(sys.argv[2]))
