"""Same-host losslessness probe; prints counts/hashes only, never row values.

Run with an existing closed binlog. Reads it; does not alter source or serving DB.
The production native parser is the independent full-prefix oracle.
"""
from __future__ import annotations

import collections
import gzip
import hashlib
import io
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import app
# Allows a pre-deployment module-only probe without replacing live application.
if os.environ.get('RAW_PROBE_MODULES'):
    app.__path__.insert(0, os.environ['RAW_PROBE_MODULES'])
from app.binlog_lite import scan, HEADER, MAX_TIME
from app.parser_bridge import parse_ndjson_chunks
from app.raw_binlog_query import Budget, decode


def run(source: Path):
    with tempfile.TemporaryDirectory(prefix='raw-binlog-oracle-') as temp:
        root = Path(temp)
        data = source.open('rb').read(2*1024**2)
        pos, cut, gtids = 4, 0, 0
        while pos+19 <= len(data):
            _, kind, _, size, _, _ = HEADER.unpack_from(data, pos)
            if size < 19 or pos+size > len(data):
                break
            if kind in (33,34):
                gtids += 1
                cut = pos
            pos += size
        assert gtids >= 3, 'Need at least three complete GTID boundaries in probe prefix'
        original=root/'oracle.binlog'
        original.write_bytes(data[:cut])
        started=time.monotonic()
        index=scan(original)
        scan_seconds=time.monotonic()-started
        oracle=[]
        for path in parse_ndjson_chunks(original,'probe-file',root/'oracle',max_lines=1000,max_bytes=8*1024**2):
            with path.open() as handle:
                oracle.extend(json.loads(line) for line in handle)
        # Native IDs use a global output ordinal; raw-v1 deliberately uses
        # a per-GTID ordinal, independently calculated on the FULL oracle.
        ordinals=collections.Counter()
        for row in oracle:
            transaction=str(row.get('gtid') or row.get('transaction_id') or 'ungrouped')
            ordinals[transaction]+=1
            identity='\x1f'.join(('raw-v1','probe-file',transaction,str(ordinals[transaction])))
            row['event_id']=hashlib.sha256(identity.encode()).hexdigest()
        counts=collections.Counter((r.get('database_name'),r.get('table_name')) for r in oracle if r.get('table_name'))
        assert len(counts)>1, 'Probe must contain multiple tables to validate pruning'
        database,table=min(counts,key=counts.get)
        expected=[r for r in oracle if r.get('database_name')==database and r.get('table_name')==table]
        payload=gzip.compress(json.dumps(index).encode(),mtime=0)
        class Bucket:
            def get_object(self,key,byte_range=None):
                if key=='index':
                    result=io.BytesIO(payload)
                    result.headers={}
                    return result
                assert key=='raw'
                a,b=byte_range
                with original.open('rb') as handle:
                    handle.seek(a)
                    body=handle.read(b-a+1)
                result=io.BytesIO(body)
                result.headers={'Content-Range':f'bytes {a}-{b}/{original.stat().st_size}'}
                return result
        entry=dict(raw=dict(oss_key='raw',size_bytes=original.stat().st_size),
                   index=dict(oss_key='index',size_bytes=len(payload),sha256=hashlib.sha256(payload).hexdigest()),
                   file_id='probe-file',instance_id='fixture',host_instance_id='fixture',source_file_name='fixture')
        storage=SimpleNamespace(paths={'scratch':root},raw_binlogs=SimpleNamespace(verify=lambda *_:None))
        budget=Budget()
        try:
            rows=list(decode(storage,SimpleNamespace(bucket=Bucket()),entry,{'database':database,'table':table},0,MAX_TIME,budget))
        finally:
            budget.close()
        actual=[r for r in rows if r.get('database_name')==database and r.get('table_name')==table]
        # All native fields, including IDs, positions, GTID, timestamp, before/
        # after images, schema and SQL, must survive transaction range assembly.
        fields=set().union(*(r.keys() for r in expected))
        canonical=lambda values: sorted(json.dumps({k:r.get(k) for k in fields},sort_keys=True,separators=(',',':')) for r in values)
        if canonical(actual)!=canonical(expected):
            changed=collections.Counter(k for a,b in zip(actual,expected) for k in fields if a.get(k)!=b.get(k))
            print(json.dumps({'mismatch_keys_only':dict(changed),'actual_count':len(actual),'expected_count':len(expected)}))
            raise AssertionError('Ranged decoding differs from full-prefix native oracle')
        detail_budget=Budget()
        try:
            detail_rows=list(decode(storage,SimpleNamespace(bucket=Bucket()),entry,{},0,MAX_TIME,detail_budget,
                                    position=int(actual[0]['event_locator'].split(':')[-1])))
            assert any(r['event_id']==actual[0]['event_id'] for r in detail_rows), 'detail ID changed after transaction narrowing'
        finally:
            detail_budget.close()
        digest=hashlib.sha256('\n'.join(canonical(actual)).encode()).hexdigest()
        print(json.dumps(dict(ok=True,prefix_bytes=original.stat().st_size,
            original_rows=len(oracle),selected_rows=len(actual),decoded_rows=len(rows),
            range_bytes=budget.bytes,range_requests=budget.requests,scan_seconds=round(scan_seconds,6),
            matched_all_native_fields=len(fields),comparison_sha256=digest)))


if __name__=='__main__':
    run(Path(sys.argv[1]))
