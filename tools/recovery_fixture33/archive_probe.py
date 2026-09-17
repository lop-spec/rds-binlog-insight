"""Read-only independent archive oracle, run in a fixture-only observer container."""
import hashlib,io,json,sqlite3
from pathlib import Path
from tools.recovery_isolation33 import crc64_xz,SCOPE

def project(row,keys):
    return {key:row[key] for key in keys}

def verify_rows(actual,expected):
    keys=tuple(expected[0]);observed=[project(row,keys) for row in actual]
    ids=[(r['instance_id'],r['event_id']) for r in observed]
    assert len(ids)==len(set(ids)),'duplicate event identity'
    canon=lambda rows:sorted(json.dumps(r,sort_keys=True,ensure_ascii=False) for r in rows)
    assert canon(observed)==canon(expected),'independent content/identity mismatch'
    return observed

def main():
    assert json.loads(Path('/fixture/guard.json').read_text())['scope']==SCOPE
    import pyarrow.parquet as pq
    from app.parser_bridge import NativeChecksumStream
    oracle=json.loads(Path('/fixture/oracle.json').read_text())
    notes=[json.loads(l) for l in Path('/control/application.jsonl').read_text().splitlines()]
    durability=[r for r in notes if r['kind']=='sqlite_durability']
    assert durability and all((r['synchronous'],r['wal_autocheckpoint'])==(2,1000) for r in durability),'application SQLite durability changed'
    db=sqlite3.connect('file:/data/metadata.sqlite3?mode=ro',uri=True);db.row_factory=sqlite3.Row
    integrity=db.execute('PRAGMA integrity_check').fetchone()[0];assert integrity=='ok'
    vectors=0
    for raw in [b'',b'123456789',bytes(range(256))]:
        stream=NativeChecksumStream();stream.update(raw);result=stream.finish()
        assert result.size_bytes==len(raw) and result.sha256==hashlib.sha256(raw).hexdigest() and int(result.crc64)==crc64_xz(raw)
        vectors+=1
    files=[];all_rows=[]
    for source in oracle['files']:
        state=dict(db.execute('SELECT * FROM binlog_files WHERE id=?',(source['id'],)).fetchone())
        parts=[dict(r) for r in db.execute('SELECT * FROM parquet_parts WHERE binlog_id=?',(source['id'],))]
        assert parts and state['state']=='done' and state['event_count']==sum(p['row_count'] for p in parts)==source['expectedEvents'],'checkpoint does not cover complete source'
        objects=[];verified_times=[]
        for part in parts:
            key=part['oss_key'];assert key and part['oss_verified_at']
            raw=(Path('/edge/objects')/key).read_bytes();headers=json.loads((Path('/edge/headers')/(key+'.json')).read_text())
            digest=hashlib.sha256(raw).hexdigest();expected=part['oss_object_sha256'] or part['sha256']
            assert digest==expected==headers['x-oss-meta-sha256'],'archive object SHA mismatch'
            assert int(headers['x-oss-hash-crc64ecma'])==crc64_xz(raw),'archive CRC mismatch'
            start=part['oss_offset'];length=part['oss_length'] or len(raw)
            assert 0<=start and start+length<=len(raw)
            body=raw[start:start+length];assert hashlib.sha256(body).hexdigest()==part['sha256'],'archived Parquet slice mismatch'
            rows=pq.ParquetFile(io.BytesIO(body)).read().to_pylist();assert len(rows)==part['row_count']
            all_rows.extend(rows)
            times=[r['atNs'] for r in notes if r['kind']=='archive_head_verified' and r['key']==key];assert times,'application verification not observed'
            verified_times.append(min(times));objects.append({'key':key,'expectedSha256':expected,'actualSha256':digest,'crcVerified':True,'partSha256':part['sha256'],'rowCount':len(rows)})
        raw_path=Path('/data/downloads')/(source['id']+'.binlog');exists=raw_path.exists()
        deletions=[r['atNs'] for r in notes if r['kind']=='raw_unlink' and r['path']==str(raw_path)]
        after=bool(deletions) and max(verified_times)<min(deletions)
        assert exists or after,'raw removed before all archive verification'
        files.append({'id':source['id'],'instance_id':source['instance_id'],'state':state['state'],'committedEvents':state['event_count'],'expectedEvents':source['expectedEvents'],'objects':objects,'rawExists':exists,'deletionAfterVerification':after})
    verify_rows(all_rows,oracle['rows']);db.close()
    print(json.dumps({'files':files,'sqlite':{'synchronous':2,'wal_autocheckpoint':1000,'integrity_check':integrity},'nativeCrcVectorsPassed':vectors,'archiveRows':len(all_rows)}))

if __name__=='__main__':main()
