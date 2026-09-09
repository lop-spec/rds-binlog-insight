"""Replay private, fixed incident fixtures through the production engine adapter.

Fixtures/config remain outside the public repo. No query runs on the business DB.
"""
from __future__ import annotations
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.mongo_store import MongoStore, atomic_json
from app.mongo_insight import WINDOW, epoch_us, analyze


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--source-dir',type=Path,required=True)
    p.add_argument('--cases-file',type=Path,required=True)
    p.add_argument('--data-dir',type=Path,required=True)
    p.add_argument('--backend',choices=['parquet','clickhouse'],default='parquet')
    p.add_argument('--archive',action='store_true')
    p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();config=json.loads(args.cases_file.read_text(encoding='utf-8'))
    store=MongoStore(args.data_dir,backend=args.backend);store.check()
    archive=None
    if args.archive:
        from app.metadata import MetadataStore
        from app.credentials import load_credential
        from app.oss_store import OssArchive
        settings=MetadataStore(args.data_dir/'metadata.sqlite3',run_migrations=False).load_settings()
        archive=OssArchive(settings,credential=load_credential(settings.credential_target))
    results=[]
    for case in config['cases']:
        begin=time.monotonic();instance=case['instance'];expected_records=0
        for file in case['sources']:
            source=json.loads((args.source_dir/file).read_text(encoding='utf-8'))
            if not source['complete'] or len(source['rows'])!=source['totals'][-1]:
                raise AssertionError('incomplete fixture '+file)
            start=epoch_us(source['day']+'T'+source['start']+':00+08:00')
            end=epoch_us(source['day']+'T'+source['end']+':00+08:00')
            windows={t:[] for t in range(start,end,WINDOW)}
            for row in source['rows']:
                t=epoch_us(row['ExecutionStartTime'])//WINDOW*WINDOW
                if t not in windows:raise AssertionError('fixture outside closed window')
                windows[t].append(row)
            for t,records in windows.items():
                m=store.publish(instance,t,t+WINDOW,records,config['families'],archive=archive)
                expected_records+=m['records']
                print(case['id'],'published',t,m['records'],flush=True)
        points=json.loads((args.source_dir/case['metrics_file']).read_text(encoding='utf-8'))
        store.telemetry(instance,'metrics',points)
        width=store.width(case['start_us'],case['end_us'])
        rows,cov=store.read(instance,case['start_us'],case['end_us'],role='Primary',kind='command',width=width)
        duration=case['end_us']-case['start_us']
        before,bcov=store.read(instance,case['baseline_start_us'],case['baseline_start_us']+duration,role='Primary',kind='command',width=width)
        result=analyze(rows,before,points,case['start_us'],case['end_us'],coverage=cov['complete'],baseline_coverage=bcov['complete'],metric=case['metric'],order=case['order'],limit=200,bucket_width=width)
        assert cov['complete'] and bcov['complete']
        for expected in case.get('expect_top3',[]):
            assert any(expected in row['namespace'] for row in result['statements'][:3]),('Top 3 missed',case['id'],expected)
        if case.get('no_total_growth'):
            assert all(row['count']<=row['baseline_count'] for row in result['totals'])
        if case.get('late_sample'):
            assert any(row['evidence']['sample_after_resource_peak'] and row['max_us']>=60_000_000 for row in result['outliers'])
        # Re-read the same immutable revisions: no duplicated data on replay.
        again,_=store.read(instance,case['start_us'],case['end_us'],role='Primary',kind='command',width=width)
        assert sum(r['count'] for r in rows)==sum(r['count'] for r in again)
        results.append(dict(id=case['id'],status='passed',records=expected_records,elapsed_seconds=round(time.monotonic()-begin,3),
                            source_coverage=cov,baseline_coverage=bcov,analysis=result,
                            root_cause_closed=case.get('root_cause_closed',False)))
        print(case['id'],'PASS',results[-1]['elapsed_seconds'],'seconds',flush=True)
    atomic_json(args.output,{'backend':args.backend,'cases':results,'no_business_database_queries':True})
    print('PASS',len(results),'fixed replays; unresolved causes remain unresolved',flush=True)

if __name__=='__main__':main()
