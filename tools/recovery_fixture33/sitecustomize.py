"""CI-only cloud-edge and observation hooks. Never packaged in service images."""
import os
if os.environ.get('RDS_RECOVERY_FIXTURE')=='1':
    try:
        import functools,json,signal,time,traceback
        from pathlib import Path
        root=Path('/control');guard=json.loads(Path('/fixture/guard.json').read_text())
        assert guard['scope']=='sql-insight-recovery-ci' and os.environ.get('RDS_BINLOG_TEST_MODE')=='1'
        def note(kind,**values):
            raw=(json.dumps({'kind':kind,'atNs':time.time_ns(),'pid':os.getpid(),**values})+'\n').encode()
            fd=os.open(root/'application.jsonl',os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o666)
            try:os.write(fd,raw);os.fsync(fd)
            finally:os.close(fd)
        def fault(stage,**values):
            if os.environ.get('RECOVERY_FAULT_ACTOR')!='1':return
            config=json.loads((root/'fault.json').read_text())
            if config['stage']!=stage:return
            try:fd=os.open(root/'fired.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o666)
            except FileExistsError:return
            with os.fdopen(fd,'w') as f:json.dump({'stage':stage,'atNs':time.time_ns(),'pid':os.getpid(),**values},f);f.flush();os.fsync(f.fileno())
            # Hold precisely this boundary until the external, scoped whole-stack SIGKILL.
            os.kill(os.getpid(),signal.SIGSTOP)
            raise RuntimeError('fault-injection process unexpectedly resumed')
        from app import downloader,pipeline,metadata,storage,oss_store,mock_api
        import oss2
        original_download=downloader.download_file
        @functools.wraps(original_download)
        def download(*args,**kwargs):
            result=original_download(*args,**kwargs);fault('raw_fsync',path=str(result.path));return result
        downloader.download_file=download;pipeline.download_file=download
        original_finish=storage.EventStorage._finish_stage
        @functools.wraps(original_finish)
        def finish(*args,**kwargs):
            result=original_finish(*args,**kwargs)
            if result[0]>0:fault('parquet_commit',fileId=kwargs.get('file_id'),parts=[p['path'] for p in result[1]])
            return result
        storage.EventStorage._finish_stage=finish
        original_chunk=metadata.MetadataStore.record_file_chunk_progress
        @functools.wraps(original_chunk)
        def chunk(*args,**kwargs):
            result=original_chunk(*args,**kwargs);fault('chunk_commit',fileId=args[1] if len(args)>1 else kwargs['file_id']);return result
        metadata.MetadataStore.record_file_chunk_progress=chunk
        original_put=oss2.Bucket.put_object_from_file
        @functools.wraps(original_put)
        def put(*args,**kwargs):
            result=original_put(*args,**kwargs);fault('oss_upload_before_verify',key=args[1]);return result
        oss2.Bucket.put_object_from_file=put
        original_head=oss_store.OssArchive._head_pack_verified
        @functools.wraps(original_head)
        def head(*args,**kwargs):
            result=original_head(*args,**kwargs)
            if result is not None:note('archive_head_verified',key=args[1])
            return result
        oss_store.OssArchive._head_pack_verified=head
        original_single_head=oss_store.OssArchive._head_verified
        @functools.wraps(original_single_head)
        def single_head(*args,**kwargs):
            result=original_single_head(*args,**kwargs)
            if result is not None:note('archive_head_verified',key=args[1])
            return result
        oss_store.OssArchive._head_verified=single_head
        from contextlib import contextmanager
        original_connection=metadata.MetadataStore.connection;observed_connections=set()
        @contextmanager
        def connection(self,*args,**kwargs):
            with original_connection(self,*args,**kwargs) as conn:
                key=(os.getpid(),id(self))
                if key not in observed_connections:
                    observed_connections.add(key)
                    note('sqlite_durability',synchronous=conn.execute('PRAGMA synchronous').fetchone()[0],wal_autocheckpoint=conn.execute('PRAGMA wal_autocheckpoint').fetchone()[0])
                yield conn
        metadata.MetadataStore.connection=connection
        original_list=mock_api.ManifestRdsClient.list_binlogs
        @functools.wraps(original_list)
        def listing(*args,**kwargs):
            deadline=time.monotonic()+110
            while not (root/'release').exists():
                if time.monotonic()>deadline:raise TimeoutError('fixture source release deadline')
                time.sleep(.1)
            return original_list(*args,**kwargs)
        mock_api.ManifestRdsClient.list_binlogs=listing
        from app.slow_log_collector import DasRpcClient
        def cloud_call(self,action,params):
            if action!='DescribeSlowLogRecords':raise RuntimeError('unexpected fixture cloud action '+action)
            note('fixture_cloud_idle',action=action);return {'Data':{'Logs':[],'TotalRecords':0}}
        DasRpcClient.call=cloud_call
        original_unlink=Path.unlink
        @functools.wraps(original_unlink)
        def unlink(path,*args,**kwargs):
            if str(path).startswith('/data/downloads/') and path.suffix=='.binlog':note('raw_unlink',path=str(path))
            return original_unlink(path,*args,**kwargs)
        Path.unlink=unlink
        from app.clickhouse_client import ClickHouseClient
        original_rows=ClickHouseClient.json_rows
        @functools.wraps(original_rows)
        def rows(self,*args,**kwargs):
            import sys
            control=sys._getframe(1).f_locals.get('control');qid=(kwargs.get('settings') or {}).get('query_id')
            if qid and getattr(control,'task_id',None):note('owned_query',queryId=qid,taskId=control.task_id)
            return original_rows(self,*args,**kwargs)
        ClickHouseClient.json_rows=rows
        note('fixture_hooks_installed',actor=os.environ.get('RECOVERY_FAULT_ACTOR','0'))
    except BaseException:
        import traceback,sys
        traceback.print_exc(file=sys.stderr);sys.stderr.flush();os._exit(78)
