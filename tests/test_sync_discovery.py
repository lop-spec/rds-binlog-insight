import tempfile
import threading
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.config import Settings
from app.credentials import CloudCredential
from app.metadata import MetadataStore
from app.pipeline import PreparedBinlog, SyncManager
from app.rds_api import RdsApiError, RdsRpcClient
from app.sync_discovery import RetainedDiscovery, fair_pending
from tests.test_core import remote


def item(name, age=0, host='host-a'):
    when = (datetime.now(UTC)-timedelta(hours=age)).strftime('%Y-%m-%dT%H:%M:%SZ')
    return remote(name, when, host=host)


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.settings = Settings(db_instance_id='rm-test000001')
        self.manager = SyncManager.__new__(SyncManager)
        self.manager.metadata = MetadataStore(self.root/'metadata.sqlite3')
        self.manager.storage = SimpleNamespace(paths={'downloads':self.root})
        self.manager._pause_after_current = threading.Event()
        self.manager._shutdown = threading.Event()
        self.manager._event = Mock()
        self.manager._update_pipeline_status = Mock()
        self.job = self.manager.metadata.create_job('sync', self.settings.db_instance_id)

    def test_fair_admission_deduplicates_and_preserves_each_cohort_order(self):
        entries = [(f'old-{i}', item(f'bin.{i:06}', 4), 'discovered') for i in range(10)]
        entries += [(f'new-{i}', item(f'bin.{i+10:06}'), 'discovered') for i in range(3)]
        entries.append(entries[-1])
        result = fair_pending(list(reversed(entries)),datetime.now(UTC))
        self.assertEqual([e[0] for e in result[:6]],['old-0','new-0','old-1','new-1','old-2','new-2'])
        self.assertEqual(len(result),13)

    def test_cached_restart_and_fresh_frontier_do_not_wait_for_full_scan(self):
        old = item('bin.000001',4)
        old_id = self.manager.metadata.upsert_remote(self.settings,old)[0]
        entered, release = threading.Event(), threading.Event()
        fresh = item('bin.000002')
        def pages(begin,end):
            if datetime.fromisoformat(begin.replace('Z','+00:00')) < datetime.now(UTC)-timedelta(hours=2):
                entered.set()
                if not release.wait(3): raise AssertionError('scan not released')
                yield [old,fresh]
            else: yield [fresh]
        client = SimpleNamespace(iter_binlog_batches=pages)
        with RetainedDiscovery(self.manager,self.job,client,self.settings,'host-a') as discovery:
            try:
                self.assertTrue(entered.wait(2))
                pending = discovery.pending()
                self.assertEqual({e[1].log_file_name for e in pending},{'bin.000001','bin.000002'})
                self.assertIn(old_id,{e[0] for e in pending})
                self.assertFalse(discovery.finished)
            finally: release.set()
            self.assertTrue(discovery.future.result(timeout=2))

    def test_unavailable_done_synthetic_and_other_host_are_not_readmitted(self):
        records = [item('bin.1'),item('bin.2'),item('bin.3'),item('slow-log/fixture'),item('bin.5',host='host-b')]
        ids = self.manager.metadata.upsert_remotes(self.settings,records)
        self.manager.metadata.set_file_state(ids[0][0],'unavailable')
        self.manager.metadata.set_file_state(ids[1][0],'done')
        client = SimpleNamespace(iter_binlog_batches=lambda *_:iter([records]))
        with RetainedDiscovery(self.manager,self.job,client,self.settings,'host-a') as discovery:
            discovery.future.result(timeout=2)
            pending = discovery.pending()
            self.assertEqual([e[1].log_file_name for e in pending],['bin.3'])
            self.assertEqual(self.manager.metadata.file_record(ids[1][0])['download_link'],'')

    def test_interrupted_other_host_resumes_only_without_primary_window_duplicate(self):
        when = (datetime.now(UTC)-timedelta(hours=4)).strftime('%Y-%m-%dT%H:%M:%SZ')
        duplicate = remote('bin.other',when,host='host-b')
        independent = item('bin.recoverable',5,host='host-b')
        for record in (duplicate,independent):
            file_id,_ = self.manager.metadata.upsert_remote(self.settings,record)
            self.manager.metadata.set_file_state(file_id,'stored')
        primary = remote('bin.primary',when)
        client = SimpleNamespace(iter_binlog_batches=lambda *_:iter([[primary]]))
        with RetainedDiscovery(self.manager,self.job,client,self.settings,'host-a') as discovery:
            discovery.future.result(timeout=2)
            names = [entry[1].log_file_name for entry in discovery.pending()]
            self.assertEqual(set(names),{'bin.primary','bin.recoverable'})

    def test_pause_stops_retained_scan_between_pages_without_claiming_complete(self):
        calls = []
        def pages(*_):
            calls.append(1)
            self.manager._pause_after_current.set()
            yield [item('bin.1')]
            calls.append(2)
            yield [item('bin.2')]
        with RetainedDiscovery(self.manager,self.job,SimpleNamespace(iter_binlog_batches=pages),self.settings,'host-a') as discovery:
            self.assertFalse(discovery.future.result(timeout=2))
            self.assertEqual(discovery.pending(),[])
        self.assertEqual(calls,[1])
        codes = [c.args[2] for c in self.manager._event.call_args_list]
        self.assertIn('RETAINED_DISCOVERY_INTERRUPTED',codes)
        self.assertNotIn('RETAINED_DISCOVERY_COMPLETE',codes)

    def test_discovery_failure_is_propagated_not_treated_as_caught_up(self):
        def pages(*_): raise RuntimeError('fixture listing failure'); yield
        with self.assertLogs('app.sync_discovery',level='ERROR'), self.assertRaisesRegex(RuntimeError,'listing failure'):
            with RetainedDiscovery(self.manager,self.job,SimpleNamespace(iter_binlog_batches=pages),self.settings,'host-a') as discovery:
                discovery.future.result(timeout=2)

    def test_bounded_metadata_upsert_batches_and_preserves_done_links(self):
        records = [item(f'bin.{i:06}') for i in range(805)]
        first = self.manager.metadata.upsert_remotes(self.settings,records)
        self.manager.metadata.set_file_state(first[0][0],'done')
        second = self.manager.metadata.upsert_remotes(self.settings,records)
        self.assertEqual(second[0][1],'done')
        self.assertEqual(len(second),805)
        self.assertEqual(self.manager.metadata.file_record(first[0][0])['download_link'],'')
        self.assertEqual(len(self.manager.metadata.recoverable_files(self.settings.db_instance_id)),0)
        self.assertEqual(len(self.manager.metadata.recoverable_files(self.settings.db_instance_id,include_discovered=True)),804)

    def test_consumer_recovers_before_full_scan_and_waits_for_retained_completion(self):
        old, fresh = item('bin.old',4), item('bin.fresh')
        self.manager.metadata.upsert_remote(self.settings,old)
        consumed=threading.Event()
        def pages(begin,end):
            if datetime.fromisoformat(begin.replace('Z','+00:00')) < datetime.now(UTC)-timedelta(hours=2):
                if not consumed.wait(3): raise AssertionError('consumer blocked behind full scan')
                yield [old,fresh]
            else: yield [fresh]
        observed=[]
        def run_batch(job,client,settings,pending,flavor,archive,*,completed,unavailable,refresh_pending):
            observed.extend(entry[1].log_file_name for entry in pending)
            for file_id,_,_ in pending: self.manager.metadata.set_file_state(file_id,'done')
            consumed.set()
            return completed+len(pending),unavailable,False
        self.manager._run_pending_parallel=run_batch
        result=self.manager._consume_pending(self.job,SimpleNamespace(iter_binlog_batches=pages),self.settings,'mysql',None,'host-a')
        self.assertEqual(result,(2,0,False))
        self.assertEqual(set(observed),{'bin.old','bin.fresh'})
        self.assertIn('RETAINED_DISCOVERY_COMPLETE',[c.args[2] for c in self.manager._event.call_args_list])

    def test_explicit_time_range_keeps_original_discovery_path(self):
        self.manager._discover=Mock(return_value=[])
        client=Mock()
        start,end=datetime.now(UTC)-timedelta(days=1),datetime.now(UTC)
        result=self.manager._consume_pending(self.job,client,self.settings,'mysql',None,'host-a',start_utc=start,end_utc=end)
        self.assertEqual(result,(0,0,False))
        client.iter_binlog_batches.assert_not_called()
        self.manager._discover.assert_called_once_with(client,self.settings,primary_host_instance_id='host-a',start_utc=start,end_utc=end)

    def run_refresh(self, *, pause=False, fail_refresh=False):
        pending = [(f'file-{i}',item(f'bin.{i:06}',4), 'discovered') for i in range(6)]
        fresh = ('fresh',item('bin.999999'),'discovered')
        downloads,commits,refreshes = [],[],[]
        def download(_job,_client,_settings,file_id,*_):
            downloads.append(file_id)
            path=self.root/(file_id+'.binlog');path.write_bytes(b'fixture')
            return path,'sha'
        def process(_job,_client,_settings,file_id,remote,*_,**kwargs):
            if pause: self.manager._pause_after_current.set()
            return PreparedBinlog(file_id,remote,kwargs['prepared_download'][0],1,.01)
        def commit(_job,_settings,prepared):
            commits.append(prepared.file_id); prepared.raw_path.unlink()
        def refresh():
            refreshes.append(1)
            if fail_refresh: raise RuntimeError('refresh failed')
            return [e for e in [fresh,*pending] if e[0] not in commits]
        self.manager._download=download; self.manager._process_one=process; self.manager._commit_prepared=commit
        self.manager.metadata=Mock()
        self.manager.metadata.file_record.return_value={'event_count':1}
        result=self.manager._run_pending_parallel(self.job,object(),self.settings,pending,'mysql',None,
            completed=0,unavailable=0,refresh_pending=refresh)
        return result,downloads,commits,refreshes

    def test_refresh_keeps_prefetched_files_unique_and_admits_fresh_before_exhaustion(self):
        result,downloads,commits,refreshes=self.run_refresh()
        self.assertEqual(result,(7,0,False))
        self.assertEqual(len(downloads),len(set(downloads)))
        self.assertEqual(commits[:4],[f'file-{i}' for i in range(4)])
        self.assertLess(commits.index('fresh'),commits.index('file-5'))
        self.assertTrue(refreshes)

    def test_pause_happens_before_discovery_callback(self):
        result,_,commits,refreshes=self.run_refresh(pause=True)
        self.assertEqual(result,(2,0,True)); self.assertEqual(len(commits),2); self.assertEqual(refreshes,[])

    def test_callback_error_propagates_without_committing_prefetched_files(self):
        with self.assertRaisesRegex(RuntimeError,'refresh failed'):
            self.run_refresh(fail_refresh=True)
        self.assertTrue((self.root/'file-2.binlog').is_file())


class PageIteratorTests(unittest.TestCase):
    def client(self,payloads):
        client=RdsRpcClient(Settings(db_instance_id='rm-test000001'),CloudCredential('fixture','fixture'))
        client.call=Mock(side_effect=payloads)
        return client

    def test_page_is_yielded_before_next_rpc(self):
        record={'LogFileName':'bin.1','LogBeginTime':'2026-09-16T10:00:00Z','LogEndTime':'2026-09-16T10:01:00Z','FileSize':123,'HostInstanceID':'host-a','RemoteStatus':'Completed'}
        client=self.client([{'TotalRecordCount':2,'Items':{'BinLogFile':[record]}},{'TotalRecordCount':2,'Items':{'BinLogFile':[dict(record,LogFileName='bin.2')]}}])
        batches=client.iter_binlog_batches('start','end')
        self.assertEqual(next(batches)[0].log_file_name,'bin.1');self.assertEqual(client.call.call_count,1)
        self.assertEqual(next(batches)[0].log_file_name,'bin.2')
        self.assertEqual(list(batches),[])

    def test_truncated_api_pagination_fails_explicitly(self):
        client=self.client([{'TotalRecordCount':2,'Items':{'BinLogFile':[]}}])
        with self.assertRaises(RdsApiError) as error: list(client.iter_binlog_batches('start','end'))
        self.assertEqual(error.exception.code,'INCOMPLETE_PAGINATION')


if __name__=='__main__': unittest.main()
