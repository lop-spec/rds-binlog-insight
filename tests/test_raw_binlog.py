from __future__ import annotations

import gzip
import hashlib
import json
import os
import struct
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.binlog_lite import HEADER, MAGIC, MAX_TIME, RawBinlogError, scan, allows
from app.metadata import MetadataStore
from app.raw_binlog import RawBinlogStore, MAX_FILES, MAX_BYTES, compact
from app.raw_binlog_query import Budget, matches, copy_range, read_object
from app.rds_api import RemoteBinlog


def fixture(path, opaque=False, no_gtid=False):
    data = bytearray(MAGIC)
    def event(kind, body, stamp=100):
        length = 19+len(body)
        data.extend(HEADER.pack(stamp, kind, 1, length, len(data)+length, 0)+body)
    event(15, struct.pack('<H',4)+b'8.0.40'.ljust(50,b'\0')+struct.pack('<I',0)+b'\x13'+b'\0'*40)
    for i, table in enumerate(['one','two','one']):
        if not no_gtid:
            body = b'\0'+b's'*16+struct.pack('<Q',i+1)+b'\x02'+struct.pack('<QQ',i,i+1)
            event(33, body+(100_500_000+i).to_bytes(7,'little'))
        event(19, (i+1).to_bytes(6,'little')+b'\0\0'+b'\x02db\0'+bytes([len(table)])+table.encode()+b'\0\x01\x03\x00\x00')
        event(40 if opaque and i==1 else 30, b'row')
        event(16, struct.pack('<Q',i))
    path.write_bytes(data)
    return bytes(data)


class LiteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)/'input.binlog'

    def test_index_keeps_original_bytes_and_complete_gtid_ranges(self):
        before = fixture(self.path)
        result = scan(self.path)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(len(result['regions']),3)
        self.assertEqual(result['events'],13)
        self.assertEqual(result['lo'],100_000_000)
        self.assertEqual(result['hi'],100_999_999)
        self.assertEqual(result['tables'], [('db','one'),('db','two')])
        selected = [r for r in result['regions'] if allows(r, {'database':'db','table':'two'},0,MAX_TIME)]
        self.assertEqual(len(selected),1)
        self.assertEqual(HEADER.unpack_from(before,selected[0]['start'])[1],33)
        self.assertEqual(HEADER.unpack_from(before,result['prefix_end'])[1],33)
        self.assertEqual(selected[0]['end'],result['regions'][2]['start'])

    def test_compressed_or_unknown_events_cannot_create_false_negatives(self):
        fixture(self.path, opaque=True)
        with self.assertLogs('app.binlog_lite',level='WARNING'):
            result = scan(self.path)
        value = result['regions'][1]
        self.assertTrue(value['unknown'])
        self.assertTrue(allows(value, {'table':'not-in-table-map'},1,2))
        self.assertEqual((value['lo'],value['hi']),(0,MAX_TIME))

    def test_without_gtid_keep_whole_file(self):
        fixture(self.path,no_gtid=True)
        with self.assertLogs('app.binlog_lite',level='WARNING'):
            result = scan(self.path)
        self.assertEqual(result['prefix_end'],4)
        self.assertEqual(len(result['regions']),1)
        self.assertTrue(result['unknown'])

    def test_truncated_event_is_not_a_completed_index(self):
        data = fixture(self.path)
        self.path.write_bytes(data[:-1])
        with self.assertRaises(RawBinlogError):
            scan(self.path)

    def test_unsupported_magic_is_explicit_error(self):
        self.path.write_bytes(b'x'*100)
        with self.assertRaises(RawBinlogError) as caught:
            scan(self.path)
        self.assertEqual(caught.exception.code,'RAW_BINLOG_FORMAT_UNSUPPORTED')

    def test_table_pair_not_cross_product(self):
        entry = dict(lo=0,hi=MAX_TIME,unknown=False,tables=[['a','one'],['b','two']])
        self.assertFalse(allows(entry, {'database':'a','table':'two'},0,MAX_TIME))
        self.assertTrue(allows(entry, {'database':'a','table':'one'},0,MAX_TIME))


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.metadata = MetadataStore(Path(self.temp.name)/'metadata.sqlite3')
        self.store = RawBinlogStore(self.metadata)

    def add_raw(self, index, *, size=100, table='one', instance='test'):
        item = RemoteBinlog(log_file_name=f'mysql-bin.{index:06}', log_begin_utc='2026-09-20T00:00:00Z',
            log_end_utc='2026-09-20T00:00:01Z', file_size=size, checksum_crc64='1',
            download_link='', intranet_download_link='', link_expired_utc='', remote_status='Completed',host_instance_id='node')
        from app.config import Settings
        file_id, _ = self.metadata.upsert_remote(Settings(db_instance_id=instance),item)
        summary = dict(lo=100,hi=200,tables=[['db',table]],unknown=False)
        descriptor = dict(file_id=file_id,raw=dict(size_bytes=size))
        with self.metadata.connection() as conn:
            conn.execute('INSERT INTO raw_binlog_archives VALUES(?,?,?,?,?,?,?)',
                (file_id,100,200,compact(summary),compact(descriptor),time.time()-10,time.time()))
        return file_id

    def test_file_limit_checked_before_any_object_access(self):
        for i in range(MAX_FILES+1):
            self.add_raw(i)
        with self.assertLogs('app.raw_binlog',level='WARNING'):
            with self.assertRaises(RawBinlogError) as caught:
                self.store.plan({'source':'binlog'},100,200)
        self.assertEqual(caught.exception.code,'QUERY_BINLOG_LIMIT')
        self.assertNotIn('raw-binlog',str(caught.exception))

    def test_table_and_instance_pruning_precedes_limit(self):
        for i in range(MAX_FILES+1):
            self.add_raw(i,table='other')
        target=self.add_raw(100,table='target')
        plan=self.store.plan({'instance':'test','table':'target'},100,200)
        self.assertEqual(plan['file_ids'],[target])
        self.assertEqual(self.store.plan({'instance':'elsewhere'},100,200)['candidate_files'],0)

    def test_byte_limit_cannot_be_bypassed_by_one_large_file(self):
        self.add_raw(0,size=MAX_BYTES+1)
        with self.assertRaises(RawBinlogError):
            self.store.plan({},100,200)

    def test_exact_boundary_is_allowed(self):
        for i in range(MAX_FILES):
            self.add_raw(i,size=MAX_BYTES//MAX_FILES)
        self.assertEqual(self.store.plan({},100,200)['candidate_files'],MAX_FILES)

    def test_raw_and_legacy_share_one_combined_file_budget(self):
        for i in range(8):
            self.add_raw(i)
        for i in range(100,109):
            file_id=self.add_raw(i)
            with self.metadata.connection() as conn:
                conn.execute('DELETE FROM raw_binlog_archives WHERE file_id=?',(file_id,))
                conn.execute('INSERT INTO parquet_file_stats VALUES(?,1,100,100,100,200,1,1,1,0)',(file_id,))
                conn.execute("INSERT OR REPLACE INTO parquet_file_stats_state VALUES(1,1,'fixture')")
        with self.assertRaises(RawBinlogError) as caught:
            self.store.plan({},100,200)
        self.assertEqual(caught.exception.code,'QUERY_BINLOG_LIMIT')

    def test_resume_verifies_both_objects_without_decoding_again(self):
        previous={'raw':{},'index':{}}
        with patch.object(self.store,'get',return_value=previous), patch.object(self.store,'verify') as verify, patch('app.raw_binlog.scan') as scanner:
            self.assertIs(self.store.archive(Mock(),Path('unused'),'file',Mock(),'mysql'),previous)
        verify.assert_called_once()
        scanner.assert_not_called()

    def test_submit_rejects_before_task_creation_or_archive_loader(self):
        from app.query_tasks import QueryTaskManager
        archive_loader=Mock()
        storage=SimpleNamespace(query_preflight=Mock(side_effect=RawBinlogError('too many','QUERY_BINLOG_LIMIT')))
        manager=QueryTaskManager(self.metadata,storage,settings_loader=Mock(),archive_loader=archive_loader)
        self.addCleanup(manager.shutdown)
        with self.assertRaises(RawBinlogError): manager.submit({'source':'binlog'})
        archive_loader.assert_not_called()
        self.assertEqual(self.metadata.query_tasks(),[])

    def test_audit_and_slowlog_are_not_binlog_reads(self):
        self.assertEqual(self.store.plan({'source':'audit'},0,MAX_TIME)['candidate_files'],0)
        self.assertEqual(self.store.plan({'source':'slowlog'},0,MAX_TIME)['candidate_files'],0)

    def test_missing_object_keeps_local_file_and_manifest_uncommitted(self):
        archive=Mock()
        archive._head_verified.return_value=None
        with self.assertRaises(RawBinlogError):
            self.store.verify(archive,{'raw':{'oss_key':'raw'},'index':{'oss_key':'index'}})

    def test_crc_failure_is_not_overwritten(self):
        archive=Mock()
        archive._head_verified.return_value={'verified':True}
        archive.bucket.head_object.return_value=SimpleNamespace(headers={'x-oss-hash-crc64ecma':'wrong'})
        with self.assertRaisesRegex(Exception,'CRC64'):
            self.store._head(archive,{'oss_key':'raw','crc64':'123'})
        archive.bucket.put_object_from_file.assert_not_called()

    def test_status_does_not_claim_rows_decoded_or_gaps_restored(self):
        self.add_raw(0)
        status=self.store.status('test')
        self.assertEqual(status['archived_files'],1)
        self.assertFalse(status['row_images_indexed'])
        self.assertFalse(status['source_gaps_verified'])
        self.assertFalse(status['within_24h'])


class RawPipelineTests(unittest.TestCase):
    def manager(self):
        import threading
        from app.pipeline import SyncManager
        manager=SyncManager.__new__(SyncManager)
        manager.metadata=Mock()
        manager._pause_after_current=threading.Event()
        manager._shutdown=threading.Event()
        manager._update_pipeline_status=Mock()
        manager.archive_for_settings=Mock(return_value=Mock())
        manager._record_file_error=Mock(side_effect=RuntimeError('must not suppress failure'))
        return manager

    def test_ready_files_progress_while_earlier_download_waits(self):
        import threading
        from app.config import Settings
        manager=self.manager()
        progressed=threading.Event()
        order=[]
        pending=[(str(i),SimpleNamespace(file_size=100,log_file_name=str(i)),'discovered') for i in range(7)]
        def download(job,client,settings,file_id,item):
            if file_id=='0':
                self.assertTrue(progressed.wait(3),'head-of-line barrier returned')
            return Path(file_id),'sha'
        def process(job,client,settings,file_id,*args,**kwargs):
            order.append(file_id)
            if file_id=='2': progressed.set()
            return 0
        manager._download=download
        manager._process_one=process
        result=manager._run_pending_raw('job',None,Settings(),pending,'mysql',Mock(),completed=0,unavailable=0)
        self.assertEqual(result,(7,0,False))
        self.assertLess(order.index('2'),order.index('0'))
        self.assertEqual(len(set(order)),7)
        self.assertTrue(all(len(c.kwargs.get('inFlightFiles',[]))<=4 for c in manager._update_pipeline_status.call_args_list))

    def test_pause_stops_admission_and_retains_pending_work(self):
        from app.config import Settings
        manager=self.manager()
        manager._pause_after_current.set()
        manager._download=Mock()
        manager._process_one=Mock()
        pending=[('0',SimpleNamespace(file_size=100,log_file_name='zero'),'discovered')]
        result=manager._run_pending_raw('job',None,Settings(),pending,'mysql',Mock(),completed=0,unavailable=0)
        self.assertEqual(result,(0,0,True))
        manager._download.assert_not_called()
        manager._process_one.assert_not_called()


class QueryTests(unittest.TestCase):
    def test_literal_substrings_exact_schema_and_operations(self):
        storage=SimpleNamespace(exact_index=Mock())
        row=dict(event_epoch_us=100,operation='UPDATE',database_name='Db',table_name='test',sql_text='a_% value')
        self.assertTrue(matches(storage,row,{'keyword':'_%','database':'db','operations':['update']},0,200,{}))
        self.assertFalse(matches(storage,row,{'keyword':'no_%'},0,200,{}))
        storage.exact_index.primary_key_match.return_value=None
        with self.assertRaises(RawBinlogError) as caught:
            matches(storage,row,{'exact':{'value':'1'}},0,200,{})
        self.assertEqual(caught.exception.code,'EXACT_SCHEMA_UNKNOWN')

    def test_deadline_and_byte_budget_fail_closed(self):
        budget=Budget()
        self.addCleanup(budget.close)
        budget.until=0
        with self.assertRaises(RawBinlogError): budget.check()
        other=Budget()
        self.addCleanup(other.close)
        with self.assertRaises(RawBinlogError): other.add(MAX_BYTES+1)

    def test_range_response_must_match_requested_offset(self):
        archive=Mock()
        response=archive.bucket.get_object.return_value
        response.headers={'Content-Range':'bytes 0-9/100'}
        budget=Budget()
        self.addCleanup(budget.close)
        with self.assertRaises(RawBinlogError):
            copy_range(archive,dict(oss_key='key',size_bytes=100),10,20,Mock(),budget)
        response.close.assert_called_once()


if __name__=='__main__':
    unittest.main()
