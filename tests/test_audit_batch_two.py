from __future__ import annotations
from contextlib import closing
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from app.sql_lex import explain_safe, statement_verb
from app.select_explain import SelectExplainWorker, SelectExplainConfig, estimate_examined_rows, _SCHEMA
from app.general_log_collector import GeneralLogCollector, GeneralLogConfig, _is_read_statement, _read_operation
from app.collector_health import collector_health
from app.io_pressure import read_full_avg10, IoPressurePaused
from app.storage import EventStorage


class SqlSafety(unittest.TestCase):
    def test_dangerous_explain_never_reaches_a_source_connection(self):
        sqls = ['SELECT SLEEP(10) FROM x','SELECT BENCHMARK(1000,1) FROM x',
                'SELECT GET_LOCK(\'x\',10) FROM x','SELECT LOAD_FILE(\'/etc/passwd\') FROM x',
                'SELECT `SLEEP`(10) FROM x','SELECT "SLEEP"(10) FROM x',
                'SELECT custom_udf(1) FROM x','SELECT db.abs(1) FROM x',
                'SELECT @session_var FROM x','SELECT * FROM x; DELETE FROM x',
                'SELECT /*!80000 SLEEP(1) */ 1 FROM x','SELECT 1 INTO OUTFILE \'x\' FROM x',
                'WITH x AS (SELECT 1) DELETE FROM t', 'SELECT * FROM x FOR UPDATE']
        for sql in sqls:
            with self.subTest(sql=sql):
                w=SelectExplainWorker('unused');w._mysql=Mock(side_effect=AssertionError('source touched'))
                self.assertEqual(w.explain_one(sql)['status'],'skip')
                w._mysql.assert_not_called()

    def test_semicolons_in_literals_and_comment_verbs(self):
        self.assertTrue(explain_safe("SELECT * FROM x WHERE a='a;b'"))
        self.assertTrue(explain_safe("SELECT COUNT(*) FROM x"))
        self.assertEqual(statement_verb('# comment\n-- comment\nWITH c AS (SELECT 1) UPDATE x SET a=1'),'UPDATE')
        self.assertFalse(_is_read_statement('WITH c AS (SELECT 1) DELETE FROM x'))
        self.assertTrue(_is_read_statement('# comment\nSELECT * FROM x'))
        self.assertEqual(_read_operation('-- comment\nSELECT * FROM x'),'SELECT')

    def test_ambiguous_database_is_not_guessed(self):
        w=SelectExplainWorker('unused');w._table_dbs=Mock(return_value=['one','two'])
        self.assertEqual(w.resolve_databases('SELECT * FROM x','SELECT * FROM x'),([], 'ambiguous_database_not_guessed'))

    def test_non_mysql_eight_never_runs_explain(self):
        w=SelectExplainWorker('unused');w.resolve_databases=Mock(return_value=(['db'],'init_db'))
        cursor=Mock();cursor.fetchone.return_value=('5.7.44',)
        connection=Mock();connection.cursor.return_value.__enter__=Mock(return_value=cursor)
        connection.cursor.return_value.__exit__=Mock(return_value=False);w._mysql=lambda:connection
        self.assertEqual(w.explain_one('SELECT * FROM x')['status'],'skip')
        self.assertEqual([c.args[0] for c in cursor.execute.call_args_list],['SELECT @@version'])

    def test_zero_filtered_does_not_scan_the_second_join(self):
        self.assertEqual(estimate_examined_rows([dict(rows=10,filtered=0),dict(rows=1000,filtered=100)]),10)

    def test_expired_successful_plan_is_recomputed_oldest_first(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'m.db'
            with closing(sqlite3.connect(path)) as c:
                c.executescript(_SCHEMA+'CREATE TABLE statements(fingerprint TEXT,action TEXT,sample_sql TEXT,last_seen_us INTEGER);')
                c.executemany('INSERT INTO statements VALUES (?,?,?,?)',[('old','SELECT','SELECT 1',1),('new','SELECT','SELECT 2',2)])
                c.execute("INSERT INTO select_explain VALUES ('old','sha','','',0,0,'[]','ok','','2000-01-01T00:00:00Z')")
                c.commit()
            config=SelectExplainConfig();config.enabled=True;config.host='fixture';config.user='fixture';config.password='local-fixture';config.batch=10
            w=SelectExplainWorker(str(path),config);w.explain_one=Mock(return_value={'status':'ok'})
            self.assertEqual(w.run_topup()['ok'],2)
            self.assertEqual([c.args[0] for c in w.explain_one.call_args_list],['SELECT 2','SELECT 1'])


class FingerprintLegacyBoundary(unittest.TestCase):
    def test_existing_version_and_supported_parameterization_remain_stable(self):
        from app.sql_fingerprint import normalize_sql,FINGERPRINT_FORMAT_VERSION,sql_action
        self.assertEqual(FINGERPRINT_FORMAT_VERSION,2)
        self.assertEqual(normalize_sql('SELECT * FROM t WHERE a=12'),normalize_sql('SELECT * FROM t WHERE a=34'))
        self.assertNotEqual(normalize_sql('UPDATE t SET @1=12'),normalize_sql('UPDATE t SET @2=12'))
        # Known v2 boundaries are not silently rewritten within the same index version.
        self.assertNotEqual(normalize_sql('select * from t'),normalize_sql('SELECT * FROM t'))
        self.assertEqual(sql_action('WITH x AS (SELECT 1) SELECT * FROM x'),'OTHER')
        self.assertTrue(normalize_sql('SELECT '+('column_name,'*1000)).endswith(' …'))


class GeneralLogBoundary(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.config=GeneralLogConfig({'instanceId':'rm-fixture','user':'collector','excludeUsers':['app']})
        self.c=GeneralLogCollector(Mock(),SimpleNamespace(paths={'root':self.root}),self.config)
        self.c._ingest_batch=Mock()

    @staticmethod
    def row(sql='SELECT 1',stamp=100,**kwargs):
        return {**dict(epoch_us=stamp,thread_id=1,server_id=1,command_type='Query',user_host='app_reader[app_reader] @ h [10.0.0.1]',sql_text=sql),**kwargs}

    def test_equal_microsecond_rows_are_not_lost_or_duplicated_after_restart(self):
        a,b=self.row(),self.row('SELECT 2')
        self.c._fetch_rows=Mock(return_value=[a]);watermark,n,_=self.c._poll_once(0)
        first=self.c._ingest_batch.call_args.args[0][0]['event_id']
        self.c._save_watermark(watermark,'')
        restarted=GeneralLogCollector(Mock(),SimpleNamespace(paths={'root':self.root}),self.config)
        watermark,_=restarted._load_watermark();restarted._ingest_batch=Mock();restarted._fetch_rows=Mock(return_value=[b,a])
        self.assertEqual(restarted._poll_once(watermark)[:2],(100,1))
        second=restarted._ingest_batch.call_args.args[0][0]['event_id']
        self.assertNotEqual(first,second)
        restarted._ingest_batch.reset_mock();restarted._fetch_rows=Mock(return_value=[a,b])
        self.assertEqual(restarted._poll_once(watermark)[1],0)
        restarted._ingest_batch.assert_not_called()

    def test_true_identical_rows_keep_distinct_occurrences(self):
        a=self.row();self.c._fetch_rows=Mock(return_value=[a,a]);self.c._poll_once(0)
        rows=self.c._ingest_batch.call_args.args[0]
        self.assertEqual(len({r['event_id'] for r in rows}),2)
        self.c._fetch_rows=Mock(return_value=[a,a,a]);self.assertEqual(self.c._poll_once(100)[1],1)
        self.assertNotIn(self.c._ingest_batch.call_args.args[0][0]['event_id'],{r['event_id'] for r in rows})

    def test_ids_do_not_depend_on_batch_position(self):
        a,b=self.row(),self.row('SELECT 2',stamp=101)
        x,_=self.c._build_events([a,b]);y,_=self.c._build_events([b])
        self.assertEqual(x[1]['event_id'],y[0]['event_id'])

    def test_self_account_exact_exclusion_and_use_quit(self):
        rows=[self.row('USE db_one'),self.row(),self.row(command_type='Quit')]
        events,_=self.c._build_events(rows)
        self.assertEqual(events[0]['database_name'],'db_one')
        self.assertNotIn(1,self.c._thread_db)
        own=self.row();own['user_host']='collector[collector] @ h'
        self.assertEqual(self.c._build_events([own])[0],[])
        self.assertEqual(len(events),1)  # app_reader is not the excluded app account

    def test_sample_deadline_and_gaps_survive_restart(self):
        self.c._sample_started_epoch=time.time()-3600;self.config.sample_max_minutes=1
        self.c._status['incompleteIntervals']=[{'reason':'forced_truncate'}];self.c._save_watermark(100,'slot')
        restored=GeneralLogCollector(Mock(),SimpleNamespace(paths={'root':self.root}),self.config)
        self.assertEqual(restored._load_watermark(),(100,'slot'))
        self.assertTrue(restored._sample_window_expired(time.monotonic()))
        self.assertEqual(restored.status()['incompleteIntervals'],[{'reason':'forced_truncate'}])


class ResourceAndReadiness(unittest.TestCase):
    def test_prior_baseline_reveals_a_whole_window_high_plateau(self):
        from app.slowlog_impact import rank_resource_overlap
        from decimal import Decimal
        minute=60_000_000;start=1_790_000_000_000_000//minute*minute
        points=[dict(nodeId='n',timestamp=(start+(i+1)*minute)//1000,Average=90) for i in range(6)]
        row=dict(event_id='e',fingerprint='f',node_id='n',database_name='db',start_us=start,duration_ms=360000,lock_ms=0,scan_rows=10)
        result=rank_resource_overlap([row],points,start_us=start,end_us=start+6*minute,index_complete=True,metric_baselines={'n':Decimal(10)})
        self.assertTrue(result['nodes'][0]['statements'])
        missing=rank_resource_overlap([row],points,start_us=start,end_us=start+6*minute,index_complete=True,metric_baselines={})
        self.assertEqual(missing['nodes'][0]['status'],'metric_baseline_unavailable')

    def test_one_bad_mongo_instance_does_not_discard_a_healthy_one(self):
        from app.mongo_service import MongoService
        with tempfile.TemporaryDirectory() as root:
            bad,good=Mock(),Mock();bad.check.side_effect=RuntimeError('fixture_failure')
            entries=[dict(instanceId='dds-bad',slowlogSource='dds'),dict(instanceId='dds-good',slowlogSource='dds')]
            with patch('app.mongo_service.load_instances',return_value=entries),patch('app.mongo_service.MongoStore',side_effect=[bad,good]),patch('app.mongo_service.MongoCollector') as factory:
                service=MongoService(Path(root),lambda:None,start=False)
            self.assertEqual(set(service.stores),{'dds-good'})
            self.assertEqual(len(service.collectors),1)
            self.assertEqual(service.status()['status'],'partial')
            self.assertEqual(service.status()['initialization_errors'],{'dds-bad':'RuntimeError'})
            service.shutdown()

    def test_dead_thread_not_ready_and_liveness_independent(self):
        collector=SimpleNamespace(status=lambda:dict(enabled=True,instanceId='rm-fixture',lastPollUtc='2026-01-01T00:00:00Z'),
            config=SimpleNamespace(poll_seconds=120),_thread=SimpleNamespace(is_alive=lambda:False))
        health=collector_health(SimpleNamespace(general_logs=[collector]))
        self.assertFalse(health['ok']);self.assertEqual(health['lanes'][0]['state'],'thread_stopped')

    def test_default_index_scope_allowed_and_unknown_identity_rejected(self):
        import threading,urllib.request,urllib.error
        from app.server import AppHTTPServer
        from app.config import Settings
        with tempfile.TemporaryDirectory() as root:
            rows=Mock();rows.coverage.return_value={'gaps':[]}
            app=SimpleNamespace(data_dir=Path(root),secondary_syncs=[],metadata=SimpleNamespace(load_settings=lambda:Settings(db_instance_id='rm-fixture')),
                storage=SimpleNamespace(binlog_rows=rows,_query_window=lambda q,r:(1,2),paths={'index':Path(root)}))
            server=AppHTTPServer(('127.0.0.1',0),app);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            try:
                base='http://127.0.0.1:'+str(server.server_port)
                with urllib.request.urlopen(base+'/api/index-coverage?instance=&database=&table=',timeout=3) as response:
                    self.assertEqual(json.load(response)['data']['scope_instance_id'],'rm-fixture')
                rows.coverage.assert_called_once_with('rm-fixture',1,2)
                with self.assertRaises(urllib.error.HTTPError) as raised:urllib.request.urlopen(base+'/api/index-coverage?instance=unknown',timeout=3)
                self.assertEqual(raised.exception.code,400);raised.exception.close()
            finally:server.shutdown();server.server_close();thread.join(3)

    def test_missing_io_pressure_is_not_zero(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(IoPressurePaused):read_full_avg10(Path(root)/'missing')

    def test_spreadsheet_formulas_are_literal_but_numeric_values_preserved(self):
        row=EventStorage._csv_safe_row({'a':' =cmd()','b':-1,'c':'SELECT 1','d':'@SUM(A1)'})
        self.assertEqual(row,{'a':"' =cmd()",'b':-1,'c':'SELECT 1','d':"'@SUM(A1)"})

    def test_audit_list_bounded_but_detail_remains_complete(self):
        from tests.test_audit_hardening import event
        from app.metadata import MetadataStore
        with tempfile.TemporaryDirectory() as root:
            store=MetadataStore(Path(root)/'meta.db');sql='SELECT '+('1,'*5000)+'1'
            store.record_tabularis_audit_log([event('dbx-fixture',sql_text=sql)])
            row=store.query_tabularis_audit_log({})['rows'][0]
            self.assertEqual(len(row['sql_text']),4096);self.assertTrue(row['sql_truncated'])
            self.assertEqual(row['source_tool'],'dbx')
            self.assertEqual(store.local_execution_event_detail('dbx-fixture')['sql_text'],sql)

    def test_client_cannot_hide_a_system_user_to_get_kill_text(self):
        from tests.test_session_service import ServiceTests,das,idle_trx,T0,SEC
        fixture=ServiceTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        fixture.service._wall_us=lambda:T0+10*SEC
        session=idle_trx(2);session['User']='rdsinternal'
        fixture.answers['rm-x','rn-b']=das(session)
        result=fixture.service.kill_statements(fixture.payload())
        self.assertEqual(result['statements'],[])
        self.assertTrue(result['skipped'])
        fixture.service._wall_us=lambda:T0+60*SEC
        with self.assertRaisesRegex(ValueError,'快照已过期'):fixture.service.kill_statements(fixture.payload())


if __name__=='__main__':unittest.main()
