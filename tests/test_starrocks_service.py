import json,tempfile,time,unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from app.starrocks_service import StarRocksService, aggregate, validate_grants

INSTANCE='c-example01'
ENTRY={'instanceId':INSTANCE,'enabled':True,'region':'cn-hangzhou','label':'fixture','credentialsFile':'unused',
       'host':'fe-'+INSTANCE+'-internal.starrocks.aliyuncs.com','port':9030,'readonlyUsername':'mcp-ro'}
GRANTS=[{'UserIdentity':"'mcp-ro'@'%'",'Grants':"GRANT SELECT ON ALL TABLES IN ALL DATABASES TO USER 'mcp-ro'@'%'"}]

def row(**kwargs):
    v=dict(queryId='query1',timestamp=datetime(2026,11,10,10,0),feIp='fe0',stmtId=1,stmt='SELECT * FROM orders WHERE id=1',
           stmt_bytes=len('SELECT * FROM orders WHERE id=1'),db='demo',user='app',clientIp='fixture-client',queryType='query',state='EOF',queryTime=1200,
           scanBytes=10,scanRows=2,cpuCostNs=100,memCostBytes=1000,pendingTimeMs=None)
    v.update(kwargs);return v

class Cursor:
    def __init__(self,connection):self.c=connection;self._result=None
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def execute(self,sql,args=None):
        self.sql=sql;self.c.calls.append((sql,args))
        if sql.startswith('SHOW PROC'):raise Exception(5203,'OPERATE denied')
    def fetchone(self):return {'who':"'mcp-ro'@'%'",'role':self.c.role,'tz':'Asia/Shanghai'}
    def fetchall(self):
        if self.sql=='SHOW GRANTS':return self.c.grants
        if self.sql.startswith('EXPLAIN'):return [{'Explain String':self.c.plan}]
        return []
    def __iter__(self):return iter(self.c.rows)

class Connection:
    def __init__(self,rows):self.rows=rows;self.calls=[];self.grants=GRANTS;self.role='NONE';self.plan='partitions=1/34';self.open=True;self._result=None;self.closed=0
    def cursor(self,*args):return Cursor(self)
    def close(self):self.open=False;self.closed+=1

class StarRocksServiceTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        (self.root/'starrocks-instances.json').write_text(json.dumps([ENTRY]))
        self.connection=Connection([row()])
        self.service=StarRocksService(self.root,lambda:None,lambda _:None,connect=lambda e:self.connection)
        end=time.time_ns()//1000-120000000
        self.params={'instance':INSTANCE,'mode':'audit','startEpochUs':str(end-300000000),'endEpochUs':str(end)}
    def tearDown(self):self.temp.cleanup()
    def test_audit_is_parameterized_and_socket_closed_once(self):
        self.params['database']="x' OR 1=1 --"
        result=self.service.database_query(self.params)
        self.assertEqual(len(result['rows']),1)
        self.assertFalse(result['coverage']['complete'])
        sql,args=next((s,a) for s,a in self.connection.calls if s.startswith('SELECT `queryId`'))
        self.assertNotIn(self.params['database'],sql);self.assertIn(self.params['database'],args)
        self.assertEqual(self.connection.closed,1)
        self.assertTrue(any(s=='SET query_timeout=10' for s,a in self.connection.calls))
        self.assertFalse(any('KILL' in s or 'SET GLOBAL' in s for s,a in self.connection.calls))
    def test_main_audit_filters_are_bound_and_status_checked(self):
        params={**self.params,'keyword':"x' OR 1=1",'connection':'fixture-client','status':'failed'}
        self.service.database_query(params)
        sql,args=next((s,a) for s,a in self.connection.calls if s.startswith('SELECT `queryId`'))
        self.assertIn('LOCATE(%s,`stmt`)',sql);self.assertIn(params['keyword'],args)
        self.assertNotIn(params['keyword'],sql);self.assertIn("`state`='ERR'",sql)
    def test_unsupported_main_filters_are_never_silently_ignored(self):
        for values in [{'status':'cancelled'},{'table':'unknown'},{'operation':'INSERT'}]:
            self.connection=Connection([])
            with self.assertRaisesRegex(ValueError,'unsupported_starrocks'):
                self.service.database_query({**self.params,**values})
    def test_wrong_host_refused_without_connection(self):
        self.service.db_entries[INSTANCE]={**ENTRY,'host':'evil.example'}
        with self.assertRaisesRegex(ValueError,'fixed_database_target'):self.service.database_query(self.params)
        self.assertFalse(self.connection.calls)
    def test_non_readonly_permission_and_role_refused(self):
        self.connection.grants=[{'Grants':"GRANT OPERATE ON SYSTEM TO USER 'mcp-ro'@'%'",'UserIdentity':"'mcp-ro'@'%'"}]
        with self.assertRaisesRegex(RuntimeError,'not_readonly'):self.service.database_query(self.params)
        self.assertEqual(self.connection.closed,1)
        c=Connection([]);c.role='admin';self.service.connect_factory=lambda _:c
        with self.assertRaisesRegex(RuntimeError,'authenticated_identity'):self.service.database_query(self.params)
    def test_global_inflight_is_permission_denied_not_empty_success(self):
        result=self.service.database_query({**self.params,'mode':'running'})
        self.assertEqual(result['status'],'permission_denied');self.assertFalse(result['complete'])
        self.assertFalse(any(s=='SHOW FULL PROCESSLIST' for s,a in self.connection.calls))
    def test_excessive_window_and_partitions_rejected(self):
        with self.assertRaisesRegex(ValueError,'max_one_hour'):
            self.service.database_query({**self.params,'startEpochUs':str(int(self.params['endEpochUs'])-7200000000)})
        self.connection=Connection([]);self.connection.plan='partitions=34/34'
        with self.assertRaisesRegex(RuntimeError,'partition_guard'):self.service.database_query(self.params)
    def test_row_cap_marks_partial_without_double_close(self):
        self.connection.rows=[row(queryId=str(i)) for i in range(5)]
        result=self.service.database_query({**self.params,'limit':'2'})
        self.assertEqual(len(result['rows']),2);self.assertTrue(result['coverage']['truncated'])
        self.assertEqual(self.connection.closed,1)
    def test_duplicate_query_and_slow_record_are_one_execution(self):
        result=aggregate([row(),row(queryType='slow_query')])
        self.assertEqual(result['observed_executions'],1)
        self.assertEqual(result['groups'][0]['costs']['queryTime']['value'],1200)
        self.assertEqual(result['groups'][0]['peak_memory'],1000)
        self.assertIsNone(result['groups'][0]['costs']['pendingTimeMs']['value'])
    def test_parameters_normalize_and_memory_is_max_not_sum(self):
        result=aggregate([row(),row(queryId='query2',stmt='SELECT * FROM orders WHERE id=2',memCostBytes=2000)])
        self.assertEqual(len(result['groups']),1)
        self.assertEqual(result['groups'][0]['count'],2)
        self.assertEqual(result['groups'][0]['peak_memory'],2000)
    def test_unknown_query_ids_do_not_merge_different_connections(self):
        r=aggregate([row(queryId=None),row(queryId=None,clientIp='another')])
        self.assertEqual(r['observed_executions'],2)
    def test_grant_option_and_empty_grants_refused(self):
        for rows in [[],[{'UserIdentity':"'mcp-ro'@'%'",'Grants':"GRANT SELECT ON ALL TABLES IN ALL DATABASES TO USER 'mcp-ro'@'%' WITH GRANT OPTION"}]]:
            with self.assertRaises(RuntimeError):validate_grants(rows,'mcp-ro')

if __name__=='__main__':unittest.main()
