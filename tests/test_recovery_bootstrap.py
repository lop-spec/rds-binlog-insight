import io,json,subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from tools.recovery_fixture33.stack import Stack,STATUS,api

class BootstrapEvidenceTests(unittest.TestCase):
    def test_bridge_request_keeps_existing_local_virtual_host(self):
        with patch('tools.recovery_fixture33.stack.urllib.request.urlopen',return_value=io.BytesIO(b'{}')) as fetch:
            self.assertEqual(api('http://172.19.0.3:8769/healthz'),{})
            request=fetch.call_args.args[0];self.assertEqual(request.get_header('Host'),'localhost:8769')
            self.assertEqual(request.full_url,'http://172.19.0.3:8769/healthz')
    def test_recovery_waits_for_every_oracle_source_and_fresh_heartbeats(self):
        with tempfile.TemporaryDirectory() as directory:
            stack=Stack.__new__(Stack);stack.root=Path(directory);stack.fixture=stack.root/'fixture';stack.fixture.mkdir()
            (stack.fixture/'oracle.json').write_text(json.dumps({'files':[{'id':'a'},{'id':'b'}]}))
            for path in STATUS.values():
                f=stack.root/'data'/path;f.parent.mkdir(parents=True,exist_ok=True);f.write_text('{}')
            stack.url='http://172.19.0.3:8769';stack.ch=lambda sql:'1';stack.fault_ns=0
            stack.snapshot=lambda:{name:{'running':True,'restartCount':1,'bridgeAddress':'172.19.0.8'} for name in ['insight','clickhouse',*STATUS]}
            before={name:{'restartCount':0} for name in stack.snapshot()}
            stack.files=lambda:[{'id':'a','state':'done'}]
            with patch('tools.recovery_fixture33.stack.api',return_value={}) as fetch:
                with self.assertRaisesRegex(RuntimeError,'source recovery incomplete'):stack.recovered(before)
                fetch.assert_called_once_with('http://172.19.0.8:8769/healthz')
                stack.files=lambda:[{'id':key,'state':'done'} for key in ['a','b']]
                self.assertEqual(len(stack.recovered(before)),6)
                stack.fault_ns=2**63-1
                with self.assertRaisesRegex(RuntimeError,'stale worker heartbeat'):stack.recovered(before)
    def test_public_probe_address_rejected(self):
        stack=Stack.__new__(Stack)
        with patch('ipaddress.ip_address') as parse:
            parse.return_value.version=4;parse.return_value.is_private=False
            with self.assertRaises(AssertionError):stack.set_app_address('fixture-public-address')
    def exercise(self, failure=None):
        with tempfile.TemporaryDirectory() as directory:
            stack=Stack.__new__(Stack);stack.root=Path(directory);stack.created=[]
            stack.common=lambda:{'RDS_RECOVERY_FIXTURE':'1'};stack.mounts=lambda:[]
            calls=[]
            def create(role,image,options,command):
                self.assertTrue(role.startswith('init-'));self.assertEqual(command[:2],['python','-u'])
                self.assertEqual(command[2],'-c');self.assertIn('faulthandler.dump_traceback_later(20)',command[3])
                name='scoped-'+role;stack.created.append(name);calls.append(name);return name
            stack.create=create
            output=subprocess.CompletedProcess([],0,stdout='migration output\n',stderr='diagnostic stack\n')
            with patch('tools.recovery_fixture33.stack.run',side_effect=failure,return_value='0\n'),patch('tools.recovery_fixture33.stack.subprocess.run',return_value=output),patch('tools.recovery_fixture33.stack.remove_owned') as remove:
                if failure:
                    with self.assertRaises(subprocess.TimeoutExpired):stack.init_container(['python','-m','app.clickhouse_migrate','--raw-oss-tables'])
                else:self.assertEqual(stack.init_container(['python','-m','app.clickhouse_migrate']),'migration output\n')
                remove.assert_called_once_with(calls[0]);self.assertEqual(stack.created,[])
                self.assertEqual(next(stack.root.glob('init-*.log')).read_text(),'migration output\ndiagnostic stack\n')
    def test_success_removes_scoped_helper_and_keeps_output(self):self.exercise()
    def test_timeout_removes_scoped_helper_and_keeps_diagnostics(self):self.exercise(subprocess.TimeoutExpired(['docker','wait'],120))
