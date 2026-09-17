import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from tools.recovery_fixture33.stack import Stack

class BootstrapEvidenceTests(unittest.TestCase):
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
