import os,unittest
from unittest.mock import patch
from tools.recovery_fixture33 import prepare_runner

class HostedPreparationTests(unittest.TestCase):
    def test_local_execution_stops_before_commands(self):
        with patch.dict(os.environ,{'GITHUB_ACTIONS':'false'}),patch.object(prepare_runner,'run') as command:
            with self.assertRaisesRegex(AssertionError,'isolated CI only'):prepare_runner.main()
            command.assert_not_called()
    def test_self_hosted_or_other_repository_rejected(self):
        for runner,repo in [('self-hosted','lop-spec/rds-binlog-insight'),('github-hosted','other-project')]:
            with patch.object(prepare_runner,'require_ci'),patch.dict(os.environ,{'RUNNER_ENVIRONMENT':runner,'GITHUB_REPOSITORY':repo}),patch.object(prepare_runner,'run') as command:
                with self.assertRaises(AssertionError):prepare_runner.main()
                command.assert_not_called()
