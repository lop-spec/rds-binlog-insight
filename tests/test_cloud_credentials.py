from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.config import Settings
from app.credentials import CloudCredential, credential_status, ecs_role_client, load_credential, save_credential
from app.oss_store import _AccessKeyCredentialsProvider, _EcsRamRoleCredentialsProvider
from app.rds_api import RdsRpcClient
from app.slow_log_collector import DasRpcClient


class CloudRoleTests(unittest.TestCase):
    def test_rpc_and_das_refresh_one_consistent_snapshot_per_request(self):
        provider = Mock()
        dynamic = CloudCredential("", "", _provider=provider)
        settings = Settings(db_instance_id="rm-test000001", region_id="cn-hangzhou")
        for client_type in (RdsRpcClient, DasRpcClient):
            provider.get_credential.return_value = SimpleNamespace(access_key_id="old-id", access_key_secret="old-secret", security_token="old-token")
            client = client_type(settings, dynamic)
            for key in ("first", "rotated"):
                provider.get_credential.return_value = SimpleNamespace(access_key_id=key, access_key_secret=key+"-secret", security_token=key+"-token")
                provider.reset_mock()
                actual = client._signed_params("DescribeFixture", {"DBInstanceId": settings.db_instance_id})
                provider.get_credential.assert_called_once_with()
                static = client_type(settings, CloudCredential(key, key+"-secret", key+"-token"))
                # Override generated nonce/time with this request's values to
                # verify that the ID, secret and token came from one generation.
                expected = static._signed_params("DescribeFixture", {"DBInstanceId": settings.db_instance_id, "SignatureNonce": actual["SignatureNonce"], "Timestamp": actual["Timestamp"]})
                self.assertEqual(actual, expected)

    def test_explicit_role_overrides_static_credentials_without_persisting(self):
        provider = Mock()
        provider.get_credential.return_value = SimpleNamespace(access_key_id="role-id", access_key_secret="role-secret", security_token="role-token")
        with patch.dict(os.environ, {"RDS_BINLOG_CLOUD_AUTH_MODE": "ecs_ram_role", "ALIBABA_CLOUD_ECS_METADATA": "fixture-role", "ALIBABA_CLOUD_ACCESS_KEY_ID": "stale", "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "stale"}), patch("app.credentials.ecs_role_client", return_value=provider) as factory, patch("app.credentials._load_file_credential") as file_loader:
            credential = load_credential("fixture")
            self.assertEqual(credential.current().access_key_id, "role-id")
            factory.assert_called_with("fixture-role")
            file_loader.assert_not_called()
            self.assertEqual(credential_status("fixture"), {"present": True, "source": "ecs-ram-role", "maskedAccessKeyId": ""})
            with self.assertRaisesRegex(ValueError, "must not be persisted"):
                save_credential("fixture", credential)

    def test_refresh_failure_and_invalid_mode_never_fall_back(self):
        provider = Mock()
        provider.get_credential.side_effect = RuntimeError("fixture IMDS unavailable")
        with patch.dict(os.environ, {"RDS_BINLOG_CLOUD_AUTH_MODE": "ecs_ram_role"}), patch("app.credentials.ecs_role_client", return_value=provider), patch("app.credentials._environment_credential") as static:
            with self.assertLogs("app.credentials", level="ERROR"), self.assertRaisesRegex(RuntimeError, "IMDS unavailable"):
                load_credential("fixture").validate()
            static.assert_not_called()
        with patch.dict(os.environ, {"RDS_BINLOG_CLOUD_AUTH_MODE": "typo"}), self.assertLogs("app.credentials", level="ERROR"), self.assertRaises(ValueError):
            load_credential("fixture")

    def test_oss_access_key_adapter_also_refreshes_dynamic_credentials(self):
        provider = Mock()
        provider.get_credential.return_value = SimpleNamespace(access_key_id="one", access_key_secret="secret-one", security_token="token-one")
        adapter = _AccessKeyCredentialsProvider(CloudCredential("", "", _provider=provider))
        provider.get_credential.return_value = SimpleNamespace(access_key_id="two", access_key_secret="secret-two", security_token="token-two")
        value = adapter.get_credentials()
        self.assertEqual(value.get_access_key_id(), "two")
        self.assertEqual(value.get_security_token(), "token-two")

    def test_sdk_imdsv2_only_and_shared_role_provider(self):
        ecs_role_client.cache_clear()
        try:
            with patch("alibabacloud_credentials.client.Client") as client:
                first = ecs_role_client("fixture-role")
                self.assertIs(first, ecs_role_client("fixture-role"))
                self.assertIs(first, _EcsRamRoleCredentialsProvider("fixture-role").client)
                config = client.call_args.args[0]
                self.assertEqual(config.type, "ecs_ram_role")
                self.assertEqual(config.role_name, "fixture-role")
                self.assertTrue(config.disable_imds_v1)
                self.assertEqual(config.timeout, 3000)
                client.assert_called_once()
        finally:
            ecs_role_client.cache_clear()

    def test_role_requires_security_token(self):
        provider = Mock()
        provider.get_credential.return_value = SimpleNamespace(access_key_id="id", access_key_secret="secret", security_token="")
        with self.assertLogs("app.credentials", level="ERROR"), self.assertRaisesRegex(ValueError, "no security token"):
            CloudCredential("", "", _provider=provider).current()


if __name__ == "__main__":
    unittest.main()
