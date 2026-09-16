import types
import unittest
from unittest.mock import patch

from app import oss_crc


class OssCrcTests(unittest.TestCase):
    def setUp(self):
        oss_crc.report_crc_backend.cache_clear()
        self.addCleanup(oss_crc.report_crc_backend.cache_clear)

    def test_known_whole_and_streamed_crc64_vectors(self):
        result = oss_crc.verify_crc_runtime()
        self.assertEqual(result['vectors'], 3)
        self.assertTrue(result['checksumEnabled'])

    def test_native_backend_logs_once(self):
        with patch.object(oss_crc, 'crc_backend', return_value='native'), self.assertLogs(oss_crc.LOGGER, level='INFO') as logs:
            for _ in range(3):
                self.assertEqual(oss_crc.report_crc_backend(), 'native')
        self.assertEqual(len(logs.output), 1)
        self.assertIn('checksum=enabled', logs.output[0])

    def test_fallback_is_visible_and_native_gate_rejects_it(self):
        for backend in ['python', 'unknown']:
            oss_crc.report_crc_backend.cache_clear()
            with self.subTest(backend=backend), patch.object(oss_crc, 'crc_backend', return_value=backend), self.assertLogs(oss_crc.LOGGER, level='WARNING') as logs:
                with self.assertRaisesRegex(RuntimeError, 'refusing slow fallback'):
                    oss_crc.verify_crc_runtime(require_native=True)
            self.assertIn('reason=', logs.output[0])

    def test_detects_actual_selected_implementation(self):
        for value, expected in [(True, 'native'), (False, 'python'), (None, 'unknown')]:
            with self.subTest(value=value), patch.object(oss_crc.importlib, 'import_module', return_value=types.SimpleNamespace(_usingExtension=value)):
                self.assertEqual(oss_crc.crc_backend(), expected)

    def test_corrupted_crc_rejected(self):
        class BadCrc:
            crc = 42
            def __call__(self, data): pass
        with patch.object(oss_crc.oss2.utils, 'Crc64', BadCrc):
            with self.assertRaisesRegex(RuntimeError, 'checksum vector mismatch'):
                oss_crc.verify_crc_runtime()


if __name__ == '__main__':
    unittest.main()
