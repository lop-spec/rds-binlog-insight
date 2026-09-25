import struct
import subprocess
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

from tools.parser_contract_fixture import (
    mysql_query,
    negative_streams,
    raw_cache_negative_streams,
    read_headers,
    rewrite_events,
    split_events,
    verify_negative_streams,
)


class ParserContractFixtureTests(unittest.TestCase):
    def test_charset_applies_to_ddl_and_mutations_without_set_names(self):
        queries = ["CREATE TABLE t (category ENUM('a','中文'));",
                   "INSERT INTO t VALUES ('中文');", "SELECT '🙂';"]
        with patch('tools.parser_contract_fixture.run', return_value='中文🙂\n'.encode()) as run:
            for query in queries:
                self.assertEqual(mysql_query('owned-fixture', query), '中文🙂\n')
                args, kwargs = run.call_args
                self.assertIn('--default-character-set=utf8mb4', args[0])
                self.assertEqual(args[0][:4], ['docker', 'exec', '-i', 'owned-fixture'])
                self.assertEqual(kwargs['stdin'], query.encode('utf-8'))

    @staticmethod
    def event(payload=b'contract', *, start=4):
        size = 19 + len(payload) + 4
        data = struct.pack('<IBIIIH', 123, 2, 77, size, start + size, 0) + payload
        return data + struct.pack('<I', zlib.crc32(data))

    def test_independent_headers_count_all_framed_events(self):
        a = self.event(); b = self.event(start=4 + len(a))
        headers = read_headers(b'\xfebin' + a + b)
        self.assertEqual(len(headers), 2)
        self.assertEqual(headers[-1]['end'], 4 + len(a) + len(b))

    def test_every_partial_header_or_body_is_rejected(self):
        event = self.event()
        for n in range(1, len(event)):
            with self.subTest(n=n), self.assertRaises(ValueError):
                read_headers(b'\xfebin' + event[:n])

    def test_crc_corruption_is_not_silent_eof(self):
        event = bytearray(self.event()); event[-5] ^= 1
        with self.assertRaisesRegex(ValueError, 'CRC32'):
            read_headers(b'\xfebin' + event)

    def test_wrong_positions_rejected_even_with_valid_crc(self):
        with self.assertRaisesRegex(ValueError, 'bounds'):
            read_headers(b'\xfebin' + self.event(start=5))

    def test_negative_context_streams_rewrite_positions_and_crc(self):
        events = []
        start = 4
        for kind in (15, 33, 19, 30, 16):
            event = bytearray(self.event(start=start)); event[4] = kind
            event[-4:] = struct.pack('<I', zlib.crc32(event[:-4]))
            events.append(bytes(event)); start += len(event)
        raw = b'\xfebin' + b''.join(events)
        self.assertEqual(len(split_events(raw)), 5)
        rebuilt = rewrite_events([events[0], events[-1]])
        self.assertEqual(len(read_headers(rebuilt)), 2)
        variants = negative_streams(raw)
        self.assertEqual(set(variants), {'missing-fde', 'missing-table-map', 'missing-gtid',
                                         'unknown-event', 'partial-header', 'partial-body',
                                         'bad-crc', 'bad-size', 'bad-position'})
        for name in ('missing-fde', 'missing-table-map', 'missing-gtid', 'unknown-event'):
            with self.subTest(name=name):
                read_headers(variants[name][0])

    def test_raw_cache_negative_matrix_mutates_every_protocol_boundary(self):
        expected_size = 10
        payload = b"compressed"
        cache = (
            b"RDSRAW1\n"
            + bytes([1])
            + struct.pack("<Q", expected_size)
            + b"a" * 32
            + struct.pack("<I", 64 * 1024)
            + b"FRM1"
            + struct.pack("<II", expected_size, len(payload))
            + b"b" * 32
            + payload
            + b"END1"
            + struct.pack("<Q", expected_size)
            + b"c" * 32
        )
        variants = raw_cache_negative_streams(cache, expected_size)
        self.assertEqual(
            set(variants),
            {
                "truncated-header",
                "bad-magic",
                "unsupported-codec",
                "wrong-identity",
                "wrong-declared-size",
                "wrong-expected-size-argument",
                "bad-frame-marker",
                "bad-frame-sha",
                "bad-footer-marker",
                "truncated-footer",
                "bad-final-sha",
                "trailing-data",
            },
        )
        self.assertTrue(all(marker for _content, _size, marker in variants.values()))
        self.assertEqual(
            variants["wrong-expected-size-argument"][1], expected_size + 1
        )
        self.assertEqual(variants["trailing-data"][0], cache + b"x")
        for name, (content, _size, _marker) in variants.items():
            if name != "wrong-expected-size-argument":
                self.assertNotEqual(content, cache, name)

    def test_negative_chunk_transport_owns_output_directory_creation(self):
        calls = []

        def failed_transport(binary, source, source_id, mode, output_dir,
                             *, chunk_format):
            self.assertFalse(output_dir.exists())
            output_dir.mkdir()
            calls.append((output_dir.name, chunk_format))
            return (
                subprocess.CompletedProcess([], 1, stdout=b'', stderr=b'boom'),
                {
                    'acknowledged_files': 0,
                    'manifests_sha256': 'manifest',
                    'acks_sha256': 'ack',
                },
                b'',
            )

        direct_failure = subprocess.CompletedProcess(
            [], 1, stdout=b'', stderr=b'boom'
        )
        with tempfile.TemporaryDirectory() as temporary:
            with patch(
                'tools.parser_contract_fixture.negative_streams',
                return_value={'bad-stream': (b'raw', 'boom')},
            ), patch(
                'tools.parser_contract_fixture.run_failed_chunk_transport',
                side_effect=failed_transport,
            ), patch(
                'tools.parser_contract_fixture.subprocess.run',
                return_value=direct_failure,
            ):
                proof = verify_negative_streams(
                    Path(temporary), Path('candidate'), b'raw', 'source-id'
                )

        self.assertEqual(
            calls,
            [
                ('bad-stream-output', 'ndjson'),
                ('bad-stream-arrow-chunk-output', 'arrow'),
            ],
        )
        self.assertEqual(set(proof), {'bad-stream'})

    def test_bad_magic_rejected(self):
        with self.assertRaisesRegex(ValueError, 'magic'):
            read_headers(b'not binlog')


if __name__ == '__main__':
    unittest.main()
