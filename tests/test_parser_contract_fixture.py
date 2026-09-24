import struct
import unittest
import zlib
from unittest.mock import patch

from tools.parser_contract_fixture import (
    mysql_query,
    negative_streams,
    read_headers,
    rewrite_events,
    split_events,
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

    def test_bad_magic_rejected(self):
        with self.assertRaisesRegex(ValueError, 'magic'):
            read_headers(b'not binlog')


if __name__ == '__main__':
    unittest.main()
