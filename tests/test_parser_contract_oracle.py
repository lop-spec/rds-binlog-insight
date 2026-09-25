import base64
import hashlib
import json
import struct
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa

from app.columnar_input import parser_schema
from app.storage import PARSER_JSON_COLUMNS
from tools.parser_contract_fixture import raw_cache_negative_streams
from tools.parser_contract_oracle import (
    check_values,
    compare_chunked_arrow,
    compare_columnar,
    compare_decoders,
    expected_images,
    verify,
    verify_negative_raw_cache,
)


def faithful_rows():
    def encode(image):
        if not image:
            return ''
        image = dict(image)
        image['payload'] = {'$bytes_base64': base64.b64encode(image['payload']).decode('ascii')}
        image['document'] = json.dumps(image['document'], ensure_ascii=False)
        return json.dumps(image, ensure_ascii=False)
    return [dict(operation=operation, database_name='fixture', table_name='rows_abi',
                 before_json=encode(before), after_json=encode(after))
            for operation, before, after in expected_images()]


class ParserContractOracleTests(unittest.TestCase):
    def test_negative_raw_cache_artifacts_are_independently_reconstructed(self):
        expected_size = 10
        payload = b"compressed"
        valid = (
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
        empty_sha = hashlib.sha256(b"").hexdigest()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cache_dir = root / "row" / "cache-mechanisms"
            cache_dir.mkdir(parents=True)
            (cache_dir / "zstd.cache").write_bytes(valid)
            negative = root / "negative" / "raw-cache"
            negative.mkdir(parents=True)
            contract = {}
            for name, (content, supplied_size, marker) in (
                raw_cache_negative_streams(valid, expected_size).items()
            ):
                (negative / f"{name}.cache").write_bytes(content)
                stderr = marker.encode("utf-8")
                for transport in ("ndjson", "arrow", "arrow-chunk"):
                    (negative / f"{name}.{transport}.stderr").write_bytes(stderr)
                (negative / f"{name}.ndjson.stdout").write_bytes(b"")
                (negative / f"{name}.arrow.stdout").write_bytes(b"")
                (negative / f"{name}.arrow-chunk.manifests.ndjson").write_bytes(b"")
                (negative / f"{name}.arrow-chunk.acks.ndjson").write_bytes(b"")
                contract[name] = {
                    "cache_bytes": len(content),
                    "cache_sha256": hashlib.sha256(content).hexdigest(),
                    "expected_size_argument": supplied_size,
                    "stderr_marker": marker,
                    "ndjson_returncode": 1,
                    "ndjson_stderr_sha256": hashlib.sha256(stderr).hexdigest(),
                    "arrow_returncode": 1,
                    "arrow_stderr_sha256": hashlib.sha256(stderr).hexdigest(),
                    "arrow_chunk_returncode": 1,
                    "arrow_chunk_stderr_sha256": hashlib.sha256(stderr).hexdigest(),
                    "arrow_chunk_manifests_sha256": empty_sha,
                    "arrow_chunk_acks_sha256": empty_sha,
                    "arrow_chunk_acknowledged_files": 0,
                    "published_files": 0,
                }
            case = {
                "raw_bytes": expected_size,
                "negative_raw_cache": contract,
            }
            self.assertEqual(
                verify_negative_raw_cache(root, case),
                {"cases": 12, "all_failed_before_publication": True},
            )
            (negative / "bad-magic.arrow.stdout").write_bytes(b"leak")
            with self.assertRaisesRegex(ValueError, "leaked output"):
                verify_negative_raw_cache(root, case)

    def test_real_storage_contract_uses_keyword_paths_and_compares_all_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); source = root / 'legacy.ndjson'
            rows = faithful_rows()
            for index, row in enumerate(rows):
                row.update(event_id=f'event-{index}', event_epoch_us=1789906364540116,
                           start_position=100 + index * 10, end_position=110 + index * 10)
            source.write_text('\n'.join(json.dumps(row) for row in rows), encoding='utf-8')
            proof = compare_columnar(source, root / 'compare', 'f' * 64)
            self.assertEqual(proof['rows'], 4)
            self.assertEqual(proof['fields'], 47)
            self.assertTrue(proof['transport_equal'])
            self.assertFalse(proof['native_arrow_producer'])
            self.assertEqual(proof['transport_fields'], 41)
            native = compare_columnar(source, root / 'compare-native', 'f' * 64,
                                      native_arrow=root / 'compare/input.arrow')
            self.assertTrue(native['transport_equal'])
            self.assertTrue(native['native_arrow_producer'])
            self.assertEqual(native['arrow_record_batches'], 2)
            changed = root / 'changed.ndjson'
            changed_rows = json.loads(json.dumps(rows))
            changed_rows[0]['operation'] = 'UPDATE'
            changed.write_text('\n'.join(json.dumps(row) for row in changed_rows), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'native Arrow values'):
                compare_columnar(changed, root / 'compare-mismatch', 'f' * 64,
                                 native_arrow=root / 'compare/input.arrow')

    def test_retained_arrow_chunk_transcript_replays_all_41_and_47_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_id = "f" * 64
            rows = faithful_rows()
            for index, row in enumerate(rows):
                row.update(
                    event_id=f"event-{index}",
                    event_epoch_us=1789906364540116,
                    start_position=100 + index * 10,
                    end_position=110 + index * 10,
                )
            ndjson = root / "candidate.ndjson"
            ndjson.write_text(
                "\n".join(json.dumps(row) for row in rows), encoding="utf-8"
            )
            table = pa.Table.from_pylist(
                rows, schema=parser_schema(PARSER_JSON_COLUMNS)
            )
            chunk_dir = root / "candidate-arrow-chunks"
            chunk_dir.mkdir()
            chunks = []
            manifests = bytearray()
            acknowledgements = bytearray()
            for sequence, batch in enumerate((table.slice(0, 3), table.slice(3, 1))):
                name = f"{source_id}-{sequence:06d}.arrow"
                path = chunk_dir / name
                with pa.OSFile(str(path), "wb") as sink:
                    with pa.ipc.new_file(sink, table.schema) as writer:
                        writer.write_table(batch)
                size = path.stat().st_size
                retained = {
                    "sequence": sequence,
                    "name": name,
                    "rows": batch.num_rows,
                    "bytes": size,
                    "decoded_bytes": 100 + sequence,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
                chunks.append(retained)
                manifests.extend((json.dumps({
                    "protocol": "parser-chunk-v1",
                    "format": "arrow-ipc-file-v1",
                    "sequence": sequence,
                    "path": str(path.resolve()),
                    "rows": batch.num_rows,
                    "bytes": size,
                    "decoded_bytes": 100 + sequence,
                }, separators=(",", ":")) + "\n").encode())
                acknowledgements.extend((json.dumps({
                    "protocol": "parser-chunk-ack-v1",
                    "sequence": sequence,
                    "status": "ok",
                }, separators=(",", ":")) + "\n").encode())
            (root / "candidate-arrow-chunks.manifests.ndjson").write_bytes(manifests)
            (root / "candidate-arrow-chunks.acks.ndjson").write_bytes(
                acknowledgements
            )
            stderr = b"parsed 4 audit records\n"
            (root / "candidate-arrow-chunks.stderr").write_bytes(stderr)
            contract = {
                "protocol": "parser-chunk-v1",
                "format": "arrow-ipc-file-v1",
                "ack_protocol": "parser-chunk-ack-v1",
                "manifests_sha256": hashlib.sha256(manifests).hexdigest(),
                "acks_sha256": hashlib.sha256(acknowledgements).hexdigest(),
                "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
                "chunks": chunks,
            }
            proof = compare_chunked_arrow(
                ndjson, root, root / "chunk-compare", source_id, contract
            )
            self.assertEqual(proof["chunks"], 2)
            self.assertEqual(proof["rows"], 4)
            self.assertEqual(proof["transport_fields"], 41)
            self.assertEqual(proof["fields"], 47)
            self.assertTrue(proof["materialization_equal"])

    def test_identity_failure_retains_a_failed_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / 'statement').mkdir()
            (root / 'statement/source.binlog').write_bytes(b'corrupt')
            contract = {'source_sha': 'candidate', 'cases': {'STATEMENT':
                        {'raw_sha256': '0' * 64, 'legacy_sha256': '0' * 64,
                         'candidate_sha256': '0' * 64}}}
            (root / 'contract.json').write_text(json.dumps(contract), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'identity'):
                verify(root)
            report = json.loads((root / 'independent-oracle.json').read_text(encoding='utf-8'))
            self.assertFalse(report['independent_values_equal'])
            self.assertEqual(report['execution_failure']['type'], 'ValueError')
            with self.assertRaises(FileExistsError):
                verify(root)

    def test_exact_bytes_and_full_known_images_pass(self):
        self.assertEqual(check_values(faithful_rows()), [])
        rows = faithful_rows()
        for row in rows:
            for field in ('before_json', 'after_json'):
                if not row[field]:
                    continue
                image = json.loads(row[field])
                raw = base64.b64decode(image['payload'].pop('$bytes_base64'))
                image['payload'] = {'$binary_base64': base64.b64encode(raw).decode('ascii'),
                                    '$length': len(raw)}
                row[field] = json.dumps(image)
        self.assertEqual(check_values(rows), [])

    def test_recovered_binary_representation_requires_exact_length(self):
        rows = faithful_rows(); image = json.loads(rows[0]['after_json'])
        encoded = image['payload'].pop('$bytes_base64')
        image['payload'] = {'$binary_base64': encoded, '$length': 99}
        rows[0]['after_json'] = json.dumps(image)
        self.assertEqual(check_values(rows)[0]['field'], 'after_json.payload')

    def test_legacy_binary_replacement_is_rejected_in_all_three_images(self):
        rows = faithful_rows()
        for row in rows:
            for field in ('before_json', 'after_json'):
                if not row[field]:
                    continue
                image = json.loads(row[field])
                raw = base64.b64decode(image['payload']['$bytes_base64'])
                image['payload'] = raw.decode('utf-8', 'replace')
                row[field] = json.dumps(image)
        failures = check_values(rows)
        self.assertEqual(len(failures), 3)
        self.assertTrue(all(f['field'].endswith('.payload') for f in failures))
        self.assertTrue(all(f['expected'] == '00fffe' for f in failures))
        self.assertTrue(all(f['actual'] == '00efbfbdefbfbd' for f in failures))

    def test_missing_mutation_never_passes(self):
        self.assertEqual(check_values(faithful_rows()[:-1])[0]['field'], 'mutation_count')

    def test_unsigned_integer_is_not_coerced_to_float(self):
        rows = faithful_rows(); image = json.loads(rows[0]['after_json'])
        image['id'] = float(image['id']); rows[0]['after_json'] = json.dumps(image)
        self.assertEqual(check_values(rows)[0]['field'], 'after_json.id')

    def test_json_boolean_and_number_are_distinct(self):
        rows = faithful_rows(); image = json.loads(rows[1]['after_json'])
        image['document'] = '[null,1,0]'; rows[1]['after_json'] = json.dumps(image)
        self.assertEqual(check_values(rows)[0]['field'], 'after_json.document')

    def test_full_decoder_differential_allows_only_binary_body_repairs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); candidate = faithful_rows()
            legacy = json.loads(json.dumps(candidate))
            for index, (new, old) in enumerate(zip(candidate, legacy)):
                new.update(event_id=f'event-{index}', event_epoch_us=1789906364540116,
                           start_position=100 + index * 10, end_position=110 + index * 10,
                           sql_text="SELECT FROM_BASE64('AP/+')")
                old.update(event_id=new['event_id'], event_epoch_us=new['event_epoch_us'],
                           start_position=new['start_position'], end_position=new['end_position'],
                           sql_text='legacy lossy pseudo SQL')
                for field in ('before_json', 'after_json'):
                    if not old[field]:
                        continue
                    image = json.loads(old[field])
                    raw = base64.b64decode(image['payload']['$bytes_base64'])
                    image['payload'] = raw.decode('utf-8', 'replace')
                    old[field] = json.dumps(image)
            legacy_path = root / 'legacy.ndjson'; candidate_path = root / 'candidate.ndjson'
            legacy_path.write_text('\n'.join(json.dumps(row) for row in legacy), encoding='utf-8')
            candidate_path.write_text('\n'.join(json.dumps(row) for row in candidate), encoding='utf-8')
            proof = compare_decoders(legacy_path, candidate_path, root / 'compare', 'f' * 64,
                                     require_binary_repair=True)
            self.assertEqual(proof['fields'], 47)
            self.assertTrue(proof['identity_fields_equal'])
            self.assertEqual(proof['differing_fields'], ['after_json', 'before_json', 'sql_text'])

    def test_invalid_binary_encoding_and_added_fields_fail_closed(self):
        rows = faithful_rows(); image = json.loads(rows[0]['after_json'])
        image['payload'] = {'$bytes_base64': '%%%%'}; image['unknown'] = 'value'
        rows[0]['after_json'] = json.dumps(image)
        self.assertEqual({f['field'] for f in check_values(rows)},
                         {'after_json.keys', 'after_json.payload'})


if __name__ == '__main__':
    unittest.main()
