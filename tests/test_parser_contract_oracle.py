import base64
import json
import tempfile
import unittest
from pathlib import Path

from tools.parser_contract_oracle import check_values, compare_columnar, expected_images, verify


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

    def test_identity_failure_retains_a_failed_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / 'statement').mkdir()
            (root / 'statement/source.binlog').write_bytes(b'corrupt')
            contract = {'source_sha': 'candidate', 'cases': {'STATEMENT':
                        {'raw_sha256': '0' * 64, 'legacy_sha256': '0' * 64}}}
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

    def test_invalid_binary_encoding_and_added_fields_fail_closed(self):
        rows = faithful_rows(); image = json.loads(rows[0]['after_json'])
        image['payload'] = {'$bytes_base64': '%%%%'}; image['unknown'] = 'value'
        rows[0]['after_json'] = json.dumps(image)
        self.assertEqual({f['field'] for f in check_values(rows)},
                         {'after_json.keys', 'after_json.payload'})


if __name__ == '__main__':
    unittest.main()
