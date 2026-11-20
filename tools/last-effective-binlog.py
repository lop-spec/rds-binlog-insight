#!/usr/bin/env python3
"""Offline, non-destructive evaluator for one sealed table/window with verified schema.

Input: normalized JSONL sorted by source position, not wall time. The caller must
separately establish source coverage, a stable schema/PK, transaction order, and
that each image is full. This module checks the supplied invariants and refuses
ambiguous histories. It never connects to a database service or deletes files.
"""
import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path


class Unsafe(ValueError):
    """An input cohort must be retained unchanged, never partially replaced."""


@dataclass(frozen=True)
class Number:
    text: str  # Preserve precision, type and spelling; never compare via float.


def unique_object(items):
    result = {}
    for key, value in items:
        if key in result:
            raise Unsafe('duplicate_json_key')
        result[key] = value
    return result


def not_number(value):
    raise Unsafe('non_finite_number')


def decode(blob):
    return json.loads(blob, object_pairs_hook=unique_object, parse_float=Number,
                      parse_constant=not_number)


def encode(value):
    if isinstance(value, Number):
        return value.text
    if isinstance(value, list):
        return '[' + ','.join(map(encode, value)) + ']'
    if isinstance(value, dict):
        return '{' + ','.join(encode(key) + ':' + encode(val)
                              for key, val in sorted(value.items())) + '}'
    return json.dumps(value, ensure_ascii=True, separators=(',', ':'), allow_nan=False)


def valid_keys(values):
    return (isinstance(values, list) and bool(values)
            and all(isinstance(x, str) and x for x in values)
            and len(set(values)) == len(values))


def validate(window):
    if not isinstance(window, dict):
        raise Unsafe('invalid_window')
    integer_keys = ('start_us', 'end_us', 'expected_events', 'max_input_bytes')
    if any(type(window.get(k)) is not int for k in integer_keys):
        raise Unsafe('invalid_window_bounds')
    if (window['start_us'] >= window['end_us'] or window['start_us'] <= 0
            or window['expected_events'] < 0 or window['max_input_bytes'] <= 0):
        raise Unsafe('invalid_window_bounds')
    for name in ('instance', 'database', 'table', 'stream', 'schema_evidence', 'coverage_evidence'):
        if not isinstance(window.get(name), str) or not window[name].strip():
            raise Unsafe('missing_' + name)
    cols, pk = window.get('columns'), window.get('primary_key')
    if not valid_keys(cols) or not valid_keys(pk) or not set(pk) <= set(cols):
        raise Unsafe('unproven_schema_or_key')


def key_for(image, columns, primary):
    if not isinstance(image, dict) or set(image) != columns:
        raise Unsafe('partial_or_wrong_schema_image')
    result = [image[k] for k in primary]
    for value in result:
        if type(value) not in (int, str) and not (
                isinstance(value, dict) and set(value) == {'$binary_base64'}
                and isinstance(value['$binary_base64'], str)):
            raise Unsafe('unsupported_primary_key_value')
    return result


def fold(window, lines, db, emit):
    """Return counters; call emit only after the whole declared scope passed."""
    validate(window)
    cols, pk = set(window['columns']), window['primary_key']
    db.execute('CREATE TABLE owner(k TEXT PRIMARY KEY, chain INTEGER) WITHOUT ROWID')
    db.execute('CREATE TABLE current(chain INTEGER PRIMARY KEY, initial TEXT NOT NULL, '
               'final TEXT NOT NULL, last TEXT)')
    totals = Counter()
    previous = None
    for raw in lines:
        totals['bytes_in'] += len(raw)
        totals['events'] += 1
        if totals['bytes_in'] > window['max_input_bytes'] or totals['events'] > window['expected_events']:
            raise Unsafe('input_budget_or_count_exceeded')
        e = decode(raw)
        if not isinstance(e, dict):
            raise Unsafe('invalid_event')
        if tuple(e.get(k) for k in ('instance','database','table','stream')) != tuple(
                window[k] for k in ('instance','database','table','stream')):
            raise Unsafe('unexpected_cohort_or_stream')
        when, pos = e.get('event_us'), e.get('source_order')
        if (type(when) is not int or not window['start_us'] <= when < window['end_us']
                or not isinstance(pos, list) or len(pos) != 3
                or any(type(x) is not int or x < 0 for x in pos)):
            raise Unsafe('invalid_event_time_or_source_order')
        if previous is not None and pos <= previous:
            raise Unsafe('duplicate_or_reordered_source_event')
        previous = pos
        if 'before' not in e or 'after' not in e:
            raise Unsafe('missing_images')
        before, after, op = e['before'], e['after'], e.get('op')
        shape = (before is not None, after is not None)
        if {'INSERT': (False, True), 'UPDATE': (True, True), 'DELETE': (True, False)}.get(op) != shape:
            raise Unsafe('non_row_operation_or_invalid_shape')
        old_key = key_for(before, cols, pk) if before is not None else None
        new_key = key_for(after, cols, pk) if after is not None else None
        old, new = (encode(k) if k is not None else None for k in (old_key,new_key))
        b, a = encode(before), encode(after)
        old_slot = db.execute('SELECT chain FROM owner WHERE k=?',(old,)).fetchone() if old is not None else None
        if old_slot is not None and old_slot[0] is None:
            raise Unsafe('write_to_known_empty_key')
        chain = old_slot[0] if old_slot else totals['events']
        saved = db.execute('SELECT final,last FROM current WHERE chain=?',(chain,)).fetchone() if old_slot else None
        if old_slot and (not saved or saved[0] != b):
            raise Unsafe('discontinuous_row_chain')
        if new is not None and old != new:
            occupant = db.execute('SELECT chain FROM owner WHERE k=?',(new,)).fetchone()
            if occupant and occupant[0] is not None:
                raise Unsafe('primary_key_destination_occupied')
            if occupant and op == 'INSERT':
                raise Unsafe('primary_key_reuse_requires_identity_proof')
        last = saved[1] if saved else None
        if b != a:
            record = {'key':new_key if new is not None else old_key, 'op':op, 'event_us':when, 'source_order':pos}
            if op == 'UPDATE':
                changed = [k for k in cols if encode(before[k]) != encode(after[k])]
                record.update(before={k:before[k] for k in changed}, after={k:after[k] for k in changed})
                if old != new:
                    record['old_key'] = old_key
                    totals['key_moves'] += 1
            elif op == 'INSERT':
                record['after'] = after
            last = encode(record)
        else:
            totals['no_change_writes'] += 1
        if old is not None and old != new:
            db.execute('INSERT INTO owner VALUES (?,NULL) ON CONFLICT(k) DO UPDATE SET chain=NULL',(old,))
        if new is not None:
            db.execute('INSERT INTO owner VALUES (?,?) ON CONFLICT(k) DO UPDATE SET chain=excluded.chain',(new,chain))
        if saved:
            db.execute('UPDATE current SET final=?,last=? WHERE chain=?',(a,last,chain))
        else:
            db.execute('INSERT INTO current VALUES (?,?,?,?)',(chain,b,a,last))
    if totals['events'] != window['expected_events']:
        raise Unsafe('input_event_count_mismatch')
    for initial, final, last in db.execute('SELECT initial,final,last FROM current ORDER BY chain'):
        totals['keys'] += 1
        if initial == final:
            totals['returned_to_initial'] += 1
        else:
            if last is None:
                raise AssertionError('changed chain without an operation')
            payload = (last + '\n').encode('ascii')
            emit(payload)
            totals['kept'] += 1
            totals['bytes_out'] += len(payload)
    return dict(totals)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def cli():
    import gzip
    import tempfile
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--window', type=Path, required=True)
    p.add_argument('--input', type=Path, required=True)
    p.add_argument('--output-dir', type=Path, required=True, help='Must not exist')
    p.add_argument('--scratch', type=Path, required=True, help='Existing private workspace')
    ns = p.parse_args()
    try:
        if ns.window.stat().st_size > (1 << 20):
            raise Unsafe('window_manifest_too_large')
        window = decode(ns.window.read_bytes())
        validate(window)
        input_hash = digest(ns.input)
        if not re.fullmatch('[a-f0-9]{64}',str(window.get('input_sha256'))) or input_hash != window['input_sha256']:
            raise Unsafe('input_sha256_mismatch')
        if ns.input.stat().st_size > window['max_input_bytes']:
            raise Unsafe('input_budget_or_count_exceeded')
        ns.output_dir.mkdir(mode=0o700)
        with tempfile.TemporaryDirectory(prefix='last-change-',dir=ns.scratch) as td:
            with closing(sqlite3.connect(Path(td)/'state.db')) as db, ns.input.open('rb') as src:
                db.execute('PRAGMA cache_size=-8192')
                db.execute('PRAGMA temp_store=FILE')
                out = ns.output_dir/'changes.jsonl.gz'
                with out.open('xb') as target:
                    with gzip.GzipFile(filename='', fileobj=target, mode='wb', mtime=0, compresslevel=6) as zipped:
                        summary = fold(window, src, db, zipped.write)
                    target.flush()
                    os.fsync(target.fileno())
        if digest(ns.input) != input_hash:
            raise Unsafe('input_changed_during_evaluation')
        with gzip.open(out,'rb') as checked:
            size = count = 0
            for line in checked:
                size += len(line)
                count += 1
            if (size, count) != (summary.get('bytes_out',0), summary.get('kept',0)):
                raise Unsafe('output_verify_failed')
        receipt = {'format':'last-effective-binlog/v1-offline-only','window':window,
                   'counts':summary,'output_bytes':out.stat().st_size,'output_sha256':digest(out)}
        with (ns.output_dir/'receipt.json').open('x',encoding='ascii') as dst:
            dst.write(encode(receipt)+'\n'); dst.flush(); os.fsync(dst.fileno())
        with (ns.output_dir/'READY.offline-only').open('x',encoding='ascii') as dst:
            dst.write(digest(ns.output_dir/'receipt.json')+'\n'); dst.flush(); os.fsync(dst.fileno())
        print(encode({'status':'offline_verified', 'counts':summary,'output_bytes':out.stat().st_size}))
    except (Unsafe, json.JSONDecodeError, OSError, sqlite3.Error) as exc:
        # Do not echo source payloads, archive paths or exception context.
        print(encode({'status':'blocked','reason':str(exc) if isinstance(exc,Unsafe) else type(exc).__name__}))
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(cli())
