"""Offline codec comparison. Input: bounded JSONEachRow via stdin. No row values are printed or persisted."""
import json
import re
import struct
import sys

import pyarrow as pa

MAX_BYTES = 16 << 20
U32 = struct.Struct('<I')
DIGITS = re.compile(rb'[0-9]+')


def strings(items):
    return b''.join(U32.pack(len(s)) + s for s in items)


def patch(before, after):
    n = min(len(before), len(after)); a = z = 0
    while a < n and before[a] == after[a]:
        a += 1
    while z < n - a and before[-1-z] == after[-1-z]:
        z += 1
    return U32.pack(a) + U32.pack(z) + after[a:len(after)-z]


def unpatch(before, delta):
    a, z = struct.unpack_from('<II', delta)
    return before[:a] + delta[8:] + before[len(before)-z:]


def compare(rows):
    codec = pa.Codec('zstd', compression_level=3)
    def stored(b):
        enc = codec.compress(b, asbytes=True)
        if codec.decompress(enc, decompressed_size=len(b), asbytes=True) != b:
            raise ValueError('roundtrip')
        return len(enc)
    stats = {'rows':len(rows)}
    if all('before_json' in row and 'after_json' in row for row in rows):
        bs = [r['before_json'].encode() for r in rows]; xs = [r['after_json'].encode() for r in rows]
        ds = [patch(b, x) for b,x in zip(bs,xs)]
        if any(unpatch(b,d) != x for b,d,x in zip(bs,ds,xs)):
            raise ValueError('image mismatch')
        stats.update(image_base=stored(strings(bs))+stored(strings(xs)), image_delta=stored(strings(bs))+stored(strings(ds)))
    if all('row_query' in row for row in rows):
        ss=[r['row_query'].encode() for r in rows]; dictionary={}; layout=[]; refs=[]; digits=[]
        for s in ss:
            parts=tuple(DIGITS.split(s)); vals=DIGITS.findall(s)
            if parts not in dictionary:
                dictionary[parts]=len(layout); layout.append(parts)
            idx=dictionary[parts]; refs.append(U32.pack(idx)); digits.extend(vals)
        vals=iter(digits)
        for ref,s in zip(refs,ss):
            parts=layout[U32.unpack(ref)[0]]
            if parts[0]+b''.join(next(vals)+part for part in parts[1:]) != s:
                raise ValueError('SQL literal mismatch')
        templates=strings([strings(parts) for parts in layout]); stats.update(sql_base=stored(strings(ss)),sql_template=stored(templates)+stored(b''.join(refs))+stored(strings(digits)))
    if len(stats)==1: raise ValueError('no matching columns')
    return stats


def main():
    rows=[]; n=0
    while line := sys.stdin.buffer.readline(MAX_BYTES+1):
        n+=len(line)
        if n>MAX_BYTES or len(rows)>=16384: raise SystemExit('sample exceeds 16 MiB or 16384 rows')
        rows.append(json.loads(line))
    if not rows: raise SystemExit('empty sample')
    print(json.dumps(compare(rows)))


if __name__=='__main__': main()
