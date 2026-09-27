#!/usr/bin/env python3
"""Reference solution: archive every event batch as a ZLIB-compressed,
row-indexed ORC file carrying column statistics.

Standard library only. The ORC container is written by hand: protobuf
messages for the postscript, footer, metadata, stripe footer and row
index; every stream and every metadata message except the postscript
written as a sequence of raw-DEFLATE chunks; byte run-length encoding
for booleans and validity bits; RLE version 2 DIRECT runs for integers
and string lengths; UTF-8 byte streams for string data; one row-index
entry per 900-row group with stream positions and per-group statistics;
statistics again for the stripe (metadata section) and for the file;
and a bloom filter per row group for every string and bigint column
(ORC's Murmur3 64-bit variant for strings, its 64-bit mix for
integers, 5632 bits and 4 hash functions).
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import zlib
from pathlib import Path

DATA = Path(os.environ.get("APP_DATA", "/app/data/batches"))
OUT = Path(os.environ.get("APP_OUT", "/app/out"))

SCHEMA = [("event_id", "string"), ("feed_id", "string"), ("seq", "long"),
          ("latency_ms", "long"), ("breach", "boolean"), ("note", "string")]
KIND = {"boolean": 0, "long": 4, "string": 7}
S_PRESENT, S_DATA, S_LENGTH, S_ROW_INDEX, S_BLOOM = 0, 1, 2, 6, 8
E_DIRECT, E_DIRECT_V2 = 0, 2
C_ZLIB = 1
STRIDE = 900                 # rows per row group (CONTRACT.md 3)
BLOCK = 65536                # compression block size (CONTRACT.md 3)
BLOOM_COLUMNS = {1, 2, 3, 4, 6}   # the string and bigint columns
BLOOM_BITS, BLOOM_HASHES = 5632, 4  # 900 entries at fpp 0.05 (CONTRACT.md 3)


# ------------------------------------------------------- bloom filters

_MASK64 = (1 << 64) - 1
_C1, _C2 = 0x87c37b91114253d5, 0x4cf5ad432745937f


def _rotl64(x: int, r: int) -> int:
    return ((x << r) | (x >> (64 - r))) & _MASK64


def _fmix64(k: int) -> int:
    k ^= k >> 33
    k = (k * 0xff51afd7ed558ccd) & _MASK64
    k ^= k >> 33
    k = (k * 0xc4ceb9fe1a85ec53) & _MASK64
    k ^= k >> 33
    return k


def murmur3_hash64(data: bytes, seed: int = 104729) -> int:
    """ORC's Murmur3 64-bit variant: 8-byte little-endian blocks through
    a single lane, the tail folded byte by byte, the length, fmix64."""
    h = seed
    nblocks = len(data) >> 3
    for i in range(nblocks):
        k = int.from_bytes(data[i * 8:i * 8 + 8], "little")
        k = _rotl64((k * _C1) & _MASK64, 31)
        h ^= (k * _C2) & _MASK64
        h = (_rotl64(h, 27) * 5 + 0x52dce729) & _MASK64
    tail = data[nblocks * 8:]
    if tail:
        k = int.from_bytes(tail, "little")
        k = _rotl64((k * _C1) & _MASK64, 31)
        h ^= (k * _C2) & _MASK64
    return _fmix64(h ^ len(data))


def _sra64(x: int, s: int) -> int:
    """Arithmetic right shift of a 64-bit two's complement value."""
    return ((x - (1 << 64)) >> s) & _MASK64 if x >> 63 else x >> s


def long_hash(key: int) -> int:
    """ORC's 64-bit integer mix for bloom filters (Java long arithmetic:
    wrapping adds and shifts, sign-propagating right shifts)."""
    key &= _MASK64
    key = ((~key & _MASK64) + (key << 21)) & _MASK64
    key ^= _sra64(key, 24)
    key = (key + (key << 3) + (key << 8)) & _MASK64
    key ^= _sra64(key, 14)
    key = (key + (key << 2) + (key << 4)) & _MASK64
    key ^= _sra64(key, 28)
    return (key + (key << 31)) & _MASK64


def _int32(x: int) -> int:
    x &= 0xFFFFFFFF
    return x - (1 << 32) if x & 0x80000000 else x


class Bloom:
    """One row group's bloom filter: BLOOM_BITS bits as little-endian
    64-bit words, BLOOM_HASHES positions per value."""

    def __init__(self):
        self.bits = bytearray(BLOOM_BITS // 8)

    def add(self, v) -> None:
        h = murmur3_hash64(v.encode("utf-8")) if isinstance(v, str) \
            else long_hash(v)
        hash1, hash2 = _int32(h), _int32(h >> 32)
        for i in range(1, BLOOM_HASHES + 1):
            combined = _int32(hash1 + i * hash2)
            if combined < 0:
                combined = ~combined
            pos = combined % BLOOM_BITS
            self.bits[pos >> 3] |= 1 << (pos & 7)


# ------------------------------------------------------------ protobuf

def varint(v: int) -> bytes:
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def key(field: int, wt: int) -> bytes:
    return varint((field << 3) | wt)


def pb_varint(field: int, v: int) -> bytes:
    return key(field, 0) + varint(v)


def pb_bytes(field: int, data: bytes) -> bytes:
    return key(field, 2) + varint(len(data)) + data


def zigzag(v: int) -> int:
    return (v << 1) ^ (v >> 63) if v < 0 else v << 1


def pb_sint(field: int, v: int) -> bytes:
    return key(field, 0) + varint(zigzag(v))


def pb_packed(field: int, values) -> bytes:
    return pb_bytes(field, b"".join(varint(v) for v in values))


# --------------------------------------------------------- compression

class OutStream:
    """A compressed stream: raw-DEFLATE chunks, each holding at most
    BLOCK bytes of content, behind the three-byte ORC chunk header.
    Bytes are buffered until a block is full and spilled lazily, so a
    position recorded at a row-group boundary names the chunk that
    will hold the boundary (the way the reference Java writer does)."""

    def __init__(self):
        self.done = bytearray()      # completed chunks
        self.buf = bytearray()       # content of the chunk being filled

    def write(self, data: bytes) -> None:
        i, n = 0, len(data)
        while i < n:
            if len(self.buf) == BLOCK:
                self._spill()
            take = min(BLOCK - len(self.buf), n - i)
            self.buf += data[i:i + take]
            i += take

    def _spill(self) -> None:
        if not self.buf:
            return
        c = zlib.compressobj(6, zlib.DEFLATED, -15)      # raw DEFLATE
        comp = c.compress(bytes(self.buf)) + c.flush()
        head = len(comp) << 1                            # original bit 0
        self.done += bytes([head & 0xFF, (head >> 8) & 0xFF, head >> 16])
        self.done += comp
        self.buf = bytearray()

    def position(self) -> list:
        """[compressed offset of the current chunk, bytes into it]."""
        return [len(self.done), len(self.buf)]

    def finish(self) -> bytes:
        self._spill()
        return bytes(self.done)


def compressed(message: bytes) -> bytes:
    s = OutStream()
    s.write(message)
    return s.finish()


# ------------------------------------------------------------ encoders

class ByteRle:
    """Byte run-length encoding: repeat runs of 3..130 bytes, literal
    groups of 1..128 bytes (a port of the reference writer's buffering,
    so positions carry a pending-value count of at most 129)."""

    def __init__(self, out: OutStream):
        self.out = out
        self.lits = bytearray()
        self.repeat = False
        self.tail = 0

    def _flush(self) -> None:
        if self.repeat:
            self.out.write(bytes([len(self.lits) - 3, self.lits[0]]))
        else:
            self.out.write(bytes([256 - len(self.lits)]) + bytes(self.lits))
        self.lits = bytearray()
        self.repeat = False
        self.tail = 0

    def write(self, b: int) -> None:
        if not self.lits:
            self.lits.append(b)
            self.tail = 1
        elif self.repeat:
            if b == self.lits[0]:
                self.lits.append(b)
                if len(self.lits) == 130:
                    self._flush()
            else:
                self._flush()
                self.lits.append(b)
                self.tail = 1
        else:
            self.tail = self.tail + 1 if b == self.lits[-1] else 1
            if self.tail == 3:
                if len(self.lits) + 1 == 3:
                    self.repeat = True
                    self.lits.append(b)
                else:
                    del self.lits[-2:]
                    self._flush()
                    self.lits = bytearray([b, b, b])
                    self.repeat = True
            else:
                self.lits.append(b)
                if len(self.lits) == 128:
                    self._flush()

    def position(self) -> list:
        return self.out.position() + [len(self.lits)]

    def finish(self) -> None:
        if self.lits:
            self._flush()


class BitField:
    """Booleans packed eight per byte, most significant bit first, over
    byte run-length encoding."""

    def __init__(self, out: OutStream):
        self.rle = ByteRle(out)
        self.cur = 0
        self.nbits = 0

    def write(self, bit: bool) -> None:
        self.cur = (self.cur << 1) | (1 if bit else 0)
        self.nbits += 1
        if self.nbits == 8:
            self.rle.write(self.cur)
            self.cur = 0
            self.nbits = 0

    def position(self) -> list:
        return self.rle.position() + [self.nbits]

    def finish(self) -> None:
        if self.nbits:
            self.rle.write(self.cur << (8 - self.nbits))
            self.cur = self.nbits = 0
        self.rle.finish()


_WIDTHS = list(range(1, 25)) + [26, 28, 30, 32, 40, 48, 56, 64]
_WIDTH_CODES = {w: c for c, w in enumerate(_WIDTHS)}


def _fit_width(maxval: int) -> int:
    for w in _WIDTHS:
        if maxval < (1 << w):
            return w
    raise ValueError("value too wide")


def _pack_msb(values, width: int) -> bytes:
    out = bytearray()
    acc, nbits = 0, 0
    for v in values:
        acc = (acc << width) | v
        nbits += width
        while nbits >= 8:
            nbits -= 8
            out.append((acc >> nbits) & 0xFF)
    if nbits:
        out.append((acc << (8 - nbits)) & 0xFF)
    return bytes(out)


class IntRleV2:
    """RLE version 2 using DIRECT runs of up to 512 values each."""

    def __init__(self, out: OutStream, signed: bool):
        self.out = out
        self.signed = signed
        self.buf = []

    def write(self, v: int) -> None:
        self.buf.append(zigzag(v) if self.signed else v)
        if len(self.buf) == 512:
            self._flush()

    def _flush(self) -> None:
        chunk = self.buf
        width = _fit_width(max(chunk)) if max(chunk) else 1
        code = _WIDTH_CODES[width]
        length = len(chunk) - 1
        self.out.write(bytes([0x40 | (code << 1) | (length >> 8),
                              length & 0xFF]) + _pack_msb(chunk, width))
        self.buf = []

    def position(self) -> list:
        return self.out.position() + [len(self.buf)]

    def finish(self) -> None:
        if self.buf:
            self._flush()


# ---------------------------------------------------------- statistics

class Stats:
    """Column statistics over a set of rows (a row group, the stripe or
    the file): number of non-null values, whether a null was seen, and
    the type's own figures (CONTRACT.md 3)."""

    def __init__(self, kind: str):
        self.kind = kind
        self.count = 0
        self.has_null = False
        self.lo = self.hi = None
        self.sum = 0                 # integers: value sum; strings: bytes
        self.trues = 0

    def add(self, v) -> None:
        if v is None:
            self.has_null = True
            return
        self.count += 1
        if self.kind == "long":
            self.lo = v if self.lo is None else min(self.lo, v)
            self.hi = v if self.hi is None else max(self.hi, v)
            self.sum += v
        elif self.kind == "string":
            self.lo = v if self.lo is None else min(self.lo, v)
            self.hi = v if self.hi is None else max(self.hi, v)
            self.sum += len(v.encode("utf-8"))
        elif self.kind == "boolean":
            self.trues += 1 if v else 0

    def to_pb(self) -> bytes:
        out = pb_varint(1, self.count)
        if self.kind == "long":
            sub = b""
            if self.count:
                sub += pb_sint(1, self.lo) + pb_sint(2, self.hi)
            if -2**63 <= self.sum < 2**63:
                sub += pb_sint(3, self.sum)
            out += pb_bytes(2, sub)
        elif self.kind == "string":
            sub = b""
            if self.count:
                sub += pb_bytes(1, self.lo.encode("utf-8"))
                sub += pb_bytes(2, self.hi.encode("utf-8"))
            sub += pb_sint(3, self.sum)
            out += pb_bytes(4, sub)
        elif self.kind == "boolean":
            out += pb_bytes(5, pb_packed(1, [self.trues]))
        out += pb_varint(10, 1 if self.has_null else 0)
        return out


# ------------------------------------------------------------- columns

class Column:
    """One data column: its streams, encoders and row-index positions."""

    def __init__(self, kind: str, values):
        self.kind = kind
        self.has_null = any(v is None for v in values)
        self.present = BitField(OutStream()) if self.has_null else None
        if kind == "string":
            self.blob = OutStream()
            self.length = IntRleV2(OutStream(), signed=False)
        elif kind == "long":
            self.data = IntRleV2(OutStream(), signed=True)
        else:
            self.data = BitField(OutStream())

    def positions(self) -> list:
        pos = self.present.position() if self.present else []
        if self.kind == "string":
            return pos + self.blob.position() + self.length.position()
        return pos + self.data.position()

    def write(self, v) -> None:
        if self.present:
            self.present.write(v is not None)
        if v is None:
            return
        if self.kind == "string":
            b = v.encode("utf-8")
            self.blob.write(b)
            self.length.write(len(b))
        elif self.kind == "long":
            self.data.write(v)
        else:
            self.data.write(bool(v))

    def finish(self) -> list:
        """[(stream kind, compressed bytes)] in PRESENT, DATA, LENGTH order."""
        streams = []
        if self.present:
            self.present.finish()
            streams.append((S_PRESENT, self.present.rle.out.finish()))
        if self.kind == "string":
            self.length.finish()
            streams.append((S_DATA, self.blob.finish()))
            streams.append((S_LENGTH, self.length.out.finish()))
        else:
            self.data.finish()
            out = self.data.rle.out if self.kind == "boolean" else self.data.out
            streams.append((S_DATA, out.finish()))
        return streams


def footer_types() -> bytes:
    root = bytearray(pb_varint(1, 12))                    # STRUCT
    root += pb_packed(2, range(1, len(SCHEMA) + 1))
    for name, _ in SCHEMA:
        root += pb_bytes(3, name.encode())
    out = pb_bytes(4, bytes(root))
    for _, kind in SCHEMA:
        out += pb_bytes(4, pb_varint(1, KIND[kind]))
    return out


def write_orc(path: Path, records: list) -> None:
    nrows = len(records)
    kinds = ["struct"] + [k for _, k in SCHEMA]
    file_stats = [Stats(k) for k in kinds]
    body = bytearray(b"ORC")
    stripes = b""
    stripe_stats = b""

    if nrows:
        cols = [Column(kind, [r[name] for r in records])
                for name, kind in SCHEMA]
        index = [[] for _ in kinds]          # per column: [(positions, Stats)]
        blooms = [[] for _ in kinds]         # per bloom column: [Bloom]
        for i, rec in enumerate(records):
            if i % STRIDE == 0:
                for c, k in enumerate(kinds):
                    pos = [] if c == 0 else cols[c - 1].positions()
                    index[c].append((pos, Stats(k)))
                    if c in BLOOM_COLUMNS:
                        blooms[c].append(Bloom())
            file_stats[0].count += 1
            index[0][-1][1].count += 1
            for c, (name, _) in enumerate(SCHEMA, start=1):
                v = rec[name]
                cols[c - 1].write(v)
                index[c][-1][1].add(v)
                file_stats[c].add(v)
                if v is not None and c in BLOOM_COLUMNS:
                    blooms[c][-1].add(v)

        stream_meta, data = [], bytearray()
        for c in range(len(kinds)):
            entries = b"".join(
                pb_bytes(1, pb_packed(1, pos) + pb_bytes(2, st.to_pb()))
                for pos, st in index[c])
            payload = compressed(entries)
            stream_meta.append((S_ROW_INDEX, c, len(payload)))
            data.extend(payload)
            if c in BLOOM_COLUMNS:
                filters = b"".join(
                    pb_bytes(1, pb_varint(1, BLOOM_HASHES)
                             + pb_bytes(3, bytes(b.bits)))
                    for b in blooms[c])
                payload = compressed(filters)
                stream_meta.append((S_BLOOM, c, len(payload)))
                data.extend(payload)
        index_len = len(data)
        for c, col in enumerate(cols, start=1):
            for skind, payload in col.finish():
                stream_meta.append((skind, c, len(payload)))
                data.extend(payload)
        data_len = len(data) - index_len
        body.extend(data)

        sfooter = bytearray()
        for skind, col, length in stream_meta:
            sfooter += pb_bytes(1, pb_varint(1, skind) + pb_varint(2, col)
                                + pb_varint(3, length))
        for c, kind in enumerate(kinds):
            enc = E_DIRECT if kind in ("struct", "boolean") else E_DIRECT_V2
            sfooter += pb_bytes(2, pb_varint(1, enc) + (
                pb_varint(3, 1) if c in BLOOM_COLUMNS else b""))  # UTF8 bloom
        sfooter += pb_bytes(3, b"UTC")
        sfooter = compressed(bytes(sfooter))
        body.extend(sfooter)
        stripes = pb_bytes(3, pb_varint(1, 3) + pb_varint(2, index_len)
                           + pb_varint(3, data_len)
                           + pb_varint(4, len(sfooter)) + pb_varint(5, nrows))
        stripe_stats = pb_bytes(1, b"".join(pb_bytes(1, st.to_pb())
                                            for st in file_stats))

    content_length = len(body)
    metadata = compressed(stripe_stats) if stripe_stats else b""
    body.extend(metadata)

    footer = bytearray()
    footer += pb_varint(1, 3)
    footer += pb_varint(2, content_length)
    footer += stripes
    footer += footer_types()
    footer += pb_varint(6, nrows)
    for st in file_stats:
        footer += pb_bytes(7, st.to_pb())
    footer += pb_varint(8, STRIDE)
    footer = compressed(bytes(footer))
    body.extend(footer)

    ps = bytearray()
    ps += pb_varint(1, len(footer))
    ps += pb_varint(2, C_ZLIB)
    ps += pb_varint(3, BLOCK)
    ps += pb_packed(4, [0, 12])                           # format 0.12
    ps += pb_varint(5, len(metadata))
    ps += pb_varint(6, 6)
    ps += pb_bytes(8000, b"ORC")
    body.extend(ps)
    body.append(len(ps))
    path.write_bytes(bytes(body))


def load_batch(path: Path) -> list:
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def main() -> int:
    # With two arguments the writer is a general converter (this is the
    # /app/convert.py interface); with none it archives the day's batches.
    if len(sys.argv) == 3:
        data, out = Path(sys.argv[1]), Path(sys.argv[2])
    elif len(sys.argv) == 1:
        data, out = DATA, OUT
    else:
        print("usage: convert.py [IN_DIR OUT_DIR]", file=sys.stderr)
        return 2
    out.mkdir(parents=True, exist_ok=True)
    # The output directory must end up holding exactly one .orc per input
    # batch and nothing else, so clear anything already there.
    for stale in out.iterdir():
        if stale.is_dir() and not stale.is_symlink():
            shutil.rmtree(stale)
        else:
            stale.unlink()
    # Zero batches is a valid day: succeed with an empty output directory.
    batches = sorted(data.glob("*.jsonl"))
    for batch in batches:
        records = load_batch(batch)
        write_orc(out / (batch.stem + ".orc"), records)
        print(f"{batch.stem}: {len(records)} rows")
    print(f"converted {len(batches)} batches")
    return 0


if __name__ == "__main__":
    sys.exit(main())
