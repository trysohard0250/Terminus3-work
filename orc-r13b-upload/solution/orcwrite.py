#!/usr/bin/env python3
"""Reference solution: archive every event batch as an uncompressed ORC file.

Standard library only. The ORC container is written by hand: protobuf
messages for the postscript, footer and stripe footer; byte run-length
encoding for booleans and validity bits; RLE version 2 DIRECT runs for
integers and string lengths; UTF-8 byte streams for string data.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path

DATA = Path(os.environ.get("APP_DATA", "/app/data/batches"))
OUT = Path(os.environ.get("APP_OUT", "/app/out"))

SCHEMA = [("event_id", "string"), ("feed_id", "string"), ("seq", "long"),
          ("latency_ms", "long"), ("breach", "boolean"), ("note", "string")]
KIND = {"boolean": 0, "long": 4, "string": 7}
S_PRESENT, S_DATA, S_LENGTH = 0, 1, 2
E_DIRECT, E_DIRECT_V2 = 0, 2


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


# ------------------------------------------------------- encodings

def byte_rle(data: bytes) -> bytes:
    out = bytearray()
    i, n = 0, len(data)
    while i < n:
        j = i
        while j + 1 < n and data[j + 1] == data[i] and j + 1 - i < 129:
            j += 1
        run = j - i + 1
        if run >= 3:
            out.append(min(run, 130) - 3)
            out.append(data[i])
            i += min(run, 130)
            continue
        j = i
        while j < n and j - i < 128:
            if j + 2 < n and data[j] == data[j + 1] == data[j + 2]:
                break
            j += 1
        lit = data[i:j]
        out.append(256 - len(lit))
        out.extend(lit)
        i = j
    return bytes(out)


def bits_to_bytes(bits) -> bytes:
    out = bytearray((len(bits) + 7) // 8)
    for i, bit in enumerate(bits):
        if bit:
            out[i // 8] |= 0x80 >> (i % 8)
    return bytes(out)


def bool_rle(bits) -> bytes:
    return byte_rle(bits_to_bytes(bits))


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


def zigzag(v: int) -> int:
    return (v << 1) ^ (v >> 63) if v < 0 else v << 1


def rlev2_direct(values, signed: bool) -> bytes:
    """RLE v2 using DIRECT runs of up to 512 values each."""
    vals = [zigzag(v) for v in values] if signed else list(values)
    out = bytearray()
    for i in range(0, len(vals), 512):
        chunk = vals[i:i + 512]
        width = _fit_width(max(chunk)) if max(chunk, default=0) else 1
        code = _WIDTH_CODES[width]
        length = len(chunk) - 1
        out.append(0x40 | (code << 1) | (length >> 8))
        out.append(length & 0xFF)
        out.extend(_pack_msb(chunk, width))
    return bytes(out)


# ------------------------------------------------------------ file

def column_streams(kind: str, values):
    present = [v is not None for v in values]
    streams = []
    if not all(present):
        streams.append((S_PRESENT, bool_rle(present)))
    nn = [v for v in values if v is not None]
    if kind == "long":
        streams.append((S_DATA, rlev2_direct(nn, signed=True)))
        return streams, E_DIRECT_V2
    if kind == "string":
        blobs = [s.encode("utf-8") for s in nn]
        streams.append((S_DATA, b"".join(blobs)))
        streams.append((S_LENGTH, rlev2_direct([len(b) for b in blobs],
                                               signed=False)))
        return streams, E_DIRECT_V2
    if kind == "boolean":
        streams.append((S_DATA, bool_rle([bool(v) for v in nn])))
        return streams, E_DIRECT
    raise ValueError(kind)


def footer_types() -> bytes:
    root = bytearray(pb_varint(1, 12))                    # STRUCT
    for i in range(1, len(SCHEMA) + 1):
        root += pb_varint(2, i)
    for name, _ in SCHEMA:
        root += pb_bytes(3, name.encode())
    out = pb_bytes(4, bytes(root))
    for _, kind in SCHEMA:
        out += pb_bytes(4, pb_varint(1, KIND[kind]))
    return out


def write_orc(path: Path, records: list) -> None:
    nrows = len(records)
    body = bytearray(b"ORC")

    stripes = b""
    if nrows:
        stream_meta, data = [], bytearray()
        encodings = [E_DIRECT]
        for idx, (name, kind) in enumerate(SCHEMA, start=1):
            streams, enc = column_streams(kind, [r[name] for r in records])
            encodings.append(enc)
            for skind, payload in streams:
                stream_meta.append((skind, idx, len(payload)))
                data.extend(payload)
        offset = len(body)
        body.extend(data)
        sfooter = bytearray()
        for skind, col, length in stream_meta:
            sfooter += pb_bytes(1, pb_varint(1, skind) + pb_varint(2, col)
                                + pb_varint(3, length))
        for enc in encodings:
            sfooter += pb_bytes(2, pb_varint(1, enc))
        sfooter += pb_bytes(3, b"UTC")
        body.extend(sfooter)
        stripes = pb_bytes(3, pb_varint(1, offset) + pb_varint(2, 0)
                           + pb_varint(3, len(data))
                           + pb_varint(4, len(sfooter)) + pb_varint(5, nrows))

    footer = bytearray()
    footer += pb_varint(1, 3)
    footer += pb_varint(2, len(body))
    footer += stripes
    footer += footer_types()
    footer += pb_varint(6, nrows)
    footer += pb_varint(8, 0)                             # no row indexes
    body.extend(footer)

    ps = bytearray()
    ps += pb_varint(1, len(footer))
    ps += pb_varint(2, 0)                                 # compression NONE
    ps += pb_varint(3, 65536)
    ps += key(4, 2) + varint(2) + bytes([0, 12])          # format 0.12
    ps += pb_varint(5, 0)                                 # no metadata section
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
