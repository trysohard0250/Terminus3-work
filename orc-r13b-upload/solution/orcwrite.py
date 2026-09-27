#!/usr/bin/env python3
"""Reference solution: archive every event batch as a ZLIB-compressed,
row-indexed ORC file carrying column statistics.

Standard library only. The ORC container is written by hand: protobuf
messages for the postscript, footer, metadata, stripe footer and row
index; every stream and every metadata message except the postscript
written as a sequence of raw-DEFLATE chunks; byte run-length encoding
for booleans and validity bits; RLE version 2 runs for integers
and string lengths as the Apache ORC Java writer chooses them (a port
of its RunLengthIntegerWriterV2); UTF-8 byte streams for string data; one row-index
entry per 900-row group with stream positions and per-group statistics;
statistics again for the stripe (metadata section) and for the file;
and a bloom filter per row group for every string and bigint column
(ORC's Murmur3 64-bit variant for strings, its 64-bit mix for
integers, 5632 bits and 4 hash functions).
"""
from __future__ import annotations

import json
import math
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


# ------------------------------------------- RLE v2, the Java writer's
# A port of org.apache.orc.impl.RunLengthIntegerWriterV2 (Apache ORC
# 2.1.3) and the SerializationUtils helpers it uses: the archive's
# integer and length streams must be byte-identical to what that
# writer produces (CONTRACT.md 3), run selection, run boundaries, bit
# widths, patch lists and positions included. Validated byte for byte
# against streams written by orc-tools 2.1.3.

MAX_SCOPE = 512
MIN_REPEAT = 3
MAX_SHORT_REPEAT_LENGTH = 10
SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA = 0, 1, 2, 3
M64 = (1 << 64) - 1


def to_s64(x):
    x &= M64
    return x - (1 << 64) if x >> 63 else x


def u64(x):
    return x & M64


# --------------------------------------------------- SerializationUtils

def get_closest_fixed_bits(n):
    if n == 0:
        return 1
    if 1 <= n <= 24:
        return n
    if n <= 26:
        return 26
    if n <= 28:
        return 28
    if n <= 30:
        return 30
    if n <= 32:
        return 32
    if n <= 40:
        return 40
    if n <= 48:
        return 48
    if n <= 56:
        return 56
    return 64


def get_closest_aligned_fixed_bits(n):
    if n == 0 or n == 1:
        return 1
    if n <= 2:
        return 2
    if n <= 4:
        return 4
    if n <= 8:
        return 8
    if n <= 16:
        return 16
    if n <= 24:
        return 24
    if n <= 32:
        return 32
    if n <= 40:
        return 40
    if n <= 48:
        return 48
    if n <= 56:
        return 56
    return 64


def encode_bit_width(n):
    n = get_closest_fixed_bits(n)
    if 1 <= n <= 24:
        return n - 1
    return {26: 24, 28: 25, 30: 26, 32: 27, 40: 28, 48: 29, 56: 30}.get(n, 31)


def decode_bit_width(n):
    if 0 <= n <= 23:
        return n + 1
    return {24: 26, 25: 28, 26: 30, 27: 32, 28: 40, 29: 48, 30: 56}.get(n, 64)


def find_closest_num_bits(value):
    """value is a Java long; counts bits of its unsigned representation."""
    value = u64(value)
    count = 0
    while value != 0:
        count += 1
        value >>= 1
    return get_closest_fixed_bits(count)


def percentile_bits(data, offset, length, p):
    if p > 1.0 or p <= 0.0:
        return -1
    hist = [0] * 32
    for i in range(offset, offset + length):
        hist[encode_bit_width(find_closest_num_bits(data[i]))] += 1
    per_len = int(length * (1.0 - p))
    for i in range(31, -1, -1):
        per_len -= hist[i]
        if per_len < 0:
            return decode_bit_width(i)
    return 0


def is_safe_subtract(left, right):
    left, right = to_s64(left), to_s64(right)
    return (left ^ right) >= 0 or (left ^ to_s64(left - right)) >= 0


def zigzag_encode(val):
    val = to_s64(val)
    return u64((val << 1) ^ (val >> 63))


def write_vulong(out, value):
    value = u64(value)
    while True:
        if (value & ~0x7F) == 0:
            out.append(value)
            return
        out.append(0x80 | (value & 0x7F))
        value >>= 7


def write_vslong(out, value):
    write_vulong(out, zigzag_encode(value))


def write_ints(out, values, offset, length, bit_size):
    """Bit-pack values[offset:offset+length] MSB first at bit_size bits
    each (the unrolled fast paths of the Java writer produce the same
    bytes)."""
    if length < 1 or bit_size < 1:
        return
    acc, nbits = 0, 0
    mask = (1 << bit_size) - 1
    for i in range(offset, offset + length):
        acc = (acc << bit_size) | (u64(values[i]) & mask)
        nbits += bit_size
        while nbits >= 8:
            nbits -= 8
            out.append((acc >> nbits) & 0xFF)
    if nbits:
        out.append((acc << (8 - nbits)) & 0xFF)


# ------------------------------------------------ RunLengthIntegerWriterV2

class _JavaRleV2Writer:
    def __init__(self, signed, aligned_bit_packing=True):
        self.out = bytearray()
        self.signed = signed
        self.aligned = aligned_bit_packing
        self.literals = [0] * MAX_SCOPE
        self.zigzag = [0] * MAX_SCOPE
        self.base_red = [0] * MAX_SCOPE
        self.adj_deltas = [0] * MAX_SCOPE
        self.fixed_run_length = 0
        self.variable_run_length = 0
        self.prev_delta = 0
        self._clear()

    def _clear(self):
        self.num_literals = 0
        self.encoding = None
        self.prev_delta = 0
        self.fixed_delta = 0
        self.zz_bits_90p = 0
        self.zz_bits_100p = 0
        self.br_bits_95p = 0
        self.br_bits_100p = 0
        self.bits_delta_max = 0
        self.patch_gap_width = 0
        self.patch_length = 0
        self.patch_width = 0
        self.gap_vs_patch_list = None
        self.min = 0
        self.is_fixed_delta = True

    # -- positions -------------------------------------------------------
    def position(self):
        """(bytes written so far, values pending): what the Java writer
        records for a row group boundary."""
        return len(self.out), self.num_literals

    # -- writing ---------------------------------------------------------
    def _opcode(self):
        return self.encoding << 6

    def _write_values(self):
        if self.num_literals != 0:
            if self.encoding == SHORT_REPEAT:
                self._write_short_repeat()
            elif self.encoding == DIRECT:
                self._write_direct()
            elif self.encoding == PATCHED_BASE:
                self._write_patched_base()
            else:
                self._write_delta()
            self._clear()

    def _write_delta(self):
        fb = self.bits_delta_max
        efb = 0
        if self.aligned:
            fb = get_closest_aligned_fixed_bits(fb)
        if self.is_fixed_delta:
            if self.fixed_run_length > MIN_REPEAT:
                length = self.fixed_run_length - 1
                self.fixed_run_length = 0
            else:
                length = self.variable_run_length - 1
                self.variable_run_length = 0
        else:
            if fb == 1:
                fb = 2
            efb = encode_bit_width(fb) << 1
            length = self.variable_run_length - 1
            self.variable_run_length = 0
        tail = (length & 0x100) >> 8
        self.out.append(self._opcode() | efb | tail)
        self.out.append(length & 0xFF)
        if self.signed:
            write_vslong(self.out, self.literals[0])
        else:
            write_vulong(self.out, self.literals[0])
        if self.is_fixed_delta:
            write_vslong(self.out, self.fixed_delta)
        else:
            write_vslong(self.out, self.adj_deltas[0])
            write_ints(self.out, self.adj_deltas, 1, self.num_literals - 2, fb)

    def _write_patched_base(self):
        fb = self.br_bits_95p
        efb = encode_bit_width(fb) << 1
        self.variable_run_length -= 1
        tail = (self.variable_run_length & 0x100) >> 8
        first = self._opcode() | efb | tail
        second = self.variable_run_length & 0xFF
        mn = to_s64(self.min)
        negative = mn < 0
        if negative:
            mn = to_s64(-mn)
        base_width = find_closest_num_bits(mn) + 1
        base_bytes = base_width // 8 if base_width % 8 == 0 else base_width // 8 + 1
        bb = (base_bytes - 1) << 5
        if negative:
            mn = to_s64(u64(mn) | (1 << (base_bytes * 8 - 1)))
        third = bb | encode_bit_width(self.patch_width)
        fourth = ((self.patch_gap_width - 1) << 5) | self.patch_length
        self.out += bytes([first, second, third, fourth])
        for i in range(base_bytes - 1, -1, -1):
            self.out.append((u64(mn) >> (i * 8)) & 0xFF)
        closest = get_closest_fixed_bits(fb)
        write_ints(self.out, self.base_red, 0, self.num_literals, closest)
        closest = get_closest_fixed_bits(self.patch_gap_width + self.patch_width)
        write_ints(self.out, self.gap_vs_patch_list, 0,
                   len(self.gap_vs_patch_list), closest)
        self.variable_run_length = 0

    def _write_direct(self):
        fb = self.zz_bits_100p
        if self.aligned:
            fb = get_closest_aligned_fixed_bits(fb)
        efb = encode_bit_width(fb) << 1
        self.variable_run_length -= 1
        tail = (self.variable_run_length & 0x100) >> 8
        self.out.append(self._opcode() | efb | tail)
        self.out.append(self.variable_run_length & 0xFF)
        write_ints(self.out, self.zigzag, 0, self.num_literals, fb)
        self.variable_run_length = 0

    def _write_short_repeat(self):
        repeat = zigzag_encode(self.literals[0]) if self.signed \
            else u64(self.literals[0])
        nbits = find_closest_num_bits(repeat)
        nbytes = nbits >> 3 if nbits % 8 == 0 else (nbits >> 3) + 1
        header = self._opcode() | ((nbytes - 1) << 3)
        self.fixed_run_length -= MIN_REPEAT
        header |= self.fixed_run_length
        self.out.append(header)
        for i in range(nbytes - 1, -1, -1):
            self.out.append((repeat >> (i * 8)) & 0xFF)
        self.fixed_run_length = 0

    # -- encoding choice -------------------------------------------------
    def _compute_zigzag(self):
        for i in range(self.num_literals):
            self.zigzag[i] = zigzag_encode(self.literals[i]) if self.signed \
                else u64(self.literals[i])

    def _determine_encoding(self):
        self._compute_zigzag()
        self.zz_bits_100p = percentile_bits(self.zigzag, 0, self.num_literals, 1.0)
        if self.num_literals <= MIN_REPEAT:
            self.encoding = DIRECT
            return
        increasing = decreasing = True
        self.is_fixed_delta = True
        self.min = self.literals[0]
        mx = self.literals[0]
        initial_delta = to_s64(self.literals[1] - self.literals[0])
        curr_delta = 0
        delta_max = 0
        self.adj_deltas[0] = initial_delta
        for i in range(1, self.num_literals):
            l1, l0 = self.literals[i], self.literals[i - 1]
            curr_delta = to_s64(l1 - l0)
            self.min = min(self.min, l1)
            mx = max(mx, l1)
            increasing &= l1 >= l0
            decreasing &= l1 <= l0
            self.is_fixed_delta &= curr_delta == initial_delta
            if i > 1:
                self.adj_deltas[i - 1] = to_s64(abs(curr_delta))   # Math.abs
                delta_max = max(delta_max, self.adj_deltas[i - 1])
        if not is_safe_subtract(mx, self.min):
            self.encoding = DIRECT
            return
        if self.min == mx:
            self.fixed_delta = 0
            self.encoding = DELTA
            return
        if self.is_fixed_delta:
            self.encoding = DELTA
            self.fixed_delta = curr_delta
            return
        if initial_delta != 0:
            self.bits_delta_max = find_closest_num_bits(delta_max)
            if increasing or decreasing:
                self.encoding = DELTA
                return
        self.zz_bits_90p = percentile_bits(self.zigzag, 0, self.num_literals, 0.9)
        if self.zz_bits_100p - self.zz_bits_90p > 1:
            for i in range(self.num_literals):
                self.base_red[i] = to_s64(self.literals[i] - self.min)
            self.br_bits_95p = percentile_bits(self.base_red, 0, self.num_literals, 0.95)
            self.br_bits_100p = percentile_bits(self.base_red, 0, self.num_literals, 1.0)
            if self.br_bits_100p - self.br_bits_95p != 0:
                self.encoding = PATCHED_BASE
                self._prepare_patched_blob()
            else:
                self.encoding = DIRECT
        else:
            self.encoding = DIRECT

    def _prepare_patched_blob(self):
        mask = (1 << self.br_bits_95p) - 1
        self.patch_length = int(math.ceil(self.num_literals * 0.05))
        gap_list = [0] * self.patch_length
        patch_list = [0] * self.patch_length
        self.patch_width = get_closest_fixed_bits(self.br_bits_100p - self.br_bits_95p)
        if self.patch_width == 64:
            self.patch_width = 56
            self.br_bits_95p = 8
            mask = (1 << self.br_bits_95p) - 1
        gap_idx = patch_idx = prev = gap = max_gap = 0
        for i in range(self.num_literals):
            if u64(self.base_red[i]) > mask:          # Java compares signed
                gap = i - prev
                if gap > max_gap:
                    max_gap = gap
                prev = i
                gap_list[gap_idx] = gap
                gap_idx += 1
                patch_list[patch_idx] = u64(self.base_red[i]) >> self.br_bits_95p
                patch_idx += 1
                self.base_red[i] = u64(self.base_red[i]) & mask
        self.patch_length = gap_idx
        if max_gap == 0 and self.patch_length != 0:
            self.patch_gap_width = 1
        else:
            self.patch_gap_width = find_closest_num_bits(max_gap)
        if self.patch_gap_width > 8:
            self.patch_gap_width = 8
            if max_gap == 511:
                self.patch_length += 2
            else:
                self.patch_length += 1
        gap_idx = patch_idx = 0
        self.gap_vs_patch_list = [0] * self.patch_length
        i = 0
        while i < self.patch_length:
            g = gap_list[gap_idx]
            gap_idx += 1
            p = patch_list[patch_idx]
            patch_idx += 1
            while g > 255:
                self.gap_vs_patch_list[i] = 255 << self.patch_width
                i += 1
                g -= 255
            self.gap_vs_patch_list[i] = (g << self.patch_width) | p
            i += 1

    # -- public ----------------------------------------------------------
    def _initialize(self, val):
        self.literals[self.num_literals] = val
        self.num_literals += 1
        self.fixed_run_length = 1
        self.variable_run_length = 1

    def write(self, val):
        val = to_s64(val)
        if self.num_literals == 0:
            self._initialize(val)
            return
        if self.num_literals == 1:
            self.prev_delta = to_s64(val - self.literals[0])
            self.literals[self.num_literals] = val
            self.num_literals += 1
            if val == self.literals[0]:
                self.fixed_run_length = 2
                self.variable_run_length = 0
            else:
                self.fixed_run_length = 0
                self.variable_run_length = 2
            return
        current_delta = to_s64(val - self.literals[self.num_literals - 1])
        if self.prev_delta == 0 and current_delta == 0:
            self.literals[self.num_literals] = val
            self.num_literals += 1
            if self.variable_run_length > 0:
                self.fixed_run_length = 2
            self.fixed_run_length += 1
            if self.fixed_run_length >= MIN_REPEAT and self.variable_run_length > 0:
                self.num_literals -= MIN_REPEAT
                self.variable_run_length -= MIN_REPEAT - 1
                tail_vals = self.literals[self.num_literals:self.num_literals + MIN_REPEAT]
                self._determine_encoding()
                self._write_values()
                for l in tail_vals:
                    self.literals[self.num_literals] = l
                    self.num_literals += 1
            if self.fixed_run_length == MAX_SCOPE:
                self._determine_encoding()
                self._write_values()
        else:
            if self.fixed_run_length >= MIN_REPEAT:
                if self.fixed_run_length <= MAX_SHORT_REPEAT_LENGTH:
                    self.encoding = SHORT_REPEAT
                    self._write_values()
                else:
                    self.encoding = DELTA
                    self.is_fixed_delta = True
                    self._write_values()
            if 0 < self.fixed_run_length < MIN_REPEAT:
                if val != self.literals[self.num_literals - 1]:
                    self.variable_run_length = self.fixed_run_length
                    self.fixed_run_length = 0
            if self.num_literals == 0:
                self._initialize(val)
            else:
                self.prev_delta = to_s64(val - self.literals[self.num_literals - 1])
                self.literals[self.num_literals] = val
                self.num_literals += 1
                self.variable_run_length += 1
                if self.variable_run_length == MAX_SCOPE:
                    self._determine_encoding()
                    self._write_values()

    def flush(self):
        if self.num_literals != 0:
            if self.variable_run_length != 0:
                self._determine_encoding()
                self._write_values()
            elif self.fixed_run_length != 0:
                if self.fixed_run_length < MIN_REPEAT:
                    self.variable_run_length = self.fixed_run_length
                    self.fixed_run_length = 0
                    self._determine_encoding()
                    self._write_values()
                elif self.fixed_run_length <= MAX_SHORT_REPEAT_LENGTH:
                    self.encoding = SHORT_REPEAT
                    self._write_values()
                else:
                    self.encoding = DELTA
                    self.is_fixed_delta = True
                    self._write_values()



class _Sink:
    """Adapts the port's byte appends to an OutStream."""

    def __init__(self, out: OutStream):
        self.out = out

    def append(self, b: int) -> None:
        self.out.write(bytes([b]))

    def __iadd__(self, data):
        self.out.write(bytes(data))
        return self

    def __len__(self) -> int:
        return 0


class IntRleV2(_JavaRleV2Writer):
    """The Java writer's RLE v2 over a compressed OutStream; positions are
    the ones it records: the stream position plus the values pending."""

    def __init__(self, out: OutStream, signed: bool):
        super().__init__(signed, aligned_bit_packing=True)
        self.stream = out
        self.out = _Sink(out)

    def position(self) -> list:
        return self.stream.position() + [self.num_literals]

    def finish(self) -> None:
        self.flush()


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
            streams.append((S_LENGTH, self.length.stream.finish()))
        else:
            self.data.finish()
            out = self.data.rle.out if self.kind == "boolean" else self.data.stream
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
