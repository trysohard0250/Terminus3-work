"""Independent structural walk of a submitted ORC file.

Parses the container directly from the bytes (protobuf wire format by
hand, ZLIB chunk framing by hand, no ORC library) and checks exactly the
rules CONTRACT.md states: ZLIB compression with a 65536-byte block size
and every chunk compressed, format version 0.12, at most one stripe (none
for an empty batch), the declared schema, the required column encodings,
a PRESENT stream exactly for columns with at least one missing value, a
row index with a 900-row stride whose entries carry the right number of
positions within the right bounds - exact positions for packed-bit
streams and string data, whose content the walk can decode without an
ORC library - and per-group statistics, a bloom filter per row group
for every string and bigint column recomputed bit for bit from the
rows, stripe statistics in the metadata section, file statistics in
the footer, and exact byte accounting with no gaps. Values are the
readers' business (test_outputs.py); the walker grades structure,
statistics and bloom filters."""
from __future__ import annotations

import zlib

COLUMNS = ["event_id", "feed_id", "seq", "latency_ms", "breach", "note"]
EXPECTED_TYPES = [("event_id", 7), ("feed_id", 7), ("seq", 4),
                  ("latency_ms", 4), ("breach", 0), ("note", 7)]
KINDS = ["struct", "string", "string", "long", "long", "boolean", "string"]
DIRECT, DIRECT_V2 = 0, 2
REQUIRED_ENCODING = [DIRECT, DIRECT_V2, DIRECT_V2, DIRECT_V2, DIRECT_V2,
                     DIRECT, DIRECT_V2]
S_PRESENT, S_DATA, S_LENGTH, S_ROW_INDEX, S_BLOOM = 0, 1, 2, 6, 8
C_ZLIB = 1
BLOCK = 65536                # compression block size (CONTRACT.md 3)
STRIDE = 900                 # row index stride (CONTRACT.md 3)
MAX_INT_SKIP = 511           # values pending in an RLE v2 run
MAX_BYTE_SKIP = 129          # bytes pending in a byte run-length run
MAX_BITS = 7                 # bits consumed in a boolean byte
BLOOM_COLUMNS = (1, 2, 3, 4, 6)     # string and bigint columns
BLOOM_BITS, BLOOM_HASHES = 5632, 4  # 900 entries at fpp 0.05
# the index area, in order: every column's ROW_INDEX stream, followed
# for the bloom columns by their BLOOM_FILTER_UTF8 stream
INDEX_STREAMS = [s for c in range(7) for s in
                 ([(S_ROW_INDEX, c)] + ([(S_BLOOM, c)]
                                        if c in BLOOM_COLUMNS else []))]


# -------------------------------------------------------- bloom filters
# ORC's bloom filter as the Java and C++ writers compute it (validated
# bit for bit against files written by the Apache ORC Java writer).

_MASK64 = (1 << 64) - 1
_C1, _C2 = 0x87c37b91114253d5, 0x4cf5ad432745937f


def _rotl64(x, r):
    return ((x << r) | (x >> (64 - r))) & _MASK64


def _fmix64(k):
    k ^= k >> 33
    k = (k * 0xff51afd7ed558ccd) & _MASK64
    k ^= k >> 33
    k = (k * 0xc4ceb9fe1a85ec53) & _MASK64
    k ^= k >> 33
    return k


def murmur3_hash64(data, seed=104729):
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


def _sra64(x, s):
    """Arithmetic right shift of a 64-bit two's complement value."""
    return ((x - (1 << 64)) >> s) & _MASK64 if x >> 63 else x >> s


def long_hash(key):
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


def _int32(x):
    x &= 0xFFFFFFFF
    return x - (1 << 32) if x & 0x80000000 else x


def expected_bloom(values):
    """The bloom filter bit set (little-endian 64-bit words) of a row
    group's non-null values."""
    bits = bytearray(BLOOM_BITS // 8)
    for v in values:
        if v is None:
            continue
        h = murmur3_hash64(v.encode("utf-8")) if isinstance(v, str) \
            else long_hash(v)
        hash1, hash2 = _int32(h), _int32(h >> 32)
        for i in range(1, BLOOM_HASHES + 1):
            combined = _int32(hash1 + i * hash2)
            if combined < 0:
                combined = ~combined
            pos = combined % BLOOM_BITS
            bits[pos >> 3] |= 1 << (pos & 7)
    return bytes(bits)


class WalkError(AssertionError):
    pass


def _fail(msg):
    raise WalkError(msg)


# ------------------------------------------------------------ protobuf

def parse_pb(buf):
    """Return list of (field, wire_type, value); value is int or bytes.

    Decodes by protobuf wire rules: unknown fields are carried through,
    and group fields (wire types 3 and 4, valid unknown fields in the
    wire format) are skipped over without contributing values."""
    out, i = [], 0
    while i < len(buf):
        tag, i = _varint(buf, i)
        field, wt = tag >> 3, tag & 7
        if wt == 0:
            v, i = _varint(buf, i)
            out.append((field, 0, v))
        elif wt == 2:
            ln, i = _varint(buf, i)
            if i + ln > len(buf):
                _fail("truncated protobuf field")
            out.append((field, 2, buf[i:i + ln]))
            i += ln
        elif wt == 5:
            out.append((field, 5, buf[i:i + 4]))
            i += 4
        elif wt == 1:
            out.append((field, 1, buf[i:i + 8]))
            i += 8
        elif wt == 3:
            i = _skip_group(buf, i, field)
        elif wt == 4:
            _fail(f"end-group tag for field {field} without a start")
        else:
            _fail(f"invalid wire type {wt}")
    return out


def _skip_group(buf, i, field):
    """Skip a group field (deprecated but valid protobuf): consume until
    the matching end-group tag, handling nested content by wire rules."""
    while True:
        if i >= len(buf):
            _fail(f"group field {field} is not terminated")
        tag, i = _varint(buf, i)
        f, wt = tag >> 3, tag & 7
        if wt == 4:
            if f != field:
                _fail(f"group field {field} closed by end tag {f}")
            return i
        if wt == 0:
            _, i = _varint(buf, i)
        elif wt == 2:
            ln, i = _varint(buf, i)
            if i + ln > len(buf):
                _fail("truncated protobuf field inside a group")
            i += ln
        elif wt == 5:
            i += 4
        elif wt == 1:
            i += 8
        elif wt == 3:
            i = _skip_group(buf, i, f)
        else:
            _fail(f"invalid wire type {wt} inside a group")


def _varint(buf, i):
    v, s = 0, 0
    while True:
        if i >= len(buf):
            _fail("truncated varint")
        b = buf[i]
        i += 1
        v |= (b & 0x7F) << s
        s += 7
        if not b & 0x80:
            return v, i


def _scalar(fields, num, default=None):
    """Decoded value of a singular varint field.

    Schema-aware protobuf decoding: an occurrence of a known field number
    with a mismatched wire type is skipped as an unknown field, and when
    the field occurs more than once with its own wire type, the LAST
    occurrence is the decoded value (standard merge semantics)."""
    out = default
    for f, wt, v in fields:
        if f == num and wt == 0:
            out = v
    return out


def _sint(v):
    """A sint64 field's value from its zigzag varint (None stays None)."""
    if v is None:
        return None
    return (v >> 1) ^ -(v & 1)


def _scalar_bytes(fields, num, default=None):
    """Decoded value of a singular length-delimited field; occurrences
    with other wire types are skipped as unknown fields."""
    out = default
    for f, wt, v in fields:
        if f == num and wt == 2:
            out = v
    return out


def _all(fields, num):
    """All occurrences of a repeated message field, in order; occurrences
    with non-length-delimited wire types are skipped as unknown fields."""
    return [v for f, wt, v in fields if f == num and wt == 2]


def _message(fields, num):
    """Decoded value of a singular embedded message: occurrences merge by
    concatenation (standard merge semantics); None when absent."""
    parts = _all(fields, num)
    return b"".join(parts) if parts else None


def _repeated_uints(fields, num):
    """Values of a repeated unsigned integer field, in order.

    Protobuf readers must accept both valid serializations of a packable
    repeated numeric field: packed (one length-delimited record holding
    varints) and unpacked (one varint record per value), even mixed.
    Occurrences with any other wire type are skipped as unknown fields."""
    out = []
    for f, wt, v in fields:
        if f != num:
            continue
        if wt == 0:
            out.append(v)
        elif wt == 2:
            i = 0
            while i < len(v):
                x, i = _varint(v, i)
                out.append(x)
    return out


# --------------------------------------------------------- compression

def dechunk(buf, what):
    """Decode a compressed section: a sequence of chunks, each a
    three-byte little-endian header ((length << 1) | original) and a
    body of that length holding one complete raw DEFLATE stream.
    Returns (content, chunk table) where the table lists (offset of the
    chunk header within the section, content length of the chunk)."""
    out, table, i = bytearray(), [], 0
    while i < len(buf):
        if i + 3 > len(buf):
            _fail(f"{what}: truncated chunk header at {i}")
        head = buf[i] | (buf[i + 1] << 8) | (buf[i + 2] << 16)
        length, original = head >> 1, head & 1
        if original:
            _fail(f"{what}: chunk at {i} is stored original; every chunk "
                  "must be compressed")
        if i + 3 + length > len(buf):
            _fail(f"{what}: chunk at {i} runs past the end of the section")
        body = buf[i + 3:i + 3 + length]
        d = zlib.decompressobj(-15)
        try:
            # never inflate past the block size: an oversized chunk is
            # rejected, not expanded
            content = d.decompress(body, BLOCK + 1)
        except zlib.error as e:
            _fail(f"{what}: chunk at {i} is not raw DEFLATE ({e})")
        if d.unconsumed_tail or len(content) > BLOCK:
            _fail(f"{what}: chunk at {i} decompresses to more than {BLOCK} "
                  "bytes")
        if not d.eof:
            _fail(f"{what}: chunk at {i} holds an incomplete DEFLATE stream")
        if d.unused_data:
            _fail(f"{what}: chunk at {i} holds bytes after its DEFLATE stream")
        if len(content) == 0:
            _fail(f"{what}: chunk at {i} decompresses to 0 bytes; 1 to "
                  f"{BLOCK} required")
        table.append((i, len(content)))
        out += content
        i += 3 + length
    return bytes(out), table


def byte_rle_runs(content, what):
    """Map each run header's content offset to the number of decoded
    bytes before that run (the end of the content included), and the
    total decoded byte count, for a byte run-length stream: a header
    below 128 opens a repeat of header+3 copies of the next byte, a
    header of 128 or more a literal group of 256-header bytes."""
    runs, i, n = {}, 0, 0
    while i < len(content):
        runs[i] = n
        h = content[i]
        if h < 128:
            if i + 2 > len(content):
                _fail(f"{what}: truncated byte run-length run")
            n += h + 3
            i += 2
        else:
            if i + 1 + (256 - h) > len(content):
                _fail(f"{what}: truncated byte run-length literal group")
            n += 256 - h
            i += 1 + (256 - h)
    runs[len(content)] = n
    return runs, n


# ---------------------------------------------------------- statistics

def expected_stats(kind, values):
    """The statistics CONTRACT.md requires over the given rows' values."""
    if kind == "struct":
        return {"count": len(values), "has_null": False}
    nn = [v for v in values if v is not None]
    st = {"count": len(nn), "has_null": len(nn) != len(values)}
    if kind == "long":
        st["min"] = min(nn) if nn else None
        st["max"] = max(nn) if nn else None
        total = sum(nn)
        st["sum"] = total if -2**63 <= total < 2**63 else None
    elif kind == "string":
        st["min"] = min(nn).encode("utf-8") if nn else None
        st["max"] = max(nn).encode("utf-8") if nn else None
        st["sum"] = sum(len(v.encode("utf-8")) for v in nn)
    elif kind == "boolean":
        st["trues"] = sum(1 for v in nn if v)
    return st


# the type-specific statistics messages of ColumnStatistics, by field
# number: a column carries only its own kind's (the root none); the ORC
# C++ reader does not survive one of another kind, so the walk rejects
# it before the reader sees it
TYPE_STATS = {2: "intStatistics", 3: "doubleStatistics",
              4: "stringStatistics", 5: "bucketStatistics",
              6: "decimalStatistics", 7: "dateStatistics",
              8: "binaryStatistics", 9: "timestampStatistics",
              12: "collectionStatistics"}
OWN_STATS = {"struct": None, "long": 2, "string": 4, "boolean": 5}


def check_stats(buf, kind, values, label):
    """One ColumnStatistics message against the values it must describe."""
    fields = parse_pb(buf)
    want = expected_stats(kind, values)
    for f, wt, _ in fields:
        if f in TYPE_STATS and f != OWN_STATS[kind] and wt == 2:
            _fail(f"{label}: carries {TYPE_STATS[f]}, which is not the "
                  f"column's kind of statistics")
    if _scalar(fields, 1, 0) != want["count"]:
        _fail(f"{label}: numberOfValues {_scalar(fields, 1, 0)}, "
              f"expected {want['count']}")
    has_null = _scalar(fields, 10)
    if has_null is None:
        _fail(f"{label}: hasNull must be present (readers take an absent "
              "hasNull as true)")
    if bool(has_null) != want["has_null"]:
        _fail(f"{label}: hasNull must be {want['has_null']}")
    if kind == "long":
        sub = parse_pb(_message(fields, 2) or b"")
        got = (_sint(_scalar(sub, 1)), _sint(_scalar(sub, 2)),
               _sint(_scalar(sub, 3)))
        if got != (want["min"], want["max"], want["sum"]):
            _fail(f"{label}: integer statistics (minimum, maximum, sum) "
                  f"{got}, expected {(want['min'], want['max'], want['sum'])}")
    elif kind == "string":
        sub = parse_pb(_message(fields, 4) or b"")
        got = (_scalar_bytes(sub, 1), _scalar_bytes(sub, 2),
               _sint(_scalar(sub, 3)))
        if got != (want["min"], want["max"], want["sum"]):
            _fail(f"{label}: string statistics (minimum, maximum, sum) "
                  f"differ from the expected values")
    elif kind == "boolean":
        sub = parse_pb(_message(fields, 5) or b"")
        got = _repeated_uints(sub, 1)
        if got != [want["trues"]]:
            _fail(f"{label}: bucket count {got}, expected {[want['trues']]}")


# ---------------------------------------------------------- positions

class Stream:
    """One data stream of a column as the walk sees it: its chunk table,
    compressed length, decompressed content, decoder ("int" for RLE v2,
    "bool" for packed bits over byte run-length, None for raw bytes)
    and, for bool streams, the run-header map of its content."""

    def __init__(self, name, table, length, content, decoder):
        self.name, self.table, self.length = name, table, length
        self.content, self.decoder = content, decoder
        self.chunks = {}                  # header offset -> content offset
        cum = 0
        for off, clen in table:
            self.chunks[off] = (cum, clen)
            cum += clen
        self.runs = byte_rle_runs(content, name)[0] if decoder == "bool" \
            else None


def _check_positions(pos, streams, targets, label):
    """One row-index entry's positions against the column's streams.

    streams: [Stream] in the order the positions must follow (PRESENT,
    DATA, LENGTH). targets: per stream, what the position must reach -
    for a raw string DATA stream the content byte at which the group's
    first value begins; for a packed-bit stream the (byte index, bit)
    of the group's first bit; None for an RLE v2 stream, whose positions
    are checked for shape and bounds here and for meaning by the C++
    reader's seek. Returns the (chunk offset, content offset) pairs, one
    per stream, for the monotonicity check."""
    extra = {None: 0, "int": 1, "bool": 2}
    i, marks = 0, []
    for s, target in zip(streams, targets):
        n = 2 + extra[s.decoder]
        if i + n > len(pos):
            _fail(f"{label}: too few positions ({len(pos)}); the {s.name} "
                  f"stream needs {n} more")
        chunk, offset = pos[i], pos[i + 1]
        if chunk in s.chunks:
            cum, clen = s.chunks[chunk]
            if offset > clen:
                _fail(f"{label}: {s.name} position {offset} is beyond the "
                      f"{clen} content bytes of the chunk at {chunk}")
            at = cum + offset               # offset in the whole content
        elif chunk == s.length:
            if offset != 0:
                _fail(f"{label}: {s.name} position at the stream's end must "
                      "have content offset 0")
            at = len(s.content)
        else:
            _fail(f"{label}: {s.name} position {chunk} is not the start of a "
                  "chunk of that stream")
        if s.decoder is None:
            if at != target:
                _fail(f"{label}: {s.name} position names content byte {at}; "
                      f"the group's first value begins at byte {target}")
        elif s.decoder == "int":
            if pos[i + 2] > MAX_INT_SKIP:
                _fail(f"{label}: {s.name} pending-value count {pos[i + 2]} "
                      f"exceeds {MAX_INT_SKIP}")
        else:
            pending, bits = pos[i + 2], pos[i + 3]
            if pending > MAX_BYTE_SKIP:
                _fail(f"{label}: {s.name} pending-byte count {pending} "
                      f"exceeds {MAX_BYTE_SKIP}")
            if bits > MAX_BITS:
                _fail(f"{label}: {s.name} bit count {bits} exceeds "
                      f"{MAX_BITS}")
            if at not in s.runs:
                _fail(f"{label}: {s.name} content offset {at} is not a run "
                      "header of the byte run-length stream")
            byte, bit = target
            if s.runs[at] + pending != byte or bits != bit:
                _fail(f"{label}: {s.name} position reaches byte "
                      f"{s.runs[at] + pending} bit {bits}; the group's first "
                      f"bit is byte {byte} bit {bit}")
        marks.append((chunk, offset))
        i += n
    if i != len(pos):
        _fail(f"{label}: {len(pos)} positions, exactly {i} required")
    return marks


# ---------------------------------------------------------------- walk

def walk(data: bytes, records: list) -> None:
    """Grade the container bytes of one batch against the records the
    batch holds (used only for row count, null presence and statistics;
    values themselves are the readers' business)."""
    nrows = len(records)
    values = {c: [r[c] for r in records] for c in COLUMNS}
    columns = [None] + [values[c] for c in COLUMNS]    # index by column id
    nulls = [any(v is None for v in values[c]) for c in COLUMNS]

    if data[:3] != b"ORC":
        _fail("file does not start with the ORC magic")
    ps_len = data[-1]
    if ps_len == 0 or 1 + ps_len + 1 > len(data):
        _fail("bad postscript length")
    ps = parse_pb(data[len(data) - 1 - ps_len:len(data) - 1])
    if _scalar(ps, 2, 0) != C_ZLIB:
        _fail("compression must be ZLIB")
    if _scalar(ps, 3, 0) != BLOCK:
        _fail(f"compressionBlockSize must be {BLOCK}")
    ver = _repeated_uints(ps, 4)
    if ver != [0, 12]:
        _fail(f"format version must be 0.12, got {ver}")
    magic = _scalar_bytes(ps, 8000)
    if magic != b"ORC":
        _fail("postscript magic missing")
    if _scalar(ps, 6, 0) < 1:
        _fail("postscript writerVersion must be at least 1 (the readers "
              "discard string and boolean statistics of an older writer)")
    footer_len = _scalar(ps, 1, 0)
    metadata_len = _scalar(ps, 5, 0)

    footer_end = len(data) - 1 - ps_len
    footer_start = footer_end - footer_len
    metadata_start = footer_start - metadata_len
    if metadata_start < 3:
        _fail("footer and metadata overlap the header")
    footer, _ = dechunk(data[footer_start:footer_end], "footer")
    footer = parse_pb(footer)

    if _scalar(footer, 1, 0) != 3:
        _fail("headerLength must be 3")
    content_length = _scalar(footer, 2, 0)
    if content_length != metadata_start:
        _fail("contentLength must equal the bytes before the metadata section")
    if _scalar(footer, 6, 0) != nrows:
        _fail(f"footer numberOfRows must be {nrows}")
    if _scalar(footer, 8, 0) != STRIDE:
        _fail(f"rowIndexStride must be {STRIDE}")

    types = _all(footer, 4)
    if len(types) != 1 + len(EXPECTED_TYPES):
        _fail("schema must have the struct root and six columns")
    root = parse_pb(types[0])
    if _scalar(root, 1, 0) != 12:
        _fail("root type must be STRUCT")
    names = [v.decode() for f, wt, v in root if f == 3 and wt == 2]
    if names != [n for n, _ in EXPECTED_TYPES]:
        _fail(f"field names must be {[n for n, _ in EXPECTED_TYPES]}")
    subtypes = _repeated_uints(root, 2)
    if subtypes != list(range(1, 7)):
        _fail("struct subtypes must be columns 1..6 in order")
    for i, (_, kind) in enumerate(EXPECTED_TYPES, start=1):
        got = _scalar(parse_pb(types[i]), 1, 0)
        if got != kind:
            _fail(f"column {i} type kind must be {kind}, got {got}")

    # file statistics: one ColumnStatistics per column, root first
    file_stats = _all(footer, 7)
    if len(file_stats) != 7:
        _fail(f"footer must carry 7 column statistics, got {len(file_stats)}")
    for c in range(7):
        check_stats(file_stats[c], KINDS[c], columns[c] or records,
                    f"file statistics column {c}")

    # metadata section: the stripe's statistics (none for an empty batch)
    if metadata_len:
        meta, _ = dechunk(data[metadata_start:footer_start], "metadata")
        stripe_stats = _all(parse_pb(meta), 1)
    else:
        stripe_stats = []
    if len(stripe_stats) != (1 if nrows else 0):
        _fail(f"metadata must hold statistics for {1 if nrows else 0} "
              f"stripe(s), got {len(stripe_stats)}")

    stripes = _all(footer, 3)
    if nrows == 0:
        if stripes:
            _fail("an empty batch must contain no stripe")
        if content_length != 3:
            _fail("empty file content must be the 3 magic bytes")
        return
    col_stats = _all(parse_pb(stripe_stats[0]), 1)
    if len(col_stats) != 7:
        _fail(f"stripe statistics must cover 7 columns, got {len(col_stats)}")
    for c in range(7):
        check_stats(col_stats[c], KINDS[c], columns[c] or records,
                    f"stripe statistics column {c}")

    if len(stripes) != 1:
        _fail(f"exactly one stripe required, got {len(stripes)}")
    st = parse_pb(stripes[0])
    offset = _scalar(st, 1, 0)
    index_len = _scalar(st, 2, 0)
    data_len = _scalar(st, 3, 0)
    sfooter_len = _scalar(st, 4, 0)
    st_rows = _scalar(st, 5, 0)
    if offset != 3:
        _fail("the stripe must start immediately after the header magic")
    if st_rows != nrows:
        _fail("stripe numberOfRows must equal the batch row count")
    if 3 + index_len + data_len + sfooter_len != content_length:
        _fail("stripe index, data and footer must tile contentLength exactly")

    sfooter, _ = dechunk(data[3 + index_len + data_len:
                              3 + index_len + data_len + sfooter_len],
                         "stripe footer")
    sfooter = parse_pb(sfooter)
    encs = [parse_pb(v) for v in _all(sfooter, 2)]
    encodings = [_scalar(e, 1, 0) for e in encs]
    if encodings != REQUIRED_ENCODING:
        _fail(f"column encodings must be {REQUIRED_ENCODING}, got {encodings}")
    for c, e in enumerate(encs):
        got = _scalar(e, 3)
        if c in BLOOM_COLUMNS and got != 1:
            _fail(f"column {c} bloomEncoding must be 1 (UTF8)")
        if c not in BLOOM_COLUMNS and got not in (None, 0):
            _fail(f"column {c} carries no bloom filter and must not declare "
                  f"bloomEncoding {got}")

    # streams, in the order they are laid out: the index area first
    # (every column's ROW_INDEX stream, each bloom column's
    # BLOOM_FILTER_UTF8 stream right after its own), then data streams
    streams = []
    for v in _all(sfooter, 1):
        s = parse_pb(v)
        streams.append((_scalar(s, 1, 0), _scalar(s, 2, 0), _scalar(s, 3, 0)))
    n_index = len(INDEX_STREAMS)
    if [(k, c) for k, c, _ in streams[:n_index]] != INDEX_STREAMS:
        _fail(f"the first {n_index} streams must be the index streams "
              f"{INDEX_STREAMS} (kind, column), in that order")
    if any(k in (S_ROW_INDEX, S_BLOOM) for k, _, _ in streams[n_index:]):
        _fail("no ROW_INDEX or BLOOM_FILTER_UTF8 stream may follow the "
              "index area")
    if sum(ln for _, _, ln in streams[:n_index]) != index_len:
        _fail("index stream lengths must sum to the stripe indexLength")
    if sum(ln for _, _, ln in streams[n_index:]) != data_len:
        _fail("data stream lengths must sum to the stripe dataLength")

    # walk every stream's chunks and keep the chunk tables and content
    pos, tables = 3, {}
    for kind, col, length in streams:
        if (col, kind) in tables:
            _fail(f"column {col} carries stream kind {kind} more than once")
        content, table = dechunk(data[pos:pos + length],
                                 f"column {col} stream kind {kind}")
        tables[(col, kind)] = (table, length, content)
        pos += length

    seen = {}
    for kind, col, _ in streams[n_index:]:
        if not 0 <= col <= 6:
            _fail(f"stream for nonexistent column {col}")
        seen.setdefault(col, []).append(kind)
    if 0 in seen:
        _fail("the struct root must not carry data streams")
    for i, has_null in enumerate(nulls, start=1):
        kinds = seen.get(i, [])
        if has_null and S_PRESENT not in kinds:
            _fail(f"column {i} has missing values and needs a PRESENT stream")
        if not has_null and S_PRESENT in kinds:
            _fail(f"column {i} has no missing values and must not carry a "
                  "PRESENT stream")
        if S_DATA not in kinds:
            _fail(f"column {i} needs a DATA stream")
        is_string = EXPECTED_TYPES[i - 1][1] == 7
        if is_string and S_LENGTH not in kinds:
            _fail(f"column {i} needs a LENGTH stream")
        if not is_string and S_LENGTH in kinds:
            _fail(f"column {i} must not carry a LENGTH stream")
        extra = [k for k in kinds if k not in (S_PRESENT, S_DATA, S_LENGTH)]
        if extra:
            _fail(f"column {i} carries unexpected stream kinds {extra}")

    # the streams whose content the walk can check exactly against the
    # rows: packed-bit streams hold exactly ceil(bits / 8) bytes (rows
    # for PRESENT, non-null values for the boolean DATA), and a string
    # DATA stream is exactly the UTF-8 bytes of the column's non-null
    # values in row order
    for c in range(1, 7):
        vals = columns[c]
        if nulls[c - 1]:
            table, length, content = tables[(c, S_PRESENT)]
            n = byte_rle_runs(content, f"column {c} PRESENT")[1]
            if n != (nrows + 7) // 8:
                _fail(f"column {c} PRESENT decodes to {n} bytes; "
                      f"{(nrows + 7) // 8} needed for {nrows} rows")
        nn = [v for v in vals if v is not None]
        if KINDS[c] == "boolean":
            table, length, content = tables[(c, S_DATA)]
            n = byte_rle_runs(content, f"column {c} DATA")[1]
            if n != (len(nn) + 7) // 8:
                _fail(f"column {c} DATA decodes to {n} bytes; "
                      f"{(len(nn) + 7) // 8} needed for {len(nn)} values")
        elif KINDS[c] == "string":
            table, length, content = tables[(c, S_DATA)]
            if content != b"".join(v.encode("utf-8") for v in nn):
                _fail(f"column {c} DATA is not the UTF-8 bytes of the "
                      "column's values in row order")

    # the row index: one entry per 900-row group in every column's
    # ROW_INDEX stream, with positions for the column's streams (none for
    # the root) and statistics over the group's rows; and, for the
    # bloom columns, one bloom filter per group, recomputed from the rows
    groups = (nrows + STRIDE - 1) // STRIDE
    for c in range(7):
        content = tables[(c, S_ROW_INDEX)][2]
        entries = _all(parse_pb(content), 1)
        if len(entries) != groups:
            _fail(f"column {c} ROW_INDEX must hold {groups} entries, "
                  f"got {len(entries)}")
        specs = []
        if c:
            vals = columns[c]
            if nulls[c - 1]:
                specs.append(Stream("PRESENT", *tables[(c, S_PRESENT)],
                                    "bool"))
            if KINDS[c] == "string":
                specs.append(Stream("DATA", *tables[(c, S_DATA)], None))
                specs.append(Stream("LENGTH", *tables[(c, S_LENGTH)], "int"))
            elif KINDS[c] == "long":
                specs.append(Stream("DATA", *tables[(c, S_DATA)], "int"))
            else:
                specs.append(Stream("DATA", *tables[(c, S_DATA)], "bool"))

        def targets_for(g):
            """What each stream's position must reach for group g."""
            if not c:
                return []
            before = vals[:g * STRIDE]
            nn = [v for v in before if v is not None]
            out = []
            if nulls[c - 1]:
                out.append((len(before) // 8, len(before) % 8))
            if KINDS[c] == "string":
                out.append(sum(len(v.encode("utf-8")) for v in nn))
                out.append(None)
            elif KINDS[c] == "long":
                out.append(None)
            else:
                out.append((len(nn) // 8, len(nn) % 8))
            return out

        previous = None
        for g, entry in enumerate(entries):
            label = f"column {c} row group {g}"
            e = parse_pb(entry)
            positions = _repeated_uints(e, 1)
            marks = _check_positions(positions, specs, targets_for(g), label)
            if g == 0 and any(positions):
                _fail(f"{label}: the first row group's positions must all "
                      "be 0")
            if previous is not None and any(m < p for m, p in
                                            zip(marks, previous)):
                _fail(f"{label}: positions move backwards within a stream")
            previous = marks
            stats = _message(e, 2)
            if stats is None:
                _fail(f"{label}: entry carries no statistics")
            rows = records[g * STRIDE:(g + 1) * STRIDE]
            check_stats(stats, KINDS[c],
                        [r[COLUMNS[c - 1]] for r in rows] if c else rows,
                        label)

    # the bloom filters: one per row group for every bloom column,
    # recomputed from the rows and compared bit for bit
    for c in BLOOM_COLUMNS:
        content = tables[(c, S_BLOOM)][2]
        filters = _all(parse_pb(content), 1)
        if len(filters) != groups:
            _fail(f"column {c} BLOOM_FILTER_UTF8 must hold {groups} "
                  f"filters, got {len(filters)}")
        for g, bf in enumerate(filters):
            f = parse_pb(bf)
            if _scalar(f, 1) != BLOOM_HASHES:
                _fail(f"column {c} row group {g}: bloom filter must declare "
                      f"{BLOOM_HASHES} hash functions")
            got = _scalar_bytes(f, 3)
            want = expected_bloom(columns[c][g * STRIDE:(g + 1) * STRIDE])
            if got is None:
                _fail(f"column {c} row group {g}: bloom filter carries no "
                      "utf8bitset")
            if len(got) != len(want):
                _fail(f"column {c} row group {g}: bloom filter has "
                      f"{len(got) * 8} bits, {BLOOM_BITS} required")
            if got != want:
                diff = sum(bin(a ^ b).count("1") for a, b in zip(got, want))
                _fail(f"column {c} row group {g}: bloom filter differs from "
                      f"the one the rows imply in {diff} bit(s)")
