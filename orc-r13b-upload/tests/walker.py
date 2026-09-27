"""Independent structural walk of a submitted ORC file.

Parses the container directly from the bytes (protobuf wire format by hand,
no ORC library) and checks exactly the rules CONTRACT.md states: NONE
compression, format version 0.12, no file metadata section, at most one
stripe (none for an empty batch), no row indexes, the declared schema, the
required column encodings, a PRESENT stream exactly for columns with at
least one missing value, and exact byte accounting with no gaps."""
from __future__ import annotations

EXPECTED_TYPES = [("event_id", 7), ("feed_id", 7), ("seq", 4),
                  ("latency_ms", 4), ("breach", 0), ("note", 7)]
DIRECT, DIRECT_V2 = 0, 2
REQUIRED_ENCODING = [DIRECT, DIRECT_V2, DIRECT_V2, DIRECT_V2, DIRECT_V2,
                     DIRECT, DIRECT_V2]
S_PRESENT, S_DATA, S_LENGTH = 0, 1, 2


class WalkError(AssertionError):
    pass


def _fail(msg):
    raise WalkError(msg)


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


def walk(data: bytes, expected_nulls: list[bool], nrows: int) -> None:
    """expected_nulls[i]: whether data column i (0-based over the five data
    columns after the struct) contains at least one missing value."""
    if data[:3] != b"ORC":
        _fail("file does not start with the ORC magic")
    ps_len = data[-1]
    if ps_len == 0 or 1 + ps_len + 1 > len(data):
        _fail("bad postscript length")
    ps = parse_pb(data[len(data) - 1 - ps_len:len(data) - 1])
    if _scalar(ps, 2, 0) != 0:
        _fail("compression must be NONE")
    ver = _repeated_uints(ps, 4)
    if ver != [0, 12]:
        _fail(f"format version must be 0.12, got {ver}")
    if _scalar(ps, 5, 0) != 0:
        _fail("file metadata section must be empty (metadataLength 0)")
    magic = _scalar_bytes(ps, 8000)
    if magic != b"ORC":
        _fail("postscript magic missing")
    footer_len = _scalar(ps, 1, 0)

    footer_end = len(data) - 1 - ps_len
    footer_start = footer_end - footer_len
    if footer_start < 3:
        _fail("footer overlaps header")
    footer = parse_pb(data[footer_start:footer_end])

    if _scalar(footer, 1, 0) != 3:
        _fail("headerLength must be 3")
    content_length = _scalar(footer, 2, 0)
    if content_length != footer_start:
        _fail("contentLength must equal the bytes before the footer")
    if _scalar(footer, 6, 0) != nrows:
        _fail(f"footer numberOfRows must be {nrows}")
    if _scalar(footer, 8, 0) != 0:
        _fail("rowIndexStride must be 0 (no row indexes)")

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

    stripes = _all(footer, 3)
    if nrows == 0:
        if stripes:
            _fail("an empty batch must contain no stripe")
        if content_length != 3:
            _fail("empty file content must be the 3 magic bytes")
        return
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
    if index_len != 0:
        _fail("indexLength must be 0 (no row indexes)")
    if st_rows != nrows:
        _fail("stripe numberOfRows must equal the batch row count")
    if 3 + data_len + sfooter_len != content_length:
        _fail("stripe data plus stripe footer must tile contentLength exactly")

    sfooter = parse_pb(data[3 + data_len:3 + data_len + sfooter_len])
    encodings = [_scalar(parse_pb(v), 1, 0) for v in _all(sfooter, 2)]
    if encodings != [REQUIRED_ENCODING[i] for i in range(7)]:
        _fail(f"column encodings must be {REQUIRED_ENCODING}, got {encodings}")

    streams = [parse_pb(v) for v in _all(sfooter, 1)]
    total = 0
    seen = {}
    for s in streams:
        kind = _scalar(s, 1, 0)
        col = _scalar(s, 2, 0)
        length = _scalar(s, 3, 0)
        if kind == 6:
            _fail("no ROW_INDEX streams are allowed")
        total += length
        seen.setdefault(col, []).append(kind)
    if total != data_len:
        _fail("declared stream lengths must sum to the stripe dataLength")
    if 0 in seen:
        _fail("the struct root must not carry streams")
    for i, has_null in enumerate(expected_nulls, start=1):
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
