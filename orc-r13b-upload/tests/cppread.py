"""Read one submitted ORC file with the Apache ORC C++ library (pyorc).

Run as a subprocess by test_outputs.py: the expected rows arrive on
stdin as JSON, the verdict leaves on stdout as JSON. A separate process
because the C++ reader's row-index seek trusts the positions it is
given: a malformed index can take the reader down, and that must fail
the file, not the verifier.

Checks: the whole file decodes to the expected rows; a seek to the first
row of every 900-row group (which the reader performs through the row
index) followed by a read to the end yields exactly the rows from that
row on; seeks inside groups and to the last row do too; the file
statistics the library reports agree with the rows, and so do the
stripe statistics it builds by merging the row index's per-group
statistics (every figure but the integer sum, which its merge carries
across a group that omitted it).
"""
import json
import sys

import pyorc

COLUMNS = ["event_id", "feed_id", "seq", "latency_ms", "breach", "note"]
KINDS = ["struct", "string", "string", "long", "long", "boolean", "string"]
STRIDE = 900


def expected_stats(kind, values):
    if kind == "struct":
        return {"number_of_values": len(values), "has_null": False}
    nn = [v for v in values if v is not None]
    st = {"number_of_values": len(nn), "has_null": len(nn) != len(values)}
    if kind == "long":
        if nn:
            st["minimum"], st["maximum"] = min(nn), max(nn)
        total = sum(nn)
        if -2**63 <= total < 2**63:
            st["sum"] = total
    elif kind == "string":
        if nn:
            st["minimum"], st["maximum"] = min(nn), max(nn)
        st["total_length"] = sum(len(v.encode("utf-8")) for v in nn)
    elif kind == "boolean":
        st["true_count"] = sum(1 for v in nn if v)
        st["false_count"] = len(nn) - st["true_count"]
    return st


def check_stats(got, kind, values, label, merged=False):
    """merged: the library builds a stripe's column statistics by merging
    the row-group statistics of the row index (not from the metadata
    section, which the walker grades directly), and its merge carries a
    sum across groups that omitted theirs; so at stripe level every
    figure but the integer sum is compared."""
    want = expected_stats(kind, values)
    if merged:
        want.pop("sum", None)
    for k, v in want.items():
        if got.get(k) != v:
            return f"{label}: {k} is {got.get(k)!r}, expected {v!r}"
    for k in ("minimum", "maximum") + (() if merged else ("sum",)):
        if k in got and k not in want:
            return f"{label}: {k} present, expected absent"
    return None


def _first_difference(got, want, base):
    """Name the first row and column where the rows read after a seek
    differ from the batch, so a wrong position is diagnosable."""
    for i, (g, w) in enumerate(zip(got, want)):
        if g != w:
            for c, (a, b) in enumerate(zip(g, w)):
                if a != b:
                    return f" (first at row {base + i}, column " \
                           f"{COLUMNS[c]})"
            return f" (first at row {base + i})"
    return " (row count differs)"


def main():
    path = sys.argv[1]
    recs = json.load(sys.stdin)
    want = [tuple(r[c] for c in COLUMNS) for r in recs]
    n = len(want)
    problems = []
    with open(path, "rb") as fh:
        reader = pyorc.Reader(fh)
        if str(reader.schema) != ("struct<event_id:string,feed_id:string,"
                                  "seq:bigint,latency_ms:bigint,"
                                  "breach:boolean,note:string>"):
            problems.append(f"schema {reader.schema}")
        if len(reader) != n:
            problems.append(f"row count {len(reader)}")
        if reader.compression != pyorc.CompressionKind.ZLIB:
            problems.append(f"compression {reader.compression}")
        if reader.compression_block_size != 65536:
            problems.append(f"block size {reader.compression_block_size}")
        if reader.row_index_stride != STRIDE:
            problems.append(f"row index stride {reader.row_index_stride}")
        if reader.num_of_stripes != (1 if n else 0):
            problems.append(f"stripes {reader.num_of_stripes}")
        try:
            rows = reader.read()
        except Exception as e:                     # the library's own error
            rows = None
            problems.append(f"reading the file raised {type(e).__name__}: "
                            f"{str(e)[:200]}")
        if rows is not None and rows != want:
            problems.append("values differ from the batch"
                            + _first_difference(rows, want, 0))
        if not problems:
            targets = set()
            for g in range((n + STRIDE - 1) // STRIDE):
                targets |= {g * STRIDE, g * STRIDE + 1, g * STRIDE + 450}
            targets |= {n - 1, n // 2, 1}
            for t in sorted(x for x in targets if 0 <= x < n):
                try:
                    reader.seek(t)
                    tail = reader.read()
                except Exception as e:
                    problems.append(f"seek to row {t} then read raised "
                                    f"{type(e).__name__}: {str(e)[:200]}")
                    break
                if tail != want[t:]:
                    problems.append(f"seek to row {t} then read gives "
                                    f"{len(tail)} rows that differ from "
                                    f"the batch's rows {t} onward"
                                    + _first_difference(tail, want[t:], t))
                    break
        for c in range(7):
            vals = [r[COLUMNS[c - 1]] for r in recs] if c else recs
            p = check_stats(reader[c].statistics, KINDS[c], vals,
                            f"file statistics column {c}")
            if p:
                problems.append(p)
        if n:
            stripe = reader.read_stripe(0)
            for c in range(7):
                vals = [r[COLUMNS[c - 1]] for r in recs] if c else recs
                p = check_stats(stripe[c].statistics, KINDS[c], vals,
                                f"row-group statistics column {c}",
                                merged=True)
                if p:
                    problems.append(p)
    print(json.dumps(problems))


if __name__ == "__main__":
    main()
