"""Held-out batch draws for grading-time conversion.

Every draw stays inside the CONTRACT.md data dictionary: six keys per row,
seq and latency_ms fit signed 64-bit (latency_ms and breach and note may
be null), strings range over the whole stated character domain. The
families are pinned in the maximum day so every regime (empty batch,
single row, the 511/512/513 run boundaries, both signed extremes, a fully
dense batch, a mixed batch with nulls, the 4096-row maximum, every
character class in every string column in every spelling, nulls in
exactly one column, columns null in every row, the bounds of the
encodings themselves: byte run-length runs and literals past their caps,
every RLE v2 bit width, constant runs and arithmetic sequences, and the
row-index regimes: a batch of exactly one 900-row group, a one-row
second group, string streams spanning many compression chunks before a
group boundary, and data streams that start, pause and end exactly at
group boundaries) is exercised regardless of the seed.
"""
import json
import random

REGIONS = ["AMER", "EMEA", "APAC", "LATAM", "MEA"]
NOTES = ["", "ok", "late upstream", "resent by vendor", "ué中文",
         "éclair", "☃ snow day", "backfill → done",
         "\U0001F4E6 box", "b" * 257,
         # escape-requiring content: quotes, backslashes, line breaks,
         # tabs, control characters and embedded U+0000 are all inside
         # the contract's "any Unicode" string domain
         'he said "ok"', "back\\slash", "line1\nline2", "tab\there",
         "\x00", "nul\x00mid", "\r cr", "\x1b[0m esc", "\x1f unit",
         "a/b", "path/to/x", "q/quart",
         "\x08", "\x0c", "back\x08space", "form\x0cfeed",
         "\U0010FFFF", "top\U0010FFFFscalar"]

FAMILIES = ["empty", "single", "b511", "b512", "b513", "extremes",
            "dense", "mixed", "large", "charset", "latnull",
            "allnull", "boolruns", "widths_a", "widths_b", "intpatterns",
            "stride900", "stride901", "wide", "nullends"]

# Pinned batch names exercise every corner of the stated name grammar
# (1 to 64 characters of letters and digits with single interior
# hyphens): single letters of both cases, the 64-character maximum,
# uppercase-only, digit-led, hyphenless and multi-hyphen names.
PINNED_STEMS = {
    "empty": "A",
    "single": "L" + "o" * 61 + "ng",
    "b511": "B511",
    "b512": "run-boundary-512",
    "b513": "RunBoundary513",
    "extremes": "EXTREMES-9",
    "dense": "d",
    "mixed": "0-Mixed-Day",
    "large": "LARGE4096",
    "charset": "u-n-i-c-o-d-e",
    "latnull": "LatencyGaps",
    "allnull": "all-null-1100",
    "boolruns": "BoolRuns",
    "widths_a": "W1to16",
    "widths_b": "w17-to-64",
    "intpatterns": "IntPatterns",
    "stride900": "Group-Edge-900",
    "stride901": "g901",
    "wide": "WIDE-strings-24",
    "nullends": "nullEnds",
}

_ALNUM = ("abcdefghijklmnopqrstuvwxyz"
          "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")


def stem(rng: random.Random) -> str:
    """A random batch name from the whole stated grammar: 1..64 chars of
    letters and digits, optionally with single interior hyphens."""
    n = rng.randrange(1, 65)
    chars = [rng.choice(_ALNUM) for _ in range(n)]
    if n >= 3:
        for i in range(1, n - 1):
            if rng.random() < 0.08 and chars[i - 1] != "-":
                chars[i] = "-"
    return "".join(chars)

# Exactly the contract's 1024-character string maximum, with multibyte
# characters so byte length exceeds character length at the endpoint.
MAX_STR = ("xé中" * 342 + "x")[:1024]
# The same maximum with escape-requiring content: quotes, backslashes,
# an embedded U+0000, a newline and a multibyte character in every
# repeat, so the length endpoint and JSON escaping meet in one string.
MAX_STR_ESC = ('a"b\\c\x00d\n\x08\x0c\U0010FFFFé' * 86)[:1024]

# Every character class of the contract's string domain, in one value
# that the "charset" batch places in EVERY string column: C0 controls
# with U+0000 and every JSON shorthand, quote/backslash/solidus, a
# literal backslash-u text sequence, JSON syntax as plain text, DEL and
# C1 controls (NEL) and no-break space, every UTF-8 encoding-length
# boundary (U+007F|U+0080, U+07FF|U+0800, U+FFFF|U+10000, U+10FFFF),
# both sides of the surrogate gap, the Unicode line and paragraph
# separators, normalization-sensitive sequences, private use, the
# replacement character, astral characters, and ALL 66 noncharacters
# (U+FDD0..U+FDEF and the last two scalars of every plane, ending at
# U+10FFFF).
BMP_NONCHARS = "".join(chr(c) for c in range(0xFDD0, 0xFDF0)) + \
    "\ufffe\uffff"
PLANE_NONCHARS = "".join(chr(p * 0x10000 + e) for p in range(1, 17)
                         for e in (0xFFFE, 0xFFFF))
_CLASS_CORE = (
    "\x00\x01\x1f"
    "\b\f\n\r\t"
    '"\\/'
    "\\u0041"
    '{"event_id":"x","note":null},[1e3,-0,true]:'
    "\x7f\x80\x85\x9f\xa0"
    "\u07ff\u0800"
    "\ud7ff\ue000"
    "\u2028\u2029"
    "e\u0301\u212b\ufb01\u1e9b\u0323"
    "\uf8ff\ufffd"
    + BMP_NONCHARS +
    "\U00010000\U0001F4E6\U000F0000\U0010FFFD"
    + PLANE_NONCHARS
)
# CLASS_A begins with U+FEFF (a value may start with it); CLASS_B has
# whitespace at both edges, ASCII and Unicode, so trimming is caught.
CLASS_A = "\ufeff" + _CLASS_CORE
CLASS_B = " \t\u3000" + _CLASS_CORE + "\u3000\t "
# The UTF-8 byte-length endpoint: 1024 four-byte scalars (4096 bytes).
ASTRAL_MAX = "\U00010000\U0001F4E6\U0010FFFD\U0010FFFF" * 256

# Spellings for the charset batch: "raw" (non-ASCII unescaped, U+2028,
# U+2029 and U+0085 included), "lower" (every non-ASCII character a
# lowercase \uXXXX escape, astral ones as surrogate pairs), "upper"
# (the same with uppercase hex), and "hexall" (EVERY character of every
# key and value a \uXXXX escape with mixed-case hex digits: quote as
# ", backslash as \, solidus, letters and digits included).
# "negzero" writes both integer fields as -0, a legal plain decimal
# spelling of zero that json.dumps never emits.
CHARSET_ROWS = [
    ("raw", CLASS_A, "canon"), ("raw", CLASS_B, "rev"),
    ("lower", CLASS_A, "rev"), ("lower", CLASS_B, "mix"),
    ("upper", CLASS_A, "mix"), ("upper", CLASS_B, "canon"),
    ("hexall", CLASS_A, "canon"), ("hexall", CLASS_B, "rev"),
    ("raw", ASTRAL_MAX, "rev"), ("lower", ASTRAL_MAX, "canon"),
    ("upper", ASTRAL_MAX, "mix"),
    ("negzero", None, "canon"), ("raw", None, "mix"),
]
MIX_KEYS = ["note", "seq", "event_id", "breach", "feed_id", "latency_ms"]


def upper_hex(line):
    """Uppercase the hex digits of every \\uXXXX escape in a JSON text
    (a scanner, so an escaped backslash followed by 'u' is left alone)."""
    out, i = [], 0
    while i < len(line):
        if line[i] == "\\":
            if line[i + 1] == "u":
                out.append("\\u" + line[i + 2:i + 6].upper())
                i += 6
            else:
                out.append(line[i:i + 2])
                i += 2
        else:
            out.append(line[i])
            i += 1
    return "".join(out)


def _mixcase(h):
    return "".join(c.lower() if k % 2 else c for k, c in enumerate(h))


def hexall(s):
    """A JSON string literal with every character \\u-escaped, astral
    characters as surrogate pairs, hex digits in mixed case."""
    units = []
    for ch in s:
        cp = ord(ch)
        if cp > 0xFFFF:
            cp -= 0x10000
            units += [0xD800 + (cp >> 10), 0xDC00 + (cp & 0x3FF)]
        else:
            units.append(cp)
    return '"' + "".join("\\u" + _mixcase("%04X" % u) for u in units) + '"'


def charset_line(rec, mode, order):
    keys = {"canon": KEYS, "rev": REV_KEYS, "mix": MIX_KEYS}[order]
    if mode == "hexall":
        return "{" + ",".join(
            hexall(k) + ":" + (hexall(rec[k]) if isinstance(rec[k], str)
                               else json.dumps(rec[k]))
            for k in keys) + "}"
    obj = {k: rec[k] for k in keys}
    if mode == "raw":
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    if mode == "lower":
        return json.dumps(obj, ensure_ascii=True, separators=(", ", ": "))
    if mode == "upper":
        return upper_hex(json.dumps(obj, ensure_ascii=True,
                                    separators=(",", ": ")))
    if mode == "negzero":
        line = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        assert rec["seq"] == 0 and rec["latency_ms"] == 0
        line = line.replace('"seq":0,', '"seq":-0,', 1)
        return line.replace('"latency_ms":0,', '"latency_ms":-0,', 1)
    raise ValueError(mode)


def serialize_charset(recs):
    """The charset batch as JSON Lines, row i in CHARSET_ROWS[i]'s
    spelling and key order."""
    return "".join(charset_line(r, mode, order) + "\n"
                   for r, (mode, _, order) in zip(recs, CHARSET_ROWS))

# Row serialization styles: the contract's JSON domain includes any key
# order, any valid escaping and any separator spacing, so held-out rows
# rotate through all of them deterministically. REV_KEYS is the exact
# reverse of the data-dictionary order.
KEYS = ["event_id", "feed_id", "seq", "latency_ms", "breach", "note"]
REV_KEYS = list(reversed(KEYS))
# (key order, ensure_ascii, layout, respelling): every dimension
# varies. Layouts: "compact" and "spaced" use json.dumps separators;
# "wild" emits the row token by token with whitespace runs of spaces
# and tabs (up to the stated eight-character maximum) after the
# opening brace, around colons and commas, and before the closing
# brace - every wild row carries a two-space run after its first
# colon and a tab after its second, wild style 7 rows carry a run of
# exactly eight characters, and wild style 4 rows begin and end the
# line with whitespace, so the whole stated whitespace domain is
# exercised deterministically. The respelling step rewrites the
# emitted line into an equally legal JSON spelling that json.dumps
# itself never emits: "solidus" escapes every / as \\/ (the optional
# escaped-solidus spelling), and "uq" respells every q as \\u0071 and
# every o as \\u006F - inside values AND inside keys (every row's
# "seq" key becomes "se\\u0071" and every "note" key "n\\u006Fte"),
# with uppercase hex digits in the o escape, so decoders must handle
# escapes anywhere a string appears, in either hex case. Both rewrites
# touch only characters that occur exclusively inside JSON strings
# (q and o appear in no JSON literal or number), so the line stays
# valid and decodes to the same row.
STYLES = [
    ("canon", False, "compact", None),
    ("rev", True, "wild", "solidus"),
    ("shuf", False, "spaced", "uq"),
    ("canon", True, "compact", "uq"),
    ("rev", False, "wild", "solidus"),
    ("shuf", True, "compact", None),
    ("canon", False, "spaced", "uq"),
    ("rev", True, "wild", None),
]

_WS_POOL = ["", " ", "  ", "\t", " \t"]
_EIGHT = " \t \t \t  "          # exactly eight whitespace characters


def _emit_wild(keys, row, ascii_, rng, style_idx):
    """One row as JSON text with whitespace runs around the structural
    tokens. Values and keys are still produced by json.dumps, so the
    whitespace never lands inside a string."""
    parts = []
    # line-edge whitespace: style 4 lines begin with spaces and end
    # with a mixed space-tab run; style 7 lines begin with a bare tab
    # (a tab directly before the brace) and end with a whitespace run
    # at the stated eight-character bound; other wild lines have no
    # edge whitespace - so space-led, tab-led, tab-trailed and
    # bound-length edge runs are all exercised deterministically
    if style_idx == 4:
        lead, trail = "  ", " \t"
    elif style_idx == 7:
        lead, trail = "\t", _EIGHT
    else:
        lead, trail = "", ""
    parts.append(lead + "{" + rng.choice(_WS_POOL))
    for i, k in enumerate(keys):
        if i:
            parts.append(rng.choice(_WS_POOL) + "," + rng.choice(_WS_POOL))
        if i == 0:
            colon = "  "                    # pinned two-space run
        elif i == 1:
            colon = "\t"                    # pinned tab run
        elif i == 2 and style_idx == 7:
            colon = _EIGHT                  # pinned eight-char run
        else:
            colon = rng.choice(_WS_POOL)
        parts.append(json.dumps(k) + rng.choice(_WS_POOL) + ":" + colon)
        parts.append(json.dumps(row[k], ensure_ascii=ascii_))
    parts.append(rng.choice(_WS_POOL) + "}" + trail)
    return "".join(parts)


def _respell(line, kind):
    if kind == "solidus":
        return line.replace("/", "\\/")
    if kind == "uq":
        return line.replace("q", "\\u0071").replace("o", "\\u006F")
    return line


def serialize(recs, rng, start=None):
    """One batch as JSON Lines text. Rows cycle through STYLES from a
    drawn (or pinned) offset, so every batch of eight or more rows
    contains canonical, fully reversed and shuffled key orders, both
    escaped (\\uXXXX) and raw non-ASCII, compact and spaced separators,
    and the legal respellings json.dumps never emits (escaped solidus,
    \\u-escaped ASCII in values and keys) - deterministically, not by
    accident."""
    if start is None:
        start = rng.randrange(len(STYLES))
    lines = []
    for i, r in enumerate(recs):
        idx = (start + i) % len(STYLES)
        order, ascii_, layout, spell = STYLES[idx]
        if order == "canon":
            keys = KEYS
        elif order == "rev":
            keys = REV_KEYS
        else:
            keys = rng.sample(KEYS, len(KEYS))
        if layout == "wild":
            line = _emit_wild(keys, r, ascii_, rng, idx)
        else:
            seps = (",", ":") if layout == "compact" else (", ", ": ")
            line = json.dumps({k: r[k] for k in keys},
                              ensure_ascii=ascii_, separators=seps)
        lines.append(_respell(line, spell))
    return "".join(line + "\n" for line in lines)


# ---------------------------------------------------------------------
# Format-structure families. Whatever the character content, the ORC
# encodings have bounds of their own: byte run-length runs of 3 to 130
# bytes and literal groups of 1 to 128 bytes (boolean data and every
# validity stream), the RLE v2 table of fixed bit widths, and the run
# shapes an adaptive integer writer picks encodings from. These batches
# drive every one of those bounds in held-out data, deterministically.

# the RLE v2 fixed bit-width table (the widths a packed run may use)
RLE_WIDTHS = list(range(1, 25)) + [26, 28, 30, 32, 40, 48, 56, 64]

# boolean column bytes (8 rows per byte, most significant bit first):
# a run of exactly 130 identical bytes (the run cap), a run of 131 (one
# past it; alternating booleans), a 129-byte stretch with no byte
# repeated (one past the 128-byte literal cap), then a run of true
BOOL_RUN_BYTES = ([0x00] * 130 + [0xAA] * 131 +
                  [(37 * i + 11) & 0xFF for i in range(129)] +
                  [0xFF] * 100)


def bytes_to_bits(bs):
    return [bool(b & (0x80 >> k)) for b in bs for k in range(8)]


def unzigzag(z):
    """The signed value whose zigzag encoding is z."""
    return z >> 1 if z % 2 == 0 else -((z + 1) >> 1)


def _width_block(rng, w, n=512):
    """n values whose zigzag encodings all need exactly w bits (w = 1:
    0 and -1 alternating), with the width's largest, second-largest and
    smallest encodings pinned first."""
    if w == 1:
        return [0 if i % 2 else -1 for i in range(n)]
    lo, hi = 1 << (w - 1), (1 << w) - 1
    zs = [hi, hi - 1, lo] + [rng.randint(lo, hi) for _ in range(n - 3)]
    return [unzigzag(z) for z in zs]


def _int_patterns(rng, lead):
    """Integer run shapes: constant runs of exactly 3, 10 and 11 values
    and of 600, arithmetic sequences longer than 512 values rising and
    falling, rising values with varying deltas, small values with rare
    large outliers, runs at both 64-bit extremes, a sequence ending at
    the maximum and one crossing zero. lead rotates the segment order
    so the two integer columns do not share run boundaries."""
    segs = [
        [7] * 3 + [70001], [-8] * 10 + [-80001],
        [123456] * 11 + [1234567], [-42] * 600 + [-4200001],
        list(range(1000, 1000 + 513)),
        list(range(5000, 5000 - 7 * 520, -7)),
        _cumulative(rng, 300),
        [(2**40 if i % 50 == 49 else rng.randrange(0, 100))
         for i in range(400)],
        [2**63 - 1] * 5 + [-2**63] * 5,
        list(range(2**63 - 20, 2**63)),
        list(range(10, -11, -1)),
    ]
    segs = segs[lead:] + segs[:lead]
    return [v for seg in segs for v in seg]


def _cumulative(rng, n):
    out, v = [], rng.randrange(-10**6, 0)
    for _ in range(n):
        v += rng.randrange(0, 1000)
        out.append(v)
    return out


# ---------------------------------------------------------------------
# Row-index and compression families. The row index groups rows 900 at
# a time and every stream is chunked at 65536 content bytes, so the
# positions an entry records depend on where a group boundary falls
# relative to runs, bytes, bits and chunks. These batches put the
# boundaries in every regime: a batch of exactly one full group, one
# with a one-row second group, string streams so wide that boundaries
# fall deep inside later chunks, and nullable columns whose data streams
# start, pause and end exactly at group boundaries.

STRIDE = 900
BLOCK = 65536
WIDE_ALPHABET = ("abcdefghijklmnopqrstuvwxyz0123456789 -_/"
                 "éü中文☃→\U0001F4E6")


def wide(rng, n):
    """A string of n characters from a mixed one-to-four-byte alphabet,
    so byte length exceeds character length unpredictably."""
    return "".join(rng.choice(WIDE_ALPHABET) for _ in range(n))


def filler(rng: random.Random) -> list:
    """A small extra batch: the drawn day is filled to the contract's
    24-batch maximum, so the stated batch-count bound is tested at its
    endpoint and any smaller fixed capacity or list prefix fails."""
    return [_row(rng) for _ in range(rng.randrange(1, 40))]


def _ident(rng, prefix):
    """Identifier strings exercise the whole string domain, not one
    template: empty, single char, whitespace, multibyte, long, and
    JSON-escape-requiring content (quotes, backslashes, line breaks,
    control characters, embedded U+0000)."""
    kind = rng.randrange(15)
    if kind == 0:
        return ""
    if kind == 1:
        return rng.choice("xy7#é")
    if kind == 2:
        return f"{prefix} with spaces {rng.randrange(100)}"
    if kind == 3:
        return f"{prefix}-ü中文-{rng.randrange(100)}"
    if kind == 4:
        return prefix + "-" + "q" * rng.randrange(50, 300)
    if kind == 5:
        return f'{prefix} "quoted {rng.randrange(100)}"'
    if kind == 6:
        return f"{prefix}\\back\\slash{rng.randrange(100)}"
    if kind == 7:
        return f"{prefix}\nline{rng.randrange(100)}"
    if kind == 8:
        return f"{prefix}\x00nul{rng.randrange(100)}"
    if kind == 9:
        return f"{prefix}\x08bs\x0cff{rng.randrange(100)}"
    if prefix == "H":
        return f"H{rng.randrange(16**10):010x}"
    return f"{rng.choice(REGIONS)}-F{rng.randrange(1000):03d}"


def _row(rng, null_rate=0.15, note_null=0.3):
    lat = None if rng.random() < null_rate else rng.randrange(-10**6, 10**7)
    breach = None if rng.random() < null_rate else rng.random() < 0.3
    note = None if rng.random() < note_null else rng.choice(NOTES)
    return {
        "event_id": _ident(rng, "H"),
        "feed_id": _ident(rng, "F"),
        "seq": rng.randrange(-10**12, 10**15),
        "latency_ms": lat,
        "breach": breach,
        "note": note,
    }


def draw(rng: random.Random, family: str) -> list:
    if family == "empty":
        return []
    if family == "single":
        # one pinned row carrying the whole representational corner in a
        # one-row batch: escape-requiring identifier content and a note
        # that is exactly U+0000 (serialized with reversed key order,
        # \uXXXX escaping and spaced separators - see the pinned style
        # in the holdout test)
        row = _row(rng, null_rate=0)
        row["event_id"] = ('said "hi" \\ a/b é中\n'
                          "\x08\x0c\U0001F4E6\U0010FFFFnl")
        row["feed_id"] = "F-\x00-mid"
        row["note"] = "\x00"
        return [row]
    if family in ("b511", "b512", "b513"):
        # all three run-boundary sizes are pinned, every run
        return [_row(rng) for _ in range(int(family[1:]))]
    if family == "extremes":
        out = [_row(rng, null_rate=0) for _ in range(rng.randrange(40, 90))]
        out[0]["seq"] = -2**63
        out[0]["latency_ms"] = -2**63
        out[1]["seq"] = 2**63 - 1
        out[1]["latency_ms"] = 2**63 - 1
        out[2]["latency_ms"] = -1
        out[3]["seq"] = 0
        # strings at the stated 1024-character maximum in every field
        out[4]["event_id"] = MAX_STR
        out[4]["feed_id"] = MAX_STR[::-1]
        out[4]["note"] = MAX_STR
        # empty, single-character and multibyte identifiers pinned
        # deterministically, not left to probabilistic draws
        out[5]["event_id"] = ""
        out[5]["feed_id"] = ""
        out[6]["event_id"] = "x"
        out[6]["feed_id"] = "\u4e2d\u6587-\u00e9"
        out[7]["note"] = ""
        # escape-requiring content pinned deterministically: quotes,
        # backslashes, line breaks, control characters, embedded U+0000,
        # and the 1024-character maximum built from escape-heavy repeats
        out[8]["event_id"] = 'he said "no \\ way"\n\x00tail'
        out[8]["note"] = "\x00"
        out[9]["event_id"] = MAX_STR_ESC
        out[9]["note"] = MAX_STR_ESC
        out[10]["feed_id"] = "\t\r\n\x00\x1f\x7f\u00e9/"
        out[11]["note"] = "a/b c/d http://x/y"
        # the Unicode scalar upper endpoint U+10FFFF, pinned in both an
        # identifier and a note, so the stated character domain is
        # tested at its last scalar value
        out[12]["event_id"] = "edge\U0010FFFFtop"
        out[12]["note"] = "\U0010FFFF"
        # null only in note here (latency_ms and breach are never null in
        # this batch), so a PRESENT stream is needed for note alone
        out[13]["note"] = None
        # a single-character feed_id, and whitespace-only values in every
        # string column
        out[14]["feed_id"] = "y"
        out[15]["event_id"] = " "
        out[15]["feed_id"] = "\t"
        out[15]["note"] = " \t "
        return out
    if family == "charset":
        # every character class in every string column, in every
        # spelling (see CHARSET_ROWS); integers -0; string values that
        # look like JSON literals; nulls only in breach (row 0), so a
        # PRESENT stream is needed for breach alone
        out = []
        for i, (mode, value, _order) in enumerate(CHARSET_ROWS):
            row = _row(rng, null_rate=0, note_null=0)
            row["breach"] = None if i == 0 else bool(i % 2)
            if value is not None:
                row["event_id"] = row["feed_id"] = row["note"] = value
            elif mode == "negzero":
                row["seq"] = row["latency_ms"] = 0
                row["event_id"], row["feed_id"], row["note"] = \
                    "null", "true", "false"
            else:
                row["event_id"], row["feed_id"], row["note"] = \
                    "0", "-0", "1e3"
            out.append(row)
        return out
    if family == "latnull":
        # nulls only in latency_ms: a PRESENT stream for that column alone
        out = [_row(rng, null_rate=0, note_null=0) for _ in
               range(rng.randrange(30, 70))]
        for i in range(0, len(out), 3):
            out[i]["latency_ms"] = None
        return out
    if family == "dense":
        return [_row(rng, null_rate=0, note_null=0) for _ in
                range(rng.randrange(200, 400))]
    if family == "mixed":
        out = [_row(rng, null_rate=0.25, note_null=0.35) for _ in
               range(rng.randrange(600, 1100))]
        for i in range(0, len(out), 13):
            out[i]["note"] = NOTES[(i // 13) % len(NOTES)]
        return out
    if family == "large":
        # Exactly the contract's 4096-row maximum: the stated row bound is
        # tested at its endpoint, so any smaller fixed capacity fails here.
        return [_row(rng) for _ in range(4096)]
    if family == "allnull":
        # every nullable column null in every row: validity streams of
        # 138 zero bytes (past the 130-byte run cap) and no data at all;
        # both identifier columns empty in every row, so their string
        # data is zero bytes long
        out = [_row(rng, null_rate=0, note_null=0) for _ in range(1100)]
        for r in out:
            r["latency_ms"] = r["breach"] = r["note"] = None
            r["event_id"] = r["feed_id"] = ""
        return out
    if family == "boolruns":
        # breach carries BOOL_RUN_BYTES (no nulls in breach); latency_ms
        # is null only in row 0 (validity: one byte, then 489 bytes of
        # 0xFF); note is null in rows 8..1055 (validity: 0xFF, then 131
        # zero bytes, then 0xFF) and the empty string everywhere else
        bits = bytes_to_bits(BOOL_RUN_BYTES)
        out = [_row(rng, null_rate=0, note_null=0) for _ in bits]
        for i, (r, b) in enumerate(zip(out, bits)):
            r["breach"] = b
            r["note"] = None if 8 <= i < 8 + 131 * 8 else ""
            if i == 0:
                r["latency_ms"] = None
        return out
    if family in ("widths_a", "widths_b"):
        # 4096 rows, no nulls: each 512-row block of seq and of latency_ms
        # holds values that all need one bit width of the RLE v2 table;
        # between the two batches every width appears
        k = 0 if family == "widths_a" else 16
        seq_w, lat_w = RLE_WIDTHS[k:k + 8], RLE_WIDTHS[k + 8:k + 16]
        seq = [v for w in seq_w for v in _width_block(rng, w)]
        lat = [v for w in lat_w for v in _width_block(rng, w)]
        out = [_row(rng, null_rate=0, note_null=0) for _ in range(4096)]
        for r, a, b in zip(out, seq, lat):
            r["seq"], r["latency_ms"] = a, b
        return out
    if family == "intpatterns":
        seq = _int_patterns(rng, 0)
        lat = _int_patterns(rng, 5)
        n = min(len(seq), len(lat))
        out = [_row(rng, null_rate=0, note_null=0) for _ in range(n)]
        for r, a, b in zip(out, seq, lat):
            r["seq"], r["latency_ms"] = a, b
        return out
    if family == "stride900":
        # exactly one full row group: a single index entry, no empty
        # second group
        return [_row(rng) for _ in range(STRIDE)]
    if family == "stride901":
        # one row past the stride: a second group holding one row
        return [_row(rng) for _ in range(STRIDE + 1)]
    if family == "wide":
        # 1801..2599 rows (two boundaries inside the batch) whose three
        # string columns each hold far more than one compression block
        # of bytes before the first boundary, so every string DATA
        # position names a later chunk with a nonzero content offset
        out = [_row(rng, null_rate=0.05, note_null=0.1) for _ in
               range(rng.randrange(1801, 2600))]
        for i, r in enumerate(out):
            r["event_id"] = wide(rng, rng.randrange(300, 1025))
            r["feed_id"] = wide(rng, rng.randrange(64, 1025))
            if r["note"] is not None:
                r["note"] = wide(rng, 1024 if i % 3 else rng.randrange(0, 1025))
        return out
    if family == "nullends":
        # 2000 rows, three groups: latency_ms is null in the whole first
        # group and from the third group's first row on (its data stream
        # begins at boundary 900 and ends at boundary 1800); note is null
        # across the whole second group (no data between two boundaries,
        # so two entries record the same position); breach is null from
        # boundary 900 on (its data ends mid-byte at that boundary)
        out = [_row(rng, null_rate=0, note_null=0) for _ in range(2000)]
        for i, r in enumerate(out):
            if i < STRIDE or i >= 2 * STRIDE:
                r["latency_ms"] = None
            if STRIDE <= i < 2 * STRIDE:
                r["note"] = None
            if i >= STRIDE:
                r["breach"] = None
        return out
    raise ValueError(f"unknown family {family}")
