"""Verifier for orc-feed-archive.

Two graded artifacts, both stated in /app/CONTRACT.md:

1. /app/out - the archived day: exactly one .orc per input batch and
   nothing else. Every file is read with the pinned Apache Arrow ORC
   reader and compared value for value against the batch, then walked
   structurally by the independent parser in walker.py.
2. /app/convert.py - the converter itself. It is executed in fresh
   unprivileged processes on the system interpreter in isolated mode,
   on held-out batches drawn at grading time from the
   contract's data dictionary, seeded by a keyed digest of the submitted
   converter (the key lives only in this image). Its outputs face the
   same reader and structural checks, so precomputed archive bytes
   cannot substitute for a working converter.
"""
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
import pyarrow.orc as orc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_holdout  # noqa: E402
import walker       # noqa: E402

OUT_DIR = Path(os.environ.get("ORC_OUT", "/app/out"))
CONVERT = Path(os.environ.get("ORC_CONVERT", "/app/convert.py"))
BATCH_DIR = Path(os.environ.get("ORC_BATCHES", "/tests/data/batches"))
WORK = Path(os.environ.get("ORC_WORK", "/work"))
SYS_PYTHON = os.environ.get("ORC_SYS_PYTHON", "/usr/local/bin/python3")
DROP = os.environ.get("ORC_DROP", "1") == "1"
CONVERT_TIMEOUT = 300        # seconds per converter invocation (CONTRACT.md 2)
SANDBOX_UID = 12000          # the unprivileged grading user

COLUMNS = ["event_id", "feed_id", "seq", "latency_ms", "breach", "note"]
BATCHES = sorted(p.stem for p in BATCH_DIR.glob("*.jsonl"))


def load_batch(stem):
    recs = []
    with open(BATCH_DIR / f"{stem}.jsonl", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                recs.append(json.loads(line))
    return recs


def check_reader(path, recs, label):
    """The pinned Apache Arrow ORC reader must see exactly the rows."""
    f = orc.ORCFile(path)
    assert f.nrows == len(recs), f"{label}: row count {f.nrows}"
    schema = f.schema
    assert schema.names == COLUMNS, f"{label}: field names {schema.names}"
    kinds = [str(schema.field(c).type) for c in COLUMNS]
    assert kinds == ["string", "string", "int64", "int64", "bool", "string"], \
        f"{label}: types {kinds}"
    got = f.read().to_pydict()
    for col in COLUMNS:
        want = [r[col] for r in recs]
        assert got[col] == want, f"{label}: column {col} differs"


def check_structure(path, recs):
    """Independent walk of the container against the stated shape rules."""
    nulls = [any(r[c] is None for r in recs) for c in COLUMNS]
    walker.walk(Path(path).read_bytes(), nulls, len(recs))


def raw_fields(line):
    """Split one JSON object line into {decoded key: raw value token},
    so the tests can see exactly how each value was spelled."""
    n = len(line)

    def ws(i):
        while i < n and line[i] in " \t":
            i += 1
        return i

    def string_end(i):                      # line[i] is the opening quote
        j = i + 1
        while line[j] != '"':
            j += 2 if line[j] == "\\" else 1
        return j + 1

    i = ws(0)
    assert line[i] == "{"
    i = ws(i + 1)
    out = {}
    while True:
        j = string_end(i)
        key = json.loads(line[i:j])
        i = ws(j)
        assert line[i] == ":"
        i = ws(i + 1)
        if line[i] == '"':
            j = string_end(i)
        else:
            j = i
            while j < n and line[j] not in ",} \t":
                j += 1
        out[key] = line[i:j]
        i = ws(j)
        if line[i] == "}":
            return out
        assert line[i] == ","
        i = ws(i + 1)


def _bool_bytes(bits):
    """Booleans packed 8 per byte, most significant bit first (the ORC
    boolean and validity layout before byte run-length encoding)."""
    out = bytearray((len(bits) + 7) // 8)
    for i, b in enumerate(bits):
        if b:
            out[i // 8] |= 0x80 >> (i % 8)
    return bytes(out)


def _runs(seq):
    """Maximal runs of equal consecutive items, as (item, length)."""
    out, i = [], 0
    while i < len(seq):
        j = i
        while j < len(seq) and seq[j] == seq[i]:
            j += 1
        out.append((seq[i], j - i))
        i = j
    return out


def _delta_runs(vals):
    """Maximal arithmetic runs, as (delta, length in values)."""
    out, i = [], 0
    while i + 1 < len(vals):
        d = vals[i + 1] - vals[i]
        j = i + 1
        while j + 1 < len(vals) and vals[j + 1] - vals[j] == d:
            j += 1
        out.append((d, j - i + 1))
        i = j
    return out


def _zigzag(v):
    return 2 * v if v >= 0 else -2 * v - 1


def check_output_set(out_dir, stems, label):
    """Exactly one .orc per batch and nothing else in the output dir."""
    entries = sorted(p.name for p in Path(out_dir).iterdir())
    want = sorted(f"{s}.orc" for s in stems)
    assert entries == want, \
        f"{label}: output entries {entries} != required {want}"


def test_input_coverage():
    """The batch families the contract talks about are all present."""
    assert len(BATCHES) == 12
    sizes = {s: len(load_batch(s)) for s in BATCHES}
    assert min(sizes.values()) == 0                      # empty batch
    assert 1 in sizes.values()                           # single row
    assert 512 in sizes.values()                         # exact run boundary
    assert max(sizes.values()) >= 4000                   # multi-run batch
    all_rows = [r for s in BATCHES for r in load_batch(s)]
    for col in ("seq", "latency_ms"):
        vals = [r[col] for r in all_rows if r[col] is not None]
        assert -2**63 in vals, f"{col} must hit the signed-64 minimum"
        assert 2**63 - 1 in vals, f"{col} must hit the signed-64 maximum"
        assert any(v < 0 for v in vals), f"{col} must include negatives"
    assert any(r["latency_ms"] is None for r in all_rows)
    assert any(r["breach"] is None for r in all_rows)
    assert any(r["note"] == "" for r in all_rows)
    assert any(r["note"] and any(ord(c) > 127 for c in r["note"])
               for r in all_rows if r["note"])
    dense = [s for s in BATCHES if load_batch(s) and not any(
        r[c] is None for r in load_batch(s) for c in COLUMNS)]
    assert dense, "one batch must be entirely free of missing values"


def test_output_set():
    assert OUT_DIR.is_dir(), f"{OUT_DIR} is missing"
    check_output_set(OUT_DIR, BATCHES, "archive")


@pytest.mark.parametrize("stem", BATCHES)
def test_output_exists(stem):
    path = OUT_DIR / f"{stem}.orc"
    assert path.is_file(), f"{path} is missing"
    assert path.stat().st_size > 0, f"{path} is empty"


@pytest.mark.parametrize("stem", BATCHES)
def test_reader_values(stem):
    check_reader(OUT_DIR / f"{stem}.orc", load_batch(stem), stem)


@pytest.mark.parametrize("stem", BATCHES)
def test_structure(stem):
    check_structure(OUT_DIR / f"{stem}.orc", load_batch(stem))


def test_converter_present():
    assert CONVERT.is_file(), f"{CONVERT} is missing"
    assert CONVERT.stat().st_size > 0, f"{CONVERT} is empty"


def _sandbox_pids():
    """Live (not yet exited) processes whose uid is the sandbox uid."""
    pids = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/status") as f:
                status = dict(line.split(":", 1) for line in f if ":" in line)
        except OSError:
            continue
        uids = status.get("Uid", "").split()
        state = status.get("State", "").split()[:1]
        if str(SANDBOX_UID) in uids and state != ["Z"]:
            pids.append(int(pid))
    return pids


KILL_ALL = "import os\ntry:\n    os.kill(-1, 9)\nexcept OSError:\n    pass\n"


def _kill_sandbox_processes():
    """Terminate every process of the sandbox uid. Runs before each
    converter invocation and again as soon as the invocation exits,
    before its output is read. kill(-1), sent as the sandbox uid,
    reaches all of that uid's processes in one pass that a forking or
    detached process cannot slip through (a /proc listing alone can be
    outrun by a process that keeps re-forking). It is sent at least
    once, then again until /proc shows none of them still running."""
    for _ in range(100):
        subprocess.run(["/usr/bin/setpriv", f"--reuid={SANDBOX_UID}",
                        f"--regid={SANDBOX_UID}", "--clear-groups",
                        "--no-new-privs", SYS_PYTHON, "-I", "-c", KILL_ALL],
                       capture_output=True, cwd="/")
        time.sleep(0.02)
        if not _sandbox_pids():
            return
    pytest.fail("processes of the grading user could not be terminated")


def _run_convert(tag, batches, texts, case_name, script_name,
                 scratch_name, in_name, out_name):
    """Stage the converter and run it once on the given batches. batches
    maps stem -> records (the expected values); texts maps stem -> the
    JSON Lines text actually written, serialized by gen_holdout.serialize
    with rotating key order, escaping and spacing, so a converter that
    assumes one canonical line shape fails while expectations stay
    independent of the serialization. An empty dict means an empty input
    directory. The process is fresh and unprivileged, runs the system
    interpreter in isolated mode, and cannot read /app or the grader's
    files (tests, key, private venv with the ORC reader).
    The grading directory, the staged file name and the scratch
    directory name are all meaningless per-submission-random names: no
    path component reveals which graded day an invocation is, and
    nothing may depend on or infer anything from the script's path or
    working directory. tag appears only in failure messages."""
    case = WORK / case_name
    shutil.rmtree(case, ignore_errors=True)
    in_dir = case / in_name
    out_dir = case / out_name
    scratch = case / scratch_name
    in_dir.mkdir(parents=True)
    out_dir.mkdir()
    scratch.mkdir()
    staged = case / script_name
    shutil.copyfile(CONVERT, staged)
    for p in (case, in_dir):
        os.chmod(p, 0o755)
    for p in (out_dir, scratch):
        os.chmod(p, 0o777)
    os.chmod(staged, 0o644)

    for name in batches:
        with open(in_dir / f"{name}.jsonl", "w", encoding="utf-8") as f:
            f.write(texts[name])
        os.chmod(in_dir / f"{name}.jsonl", 0o644)

    cmd = [SYS_PYTHON, "-I", str(staged), str(in_dir), str(out_dir)]
    if DROP:
        cmd = ["/usr/bin/setpriv", f"--reuid={SANDBOX_UID}",
               f"--regid={SANDBOX_UID}", "--clear-groups",
               "--no-new-privs"] + cmd
        _kill_sandbox_processes()
        # world-writable temp locations and System V IPC: nothing the
        # grading user left behind reaches the next invocation
        for d in ("/tmp", "/var/tmp", "/dev/shm", "/dev/mqueue",
                  "/run/lock"):
            if os.path.isdir(d):
                subprocess.run(["find", d, "-user", str(SANDBOX_UID),
                                "-delete"],
                               capture_output=True)
        try:
            subprocess.run(["ipcrm", "--all"], capture_output=True)
        except OSError:
            pass
    env = {"PATH": str(case / "nobin"), "HOME": str(scratch),
           "PYTHONDONTWRITEBYTECODE": "1"}
    proc = subprocess.Popen(cmd, env=env, cwd=str(scratch),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, start_new_session=True)
    try:
        _, err = proc.communicate(timeout=CONVERT_TIMEOUT)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, 9)
        proc.communicate()
        pytest.fail(f"convert.py ({tag}) exceeded {CONVERT_TIMEOUT} s")
    finally:
        try:
            os.killpg(proc.pid, 9)     # the converter's process group
        except (ProcessLookupError, PermissionError, OSError):
            pass
        if DROP:
            # detached processes too: nothing of the grading user is
            # still running when the output is read
            _kill_sandbox_processes()
    assert proc.returncode == 0, \
        f"convert.py failed on held-out batches ({tag})\n{err[-800:]}"

    check_output_set(out_dir, sorted(batches), f"holdout {tag}")
    for name in sorted(batches):
        path = out_dir / f"{name}.orc"
        check_reader(path, batches[name], name)
        check_structure(path, batches[name])
    # lock the finished case away from the sandbox uid, so no later
    # invocation can read or reuse this one's inputs, outputs or scratch
    os.chmod(case, 0o700)


def test_holdout_conversion():
    """Run the submitted converter on held-out batches drawn at grading
    time. The draws are seeded by a keyed digest of the converter: the
    key exists only in the verifier image, so the draws are deterministic
    per submission but cannot be predicted during the episode. Three
    invocations: one with an empty input directory (zero batches is a
    valid day), one with a day of intermediate size drawn between 1 and
    23 batches, and one holding exactly the contract's 24-batch maximum
    with every family pinned - all three run-boundary sizes, a batch at
    the 4096-row maximum, strings at the 1024-character maximum, every
    character class in every string column in every spelling, nulls in
    exactly one column for each nullable column, columns null in every
    row, and batches that drive the encodings' own bounds (byte
    run-length runs and literals past their caps, every RLE v2 bit
    width, constant runs and arithmetic sequences) - so the stated
    bounds are tested at their endpoints and a converter special-cased
    to the disclosed counts fails the drawn one."""
    if not CONVERT.is_file():
        pytest.fail(f"{CONVERT} is missing")
    salt = (Path(__file__).parent / "salt.txt").read_bytes()
    digest = hashlib.sha256(salt + CONVERT.read_bytes()).hexdigest()
    rng = random.Random(int(digest, 16))

    def names(n):
        # grading dir, staged script, scratch dir, input dir, output
        # dir: all meaningless random names, so no path reveals which
        # graded day this is and the input and output locations cannot
        # be inferred from the script's own path
        return (f"d{rng.getrandbits(32):08x}",
                f"c{rng.getrandbits(32):08x}.py",
                f"s{rng.getrandbits(32):08x}",
                f"i{rng.getrandbits(32):08x}",
                f"o{rng.getrandbits(32):08x}")

    empty_names = names(0)

    batches = {}
    for fam in gen_holdout.FAMILIES:           # pinned name-grammar corners
        batches[gen_holdout.PINNED_STEMS[fam]] = gen_holdout.draw(rng, fam)
    while len(batches) < 24:                   # fill to the stated 24-batch
        nm = gen_holdout.stem(rng)             # maximum with names drawn
        if nm not in batches:                  # from the whole grammar
            batches[nm] = gen_holdout.filler(rng)
    single_stem = gen_holdout.PINNED_STEMS["single"]
    charset_stem = gen_holdout.PINNED_STEMS["charset"]
    texts = {name: gen_holdout.serialize_charset(recs)
             if name == charset_stem else gen_holdout.serialize(
                 recs, rng,
                 # the one-row batch is pinned to the reversed, \uXXXX-
                 # escaped, space-separated style, so the fully
                 # non-canonical line shape appears in a one-row batch
                 # on every run, not by accident
                 start=1 if name == single_stem else None)
             for name, recs in batches.items()}

    # machine checks: the serialized held-out day covers the whole
    # representational domain on every run, whatever the digest
    st = texts[single_stem]
    for token in ('\\u4e2d', '\\u0000', '\\"', '\\\\', '\\n',
                  '\\b', '\\f', '\\ud83d', '\\udce6',  # shorthand +
                  ':  ', '\t'):                        # surrogate pair,
        assert token in st, f"single-batch pin missing {token!r}"
    # first quoted token is the first key: fully reversed order
    assert st.split('"')[1] == "note", "single row must be reversed order"
    dense = texts[gen_holdout.PINNED_STEMS["dense"]]
    for token in ('\n{"event_id"',       # canonical compact rows
                  '": ',                 # spaced layout rows
                  '\n  {',               # space-led lines
                  '\n\t{',               # tab-led lines
                  '\t\n',                # tab at line end
                  ' \t \t \t  \n',       # edge run at the 8-char bound
                  ' \t \t \t  ',         # the eight-char run, at bound
                  '\\u0071',             # \u-escaped ASCII value/key
                  '"se\\u0071"',         # escaped "seq" key
                  '\\u006F'):            # uppercase-hex escaped o
        assert token in dense, f"style rotation missing {token!r}"
    all_vals = [r[c] for recs in batches.values() for r in recs
                for c in ("event_id", "feed_id", "note")
                if isinstance(r[c], str)]
    for ch, what in [("\x00", "embedded U+0000"), ('"', "quote"),
                     ("\\", "backslash"), ("\n", "newline"),
                     ("\x08", "backspace"), ("\x0c", "form feed"),
                     ("/", "solidus")]:
        assert any(ch in v for v in all_vals), f"no drawn string has {what}"
    assert any(len(v) == 1024 and "\x00" in v for v in all_vals), \
        "no escape-heavy string at the 1024-character maximum"
    # the escaped-solidus spelling is pinned on every run: the single
    # row carries a / and its pinned style respells it \/
    assert "\\/" in st, "single-batch pin missing escaped solidus"
    for token in ("\\udbff", "\\udfff"):   # U+10FFFF as a surrogate pair
        assert token in st, f"single-batch pin missing {token!r}"
    assert any(chr(0x10FFFF) in v for v in all_vals), \
        "no drawn string reaches the U+10FFFF scalar endpoint"

    # every character class, in every string column, in every spelling:
    # each charset line decodes to its record, and each string value's
    # raw token is exactly the stated spelling (raw, lowercase hex,
    # uppercase hex, fully escaped mixed-case hex) of a value that holds
    # all 66 noncharacters, every UTF-8 length boundary, controls,
    # separators, astral characters and the rest of CLASS_A / CLASS_B
    spell = {"raw": lambda v: json.dumps(v, ensure_ascii=False),
             "lower": lambda v: json.dumps(v, ensure_ascii=True),
             "upper": lambda v: gen_holdout.upper_hex(
                 json.dumps(v, ensure_ascii=True)),
             "hexall": gen_holdout.hexall}
    cs_lines = texts[charset_stem].split("\n")[:-1]   # rows end at \n only
    cs_recs = batches[charset_stem]
    assert len(cs_lines) == len(cs_recs) == len(gen_holdout.CHARSET_ROWS)
    seen = set()
    for line, rec, (mode, value, _) in zip(cs_lines, cs_recs,
                                           gen_holdout.CHARSET_ROWS):
        assert json.loads(line) == rec, "charset line does not decode"
        toks = raw_fields(line)
        for col in ("event_id", "feed_id", "note"):
            if value is not None:
                assert rec[col] == value
                assert toks[col] == spell[mode](value), \
                    f"charset {col} not spelled {mode}"
                seen.add((col, mode, value))
    for col in ("event_id", "feed_id", "note"):
        for mode in spell:
            for value in (gen_holdout.CLASS_A, gen_holdout.CLASS_B):
                assert (col, mode, value) in seen, (col, mode)
        for mode in ("raw", "lower", "upper"):
            assert (col, mode, gen_holdout.ASTRAL_MAX) in seen, (col, mode)
    nonchars = gen_holdout.BMP_NONCHARS + gen_holdout.PLANE_NONCHARS
    assert len(set(nonchars)) == 66 and set(nonchars) <= set(
        gen_holdout.CLASS_A) and set(nonchars) <= set(gen_holdout.CLASS_B)
    for ch in "\x00\x7f\x80\u07ff\u0800\ud7ff\ue000\uffff\U00010000" \
              "\U0010FFFF\u2028\u2029\x85":
        assert ch in gen_holdout.CLASS_A and ch in gen_holdout.CLASS_B
    assert len(gen_holdout.ASTRAL_MAX) == 1024 and \
        len(gen_holdout.ASTRAL_MAX.encode("utf-8")) == 4096
    assert gen_holdout.CLASS_A[0] == "\ufeff"             # BOM leads
    assert gen_holdout.CLASS_B[0].isspace() and \
        gen_holdout.CLASS_B[-1].isspace()                  # edge spaces
    zero = [raw_fields(ln) for ln in cs_lines if ":-0," in ln]
    assert any(t["seq"] == "-0" and t["latency_ms"] == "-0" for t in zero)
    looks = {(r["event_id"], r["feed_id"], r["note"]) for r in cs_recs}
    assert ("null", "true", "false") in looks and ("0", "-0", "1e3") in looks
    # per string column: empty, single-character, whitespace-only and
    # 1024-character values, deterministically
    for col in ("event_id", "feed_id", "note"):
        vals = [r[col] for recs in batches.values() for r in recs
                if isinstance(r[col], str)]
        assert "" in vals and any(len(v) == 1 for v in vals), col
        assert any(v.isspace() for v in vals if v), col
        assert any(len(v) == 1024 for v in vals), col
    # a PRESENT stream needed for exactly one nullable column, per column
    for col in ("latency_ms", "breach", "note"):
        assert any(
            any(r[col] is None for r in recs) and not any(
                r[c] is None for r in recs for c in COLUMNS if c != col)
            for recs in batches.values()), f"no batch with nulls in {col} only"
    # format-structure families: every bound of the encodings themselves
    # is driven on every run - byte run-length runs at and past the
    # 130-byte cap, a literal stretch past the 128-byte cap, validity
    # streams with long runs of both kinds, columns null in every row,
    # every width of the RLE v2 table, and the integer run shapes
    pin = gen_holdout.PINNED_STEMS
    allnull = batches[pin["allnull"]]
    assert len(allnull) >= 131 * 8 and all(
        r[c] is None for r in allnull
        for c in ("latency_ms", "breach", "note")), "all-null batch"
    assert all(r["event_id"] == r["feed_id"] == "" for r in allnull)
    boolruns = batches[pin["boolruns"]]
    data = _bool_bytes([r["breach"] for r in boolruns])
    assert all(r["breach"] is not None for r in boolruns)
    runs = _runs(data)
    assert (0x00, 130) in runs, "no byte run of exactly 130"
    assert any(n == 131 for _, n in runs), "no byte run of 131"
    stretch = longest = 1
    for a, b in zip(data, data[1:]):
        stretch = stretch + 1 if a != b else 1
        longest = max(longest, stretch)
    assert longest >= 129, "no 129-byte stretch without a repeated byte"
    for col, byte in (("latency_ms", 0xFF), ("note", 0x00)):
        present = _runs(_bool_bytes([r[col] is not None for r in boolruns]))
        assert any(b == byte and n >= 131 for b, n in present), col
    notes = {r["note"] for r in boolruns}
    assert notes == {None, ""}, "note: nulls and empty strings only"
    widths = set()
    for fam in ("widths_a", "widths_b"):
        recs = batches[pin[fam]]
        assert len(recs) == 4096
        for col in ("seq", "latency_ms"):
            vals = [r[col] for r in recs]
            for k in range(0, 4096, 512):
                need = {max(_zigzag(v).bit_length(), 1)
                        for v in vals[k:k + 512]}
                assert len(need) == 1, (fam, col, k)
                widths |= need
    assert widths == set(gen_holdout.RLE_WIDTHS), "RLE v2 width table"
    for col in ("seq", "latency_ms"):
        vals = [r[col] for r in batches[pin["intpatterns"]]]
        lens = {n for _, n in _runs(vals)}
        assert {3, 10, 11} <= lens and max(lens) >= 600, col
        deltas = _delta_runs(vals)
        assert any(d == 1 and n >= 513 for d, n in deltas), col
        assert any(d < 0 and n >= 513 for d, n in deltas), col
        assert (2**63 - 1, 5) in _runs(vals) and (-2**63, 5) in _runs(vals)
        assert 2**40 in vals and sum(0 <= v < 100 for v in vals) >= 300
    drawn_names = names(1)

    mid = {}
    n_mid = rng.randrange(1, 24)               # unpredictable middle size
    while len(mid) < n_mid:
        nm = gen_holdout.stem(rng)
        if nm not in mid:
            mid[nm] = gen_holdout.filler(rng)
    mid_texts = {name: gen_holdout.serialize(recs, rng)
                 for name, recs in mid.items()}
    mid_names = names(2)

    # the three graded days run in an order drawn per submission, and
    # every path they see is a meaningless random name, so neither the
    # order nor any path component identifies a scenario
    runs = [("empty-day", {}, {}, empty_names),
            ("drawn", batches, texts, drawn_names),
            ("mid-day", mid, mid_texts, mid_names)]
    rng.shuffle(runs)
    for tag, b, t, (dn, sn, cn, inn, on) in runs:
        _run_convert(tag, b, t, dn, sn, cn, inn, on)
