# Feed event archive: data and grading contract

This file states everything the grader checks. There are no rules outside
this document.

## 1. Input

/app/data/batches/ holds the day's event batches, one JSON Lines file per
batch. Every line is one object with exactly these keys:

- event_id: string, up to 1024 characters, any Unicode, may be empty
- feed_id: string, up to 1024 characters, any Unicode, may be empty
- seq: integer, fits in a signed 64-bit value
- latency_ms: integer fitting a signed 64-bit value, or null
- breach: true, false, or null
- note: string (may be empty, may contain any Unicode, up to 1024
  characters), or null

A batch holds up to 4096 rows, including none; a day holds up to 24
batches, including none. Row order matters. Batch file names are
<name>.jsonl, where <name> is 1 to 64 characters of letters and digits,
optionally with single interior hyphens (a hyphen never starts or ends
the name, and hyphens never touch).

Batch files are UTF-8 with \n line endings, no byte-order mark, no
blank lines, and one row per line; rows end only at \n (U+0085, U+2028
and U+2029 are ordinary characters that may appear unescaped inside
strings, and a string value may begin with U+FEFF). Each line is
ordinary JSON: object
key order is arbitrary; whitespace (spaces and tabs, in runs of up to
eight characters) may appear around any token - after the opening
brace, before or after any colon or comma, before the closing brace,
and at the start or end of a line (a line may begin with a tab
directly before the brace and may end with a whitespace run, spaces
and tabs mixed); and any valid JSON string escaping
may appear - the shorthand escapes \b, \f, \n, \r and \t, \uXXXX
escapes for any character (ASCII included, quote and backslash
included) with uppercase, lowercase or mixed-case hex digits,
surrogate-pair \uXXXX escapes for characters beyond the basic plane,
the optional \/ spelling of the solidus, and escapes inside object
keys as well as values. seq and latency_ms are written as plain
decimal integers (no exponent, no leading plus, no fractional part;
zero may also be written -0). "Any Unicode" means exactly that: string values range
over every Unicode scalar value, from U+0000 up to and including
U+10FFFF (noncharacters included; lone surrogates are not valid
scalar values and never appear) - quotes, backslashes, line breaks,
control characters (backspace and form feed included), embedded
U+0000 and the U+10FFFF endpoint itself, escaped per JSON in the
file and present in the decoded value. Values are graded exactly as
decoded: no Unicode normalization, trimming or other transformation.

## 2. Required artifacts

Two artifacts are graded:

1. For every /app/data/batches/<name>.jsonl, write /app/out/<name>.orc:
   the same rows, in the same order, as an Apache ORC file. /app/out must
   contain exactly one .orc per input batch and nothing else.
2. Write the converter itself to /app/convert.py. Invoked as

       python3 convert.py IN_DIR OUT_DIR

   it must convert every .jsonl batch in IN_DIR (same data dictionary
   and bounds as section 1) into OUT_DIR/<name>.orc files meeting every
   rule of section 3, writing nothing else into OUT_DIR. OUT_DIR is
   provided empty. Any batch count within the day bound is valid, zero
   included: on an empty IN_DIR the converter completes successfully and
   leaves OUT_DIR empty. The grader invokes it on held-out inputs it
   draws at grading time - once with an empty input directory, once with
   a day of intermediate size drawn between 1 and 23 batches, and once
   with a day at the 24-batch maximum that includes a batch at the
   4096-row maximum, so the bounds are exercised at their endpoints and
   the space between them is sampled. The three invocations run in an
   order drawn per submission, and every path an invocation sees - its
   grading directory, its staged copy, its scratch space, and IN_DIR
   and OUT_DIR themselves - is a meaningless random name that varies
   per submission and identifies nothing; the grading root cannot be
   enumerated, and the input and output locations cannot be inferred
   from the script's own path: use exactly the IN_DIR and OUT_DIR you
   are given. Handle every input
   directory on its own terms: nothing may be depended on or inferred
   from the script's own name, path, working directory, or the
   invocation order. Each invocation runs a byte-identical copy of
   your converter, staged under that private path, as
   python3 -I <copy> IN_DIR OUT_DIR: a fresh unprivileged process on a
   Python 3.13 interpreter in isolated mode with its standard library,
   the environment variable PATH set to a directory that does not exist
   (a bare command name does not resolve), no network, no read access
   to /app or to the grader's own files (its tests, its key, and its
   private environment holding the ORC reader), and a 300 second
   budget per invocation. When an invocation exits, every process of
   the grading user that is still running is terminated before its
   output is read. Before the next invocation, the grading user's
   files in the shared temporary locations (/tmp, /var/tmp, /dev/shm,
   /dev/mqueue, /run/lock) and all System V IPC objects are removed,
   and each finished grading directory is made inaccessible to it.
   The maximum day pins one
   batch per family: empty, single row, all three run-boundary row
   counts (511, 512 and 513), signed 64-bit extremes in both integer
   columns (with nulls in note only), fully dense, mixed nulls with
   Unicode notes, the maximum-length batch, a batch with nulls in
   latency_ms only, a character-class batch (with nulls in breach
   only), and batches that drive the bounds of the ORC encodings
   themselves: a batch whose three nullable columns are null in every
   row and whose event_id and feed_id are empty in every row (1100
   rows); a batch whose breach values pack into a byte run
   of exactly 130 identical bytes, a run of 131, and a 129-byte
   stretch with no byte repeated, with validity streams holding runs
   of more than 130 identical bytes of both kinds (its note is null or
   empty in every row); two 4096-row
   batches in which each 512-row block of seq and of latency_ms holds
   values whose zigzag encodings all need the same number of bits,
   covering every width of the RLE v2 fixed bit-width table (1 to 24,
   26, 28, 30, 32, 40, 48, 56, 64); and a batch of integer run shapes
   in both integer columns (constant runs of 3, 10, 11 and 600
   values, arithmetic sequences of more than 512 values rising and
   falling, rising values with varying steps, small values with rare
   large outliers, and repeats of both signed 64-bit extremes). In the
   character-class batch every string column holds
   values containing every character class of section 1 - all 66
   noncharacters, every UTF-8 encoding-length boundary (U+007F/U+0080,
   U+07FF/U+0800, U+FFFF/U+10000, U+10FFFF), U+0000 and other
   controls, quote, backslash and solidus, U+0085, U+2028 and U+2029,
   a leading U+FEFF, leading and trailing whitespace,
   normalization-sensitive sequences, private-use and astral
   characters, and JSON syntax as text - each written in four
   spellings: non-ASCII characters unescaped (U+0085, U+2028 and
   U+2029 included), lowercase-hex \uXXXX escapes, uppercase-hex
   \uXXXX escapes, and every character \u-escaped with mixed-case hex
   digits, keys included. The same
   batch carries 1024-character strings of four-byte characters (4096
   UTF-8 bytes) in every string column, both integers written -0, and
   string values spelled like JSON literals. Identifier strings range over the whole stated
   string domain, not a fixed shape, including strings at the
   1024-character maximum and strings containing quotes, backslashes,
   line breaks, control characters and embedded U+0000; rows are
   serialized with rotating key order (the data-dictionary order, its
   exact reverse, and shuffled), rotating \uXXXX escaping, rotating
   whitespace layouts (compact, single-space, and multi-space and tab
   runs up to the stated eight-character maximum, including lines that
   begin and end with whitespace), and rotating legal respellings (the
   escaped solidus \/, and \u-escaped ASCII in values and in keys with
   both hex cases), with the shorthand \b and \f escapes and
   surrogate-pair escapes pinned in the drawn strings, so nothing may
   assume a canonical line shape; and
   batch names span the whole stated name grammar, including
   single-character and 64-character names, uppercase, digit-led and
   hyphenless names. The draws are seeded by a keyed digest of
   your submitted converter; the key exists only in the grading image,
   so the draws are deterministic for a given submission but cannot be
   predicted or steered from inside the environment.

## 3. What the grader accepts

Each submitted file is read with the ORC reader of Apache Arrow 25.0.1
(pyarrow.orc), which is not present in this environment. The file must:

- open without error, and report exactly one stripe (no stripe when the
  batch is empty) and the batch's row count;
- carry exactly this schema, in this order:
  struct<event_id:string,feed_id:string,seq:bigint,latency_ms:bigint,breach:boolean,note:string>
- decode to exactly the batch's values, row for row, nulls included.

In addition, the grader walks the container bytes directly and rejects the
file unless all of the following hold:

- compression is NONE and the postscript declares format version 0.12;
- the file metadata section is empty (metadataLength 0);
- there are no row indexes: rowIndexStride is 0, the stripe's indexLength
  is 0, and no ROW_INDEX stream appears;
- headerLength is 3, the stripe starts at offset 3, the declared stream
  lengths sum exactly to the stripe's dataLength, and contentLength equals
  3 + dataLength + stripe footer length (an empty batch has contentLength
  3 and no stripe);
- column encodings are DIRECT_V2 for the bigint and string columns and
  DIRECT for the struct root and the boolean column;
- a column carries a PRESENT stream exactly when it has at least one null
  in that batch; string columns carry DATA and LENGTH streams, other
  columns DATA only, and the struct root carries no stream.

The walk decodes the metadata messages by protobuf wire rules: any valid
serialization is accepted, and only the decoded values are graded. A
repeated numeric field may be packed, unpacked, or a mix, and its values
concatenate in order; a singular field that occurs more than once decodes
to its last occurrence (standard protobuf merge semantics); unknown
fields are skipped without effect, including well-formed group fields
and any occurrence of a known field number with a mismatched wire type
(standard unknown-field handling).

Each of these rules applies to every graded file, whether it sits under
/app/out or was produced by /app/convert.py on the held-out batches.

## 4. Environment

Python 3.13 and its standard library. No network access. No ORC,
Arrow, Avro or Parquet software is installed here, and none can be
installed.
How you produce the bytes is up to you; the grader reads /app/out and
runs /app/convert.py exactly as section 2 states, and nothing else.
