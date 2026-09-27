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
   contain exactly one .orc per input batch and nothing else, each a
   regular file (no symbolic links, directories or special files).
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
   private environment holding the ORC readers), and a 300 second
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
   only), batches that drive the bounds of the ORC encodings
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
   large outliers, and repeats of both signed 64-bit extremes); and
   batches that drive the row index and the compression chunking of
   section 3: a batch of exactly 900 rows (one full row group), a
   batch of 901 rows (a second group holding one row), a wide-string
   batch of 1801 to 2599 rows whose three string columns each hold
   more than four times the compression block size of bytes (more
   than 262144 bytes) before the first group boundary, and a 2000-row
   batch whose nullable columns are
   null across whole row groups (latency_ms null in the first group
   and from the third group on, note null across the second group,
   breach null from the second group on). In the
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

Each submitted file is read with two ORC readers, neither of which is
present in this environment: the ORC reader of Apache Arrow 25.0.1
(pyarrow.orc) and the Apache ORC C++ library (pyorc 0.11.0). The file
must:

- open without error in both, and report exactly one stripe (no stripe
  when the batch is empty), the batch's row count, ZLIB compression, a
  compression block size of 65536 and a row index stride of 900;
- carry exactly this schema, in this order:
  struct<event_id:string,feed_id:string,seq:bigint,latency_ms:bigint,breach:boolean,note:string>
- decode to exactly the batch's values, row for row, nulls included,
  in both readers;
- report, through the ORC C++ reader, column statistics that agree
  with the batch: at file level every figure the statistics rules
  below define (numberOfValues, hasNull, minimum, maximum, sum or
  count, with the same presence rules), and at stripe level - which
  that reader builds by merging the row-group entries of the row
  index, not from the metadata section - every figure but the integer
  sum;
- seek: for every row group of the file (rows are grouped 900 at a
  time, see below) the grader positions the ORC C++ reader at the
  group's first row by row number - a seek the reader performs through
  the row index - and reads to the end of the file; the rows read must
  be exactly the batch's rows from that row on. The grader also seeks
  to rows inside groups (a seek to the group's entry followed by a
  skip) and to the last row.

In addition, the grader walks the container bytes directly and rejects
the file unless all of the following hold.

Compression:

- the postscript declares compression ZLIB, compressionBlockSize 65536,
  format version 0.12, a writerVersion of at least 1 (the readers
  discard the string and boolean statistics of a file that declares an
  older writer; 6 is a fine choice) and the magic string ORC (field
  8000); the postscript itself is never compressed;
- every stream (index and data alike), the stripe footer, the file
  footer and the metadata section are compressed, each as a sequence
  of chunks: a chunk is a three-byte little-endian header holding
  (length << 1) | original, followed by exactly that many bytes holding
  one complete raw DEFLATE stream (RFC 1951; no zlib or gzip wrapper)
  that decompresses to between 1 and 65536 bytes. The original flag is
  never set: no chunk is stored uncompressed. A stream of no bytes has
  no chunks. All lengths in the container (stream lengths, indexLength,
  dataLength, the stripe footer length, footerLength, metadataLength)
  are compressed lengths.

Layout and accounting:

- headerLength is 3 and the stripe starts at offset 3; an empty batch
  has no stripe and contentLength 3;
- otherwise the stripe consists of its index streams, then its data
  streams, then its stripe footer; its numberOfRows is the batch's row
  count (as is the footer's), and contentLength equals
  3 + indexLength + dataLength + stripe footer length; the metadata
  section begins at contentLength, followed by the footer, the
  postscript and its length byte;
- the stripe footer lists the streams in the order they are laid out,
  and they tile the index and data areas exactly: the index area comes
  first - for each column 0 to 6 in order, its ROW_INDEX stream,
  followed for the string and bigint columns (columns 1, 2, 3, 4 and
  6) by its BLOOM_FILTER_UTF8 stream, twelve streams in all, whose
  lengths sum to indexLength - then the data streams in any order
  (their lengths sum to dataLength); no other ROW_INDEX or
  BLOOM_FILTER_UTF8 stream appears;
- column encodings are DIRECT_V2 for the bigint and string columns and
  DIRECT for the struct root and the boolean column; the encodings of
  the five bloom-filter columns declare bloomEncoding 1 (UTF8), the
  others declare none;
- a column carries a PRESENT stream exactly when it has at least one
  null in that batch; string columns carry DATA and LENGTH streams,
  other columns DATA only; the struct root carries no data stream;
  every stream belongs to one of the seven columns (0 to 6); no
  stream kind other than ROW_INDEX, BLOOM_FILTER_UTF8, PRESENT, DATA
  and LENGTH appears, and no column carries the same stream kind
  twice. A stream is listed
  in the stripe footer even when it holds no bytes (length 0, no
  chunks: the DATA stream of a column null in every row, the DATA
  stream of a string column empty in every row); its positions are
  then 0, 0.

Row index (rowIndexStride 900):

- the footer declares rowIndexStride 900 in every file, the empty batch
  included. Rows are grouped 900 at a time (rows 0-899, 900-1799, ...),
  the last group may be shorter, and a batch of n rows has
  ceil(n / 900) groups;
- every column, the struct root included, carries a ROW_INDEX stream
  holding a RowIndex message with one RowIndexEntry per group, in
  group order;
- every entry carries statistics; a data column's entry also carries
  the positions of its streams, concatenated in the order PRESENT
  (when the column has one), DATA, LENGTH (string columns); the root's
  entries carry no positions at all. A stream's positions are what the
  ORC specification defines for a compressed stream of its kind and
  encoding: the pair of offsets that locate the group's first value
  in the chunked stream, followed by the counts the run-length
  encodings need to resume decoding exactly there (none for the raw
  bytes of a string DATA stream), so that a reader seeking to the
  group's first row through the entry decodes exactly the rows from
  that row on. Every entry of a column holds the same, exact number
  of positions, and the walk checks each one: the first offset must
  name a chunk of the stream (or its end) and the second a content
  offset within that chunk; the resume counts must lie within the
  bounds the encodings allow (511 values for an RLE v2 stream; 129
  bytes and 7 bits for a byte run-length stream of packed bits); the
  positions of packed-bit streams and of string DATA streams are
  recomputed from the rows and must match exactly (a packed-bit
  position's content offset must be a run header from which the
  counts reach precisely the byte and bit holding the group's first
  bit; a string DATA position must name the byte at which the group's
  first value begins - in both cases the position a writer records at
  the boundary, including a stream that holds no further value); the
  first group's positions are all 0; and a stream's position never
  moves backwards from one group to the next. RLE v2 positions are
  checked for meaning by the C++ reader's seek. Packed-bit streams
  hold exactly ceil(n / 8) bytes for their n bits, and a string DATA
  stream is exactly the UTF-8 bytes of the column's non-null values in
  row order. Like the encodings themselves, the meaning of each
  position value is defined by the ORC format and not restated here.

Bloom filters (the readers' predicate pushdown prunes row groups with
them, so they are recomputed from the rows and compared bit for bit):

- each of the five bloom-filter columns carries, in its
  BLOOM_FILTER_UTF8 stream, a BloomFilterIndex message with one
  BloomFilter per row group, in group order. A BloomFilter declares
  numHashFunctions 4 and carries utf8bitset: 5632 bits as 88 64-bit
  words, each word little-endian (bit b of the set is bit b mod 64 of
  word floor(b / 64), so byte floor(b / 8), bit b mod 8, of the 704
  bytes). These are the ORC parameters for 900 expected entries at a
  false-positive probability of 0.05: bits = floor(-900 ln 0.05 /
  (ln 2)^2) = 5611, rounded up to the next multiple of 64 with at
  least one full word added, 5611 + (64 - 5611 mod 64) = 5632; hash
  functions = max(1, round(5632 / 900 x ln 2)) = 4;
- every non-null value of the group is added to its group's filter;
  nulls are not. A group whose values are all null yields a filter with
  no bit set;
- values are hashed exactly as the Apache ORC writers hash them for
  the UTF8 bloom filter version. A string value is hashed as its
  UTF-8 bytes with ORC's Murmur3 64-bit variant - the function the
  ORC Java and C++ libraries implement as Murmur3.hash64
  (org.apache.orc.util.Murmur3; the C++ port in the library's
  BloomFilter sources), seeded with ORC's default seed 104729. That
  function applies MurmurHash3's x64 block and finalization mixing to
  8-byte little-endian blocks through a single 64-bit lane, folds the
  remaining tail bytes in, and mixes in the length before the final
  avalanche; it is not the first word of the 128-bit x64 MurmurHash3,
  which mixes two lanes over 16-byte blocks. An integer value is
  hashed with ORC's 64-bit integer mix, BloomFilter.getLongHash in the
  same libraries (Thomas Wang's 64-bit mix in signed two's complement
  arithmetic with sign-propagating right shifts);
- the 64-bit hash h sets 4 bits as the bloom filter section of the
  ORC specification states: hash1 = the low 32 bits of h as a signed
  32-bit integer, hash2 = the high 32 bits as a signed 32-bit integer;
  for i = 1, 2, 3, 4: combined = hash1 + i x hash2 in wrapping signed
  32-bit arithmetic; if combined is negative, combined = ~combined
  (its bitwise complement, which is non-negative); the bit at position
  combined mod 5632 is set.
  Like the run-length encodings and the protobuf messages, the hash
  functions are defined by the ORC format, not restated here; the
  grader's own implementation of them was checked bit for bit against
  files written by the Apache ORC Java writer.

Statistics:

- an entry's statistics describe the group's rows; the metadata section
  holds a Metadata message with exactly one StripeStatistics whose
  colStats describe the stripe; the footer's statistics describe the
  file. The footer's statistics hold one ColumnStatistics per column
  in column order, the struct root first (seven entries), in every
  file, the empty batch included; StripeStatistics.colStats hold the
  same seven entries whenever a stripe exists, and never for the empty
  batch, which has no stripe: its metadata section is absent
  (metadataLength 0) or decodes to a Metadata message with no
  StripeStatistics;
- a ColumnStatistics carries: numberOfValues, the number of non-null
  values (for the root, the number of rows); hasNull, always written,
  true when at least one value is null and false otherwise (the root
  is never null; readers take an absent hasNull as true, so it is
  never omitted); and the column's type-specific statistics:
  - bigint columns: intStatistics with minimum and maximum, present
    exactly when there is at least one value, and sum, present exactly
    when the exact sum of the values fits a signed 64-bit integer (the
    sum of no values is 0 and present). These are sint64 fields:
    zigzag varints;
  - string columns: stringStatistics with minimum and maximum, present
    exactly when there is at least one value - the smallest and largest
    values in code point order (for UTF-8 bytes, byte order), written
    in full whatever their length - and sum, the total UTF-8 byte
    length of the values (sint64, always present);
  - the boolean column: bucketStatistics with exactly one count, the
    number of true values;
  - the root: no type-specific statistics.
  A ColumnStatistics carries no type-specific statistics message other
  than its column's own (no intStatistics on a string column, no
  doubleStatistics, decimalStatistics, dateStatistics,
  binaryStatistics, timestampStatistics or collectionStatistics
  anywhere): the ORC C++ reader does not survive one, and the walk
  rejects it. Other fields (bytesOnDisk, or field numbers the ORC
  definition does not use) are not graded.

The walk decodes the metadata messages by protobuf wire rules: any valid
serialization is accepted, and only the decoded values are graded. A
repeated numeric field may be packed, unpacked, or a mix, and its values
concatenate in order; a singular field that occurs more than once decodes
to its last occurrence (an embedded message field, by concatenation:
standard protobuf merge semantics); unknown fields are skipped without
effect, including well-formed group fields and any occurrence of a known
field number with a mismatched wire type (standard unknown-field
handling).

Each of these rules applies to every graded file, whether it sits under
/app/out or was produced by /app/convert.py on the held-out batches.

## 4. Environment

Python 3.13 and its standard library (zlib included). No network
access. No ORC, Arrow, Avro or Parquet software is installed here, and
none can be installed.
How you produce the bytes is up to you; the grader reads /app/out and
runs /app/convert.py exactly as section 2 states, and nothing else.
