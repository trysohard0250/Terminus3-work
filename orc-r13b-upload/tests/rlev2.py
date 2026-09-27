"""Port of org.apache.orc.impl.RunLengthIntegerWriterV2 (Apache ORC 2.1.3)
and the SerializationUtils helpers it uses: the run selection
(SHORT_REPEAT, DIRECT, PATCHED_BASE, DELTA), run boundaries, percentile
bit widths, base values, patch lists and aligned bit packing of the
Java writer, so the verifier can recompute the integer and length
streams a batch must carry and the positions the writer records at
row-group boundaries. Validated byte for byte against streams written
by the Java writer (orc-tools 2.1.3) on adversarial corpora covering
every sub-encoding. Java long arithmetic is emulated on 64-bit two's
complement values."""

import math

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

class JavaRleV2Writer:
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
        return bytes(self.out)


def encode(values, signed, aligned=True):
    w = JavaRleV2Writer(signed, aligned)
    for v in values:
        w.write(v)
    return w.flush()


def stream_and_positions(values, signed, boundaries):
    """Encode values (non-null, in row order) as the Java writer would,
    recording at each boundary - the number of values that precede a
    row group - the position the writer records there: (bytes written
    so far, values pending). Returns (stream bytes, [(offset, pending)])."""
    w = JavaRleV2Writer(signed)
    marks, bi = [], 0
    bounds = sorted(boundaries)
    for i, v in enumerate(values):
        while bi < len(bounds) and bounds[bi] == i:
            marks.append(w.position())
            bi += 1
        w.write(v)
    while bi < len(bounds):
        marks.append(w.position())
        bi += 1
    return w.flush(), marks
