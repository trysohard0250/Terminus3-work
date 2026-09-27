#!/usr/bin/env python3
"""Deterministic event batches for the ORC archiving task.

Families each batch exercises (all stated in verification_explanation):
single row, empty batch, exactly-512 and just-past-512 rows (run boundary),
no nulls anywhere, all-null columns, int64 extremes and negatives in both
integer columns,
multibyte UTF-8 and empty strings, boolean run/literal stress, large batch.
"""
import json
import random
import sys
from pathlib import Path

REGIONS = ["AMER", "EMEA", "APAC", "LATAM"]
NOTES = ["", "ok", "late upstream", "resent by vendor", "ué中文",
         "éclair", "☃ snow day", "backfill → done",
         "\U0001F4E6 box", "a" * 300]


def rows(rng, n, null_rate=0.15, note_null=0.3, extremes=False):
    out = []
    for i in range(n):
        seq = rng.randrange(-10**12, 10**15)
        lat = None if rng.random() < null_rate else rng.randrange(0, 10**7)
        if extremes:
            if i == 0:
                seq = -2**63
                lat = -2**63
            elif i == 1:
                seq = 2**63 - 1
                lat = 2**63 - 1
            elif i == 2:
                lat = -1
            elif i % 5 == 2:
                seq = rng.choice([-1, 0, 1, -2**62, 2**62])
            if lat is not None and i % 7 == 3:
                lat = rng.choice([0, 2**63 - 1, 2**40, -2**40, -1])
        breach = None if rng.random() < null_rate else rng.random() < 0.3
        note = None if rng.random() < note_null else rng.choice(NOTES)
        out.append({
            "event_id": f"E{rng.randrange(16**10):010x}",
            "feed_id": f"{rng.choice(REGIONS)}-F{rng.randrange(1000):03d}",
            "seq": seq,
            "latency_ms": lat,
            "breach": breach,
            "note": note,
        })
    return out


def gen(seed, dest):
    rng = random.Random(seed)
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    batches = {}
    batches["b01-basic"] = rows(rng, 3, null_rate=0)
    batches["b02-single"] = rows(rng, 1, null_rate=0.5)
    batches["b03-empty"] = []
    batches["b04-past-run"] = rows(rng, 520)
    batches["b05-mixed"] = rows(rng, 4000)
    b6 = rows(rng, 240, null_rate=0)
    for r in b6:
        r["latency_ms"] = None
        r["note"] = None
    batches["b06-null-columns"] = b6
    batches["b07-dense"] = rows(rng, 700, null_rate=0, note_null=0)
    batches["b08-extremes"] = rows(rng, 900, extremes=True)
    b9 = rows(rng, 333, note_null=0)
    for i, r in enumerate(b9):
        r["note"] = NOTES[i % len(NOTES)]
    batches["b09-unicode"] = b9
    b10 = rows(rng, 2049, null_rate=0)
    for i, r in enumerate(b10):
        if i < 600:
            r["breach"] = True            # long run
        elif i < 1200:
            r["breach"] = bool(i % 2)     # literals
        elif i % 11 == 0:
            r["breach"] = None
    batches["b10-bool-stress"] = b10
    batches["b11-large"] = rows(rng, 2600)
    batches["b12-run-boundary"] = rows(rng, 512)

    for name, recs in batches.items():
        with open(dest / f"{name}.jsonl", "w", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r, ensure_ascii=False,
                                   separators=(",", ":")) + "\n")
        print(name, len(recs))


if __name__ == "__main__":
    gen(int(sys.argv[1]) if len(sys.argv) > 1 else 90210,
        sys.argv[2] if len(sys.argv) > 2 else "batches")
