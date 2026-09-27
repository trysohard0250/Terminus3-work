Our delivery desk keeps a daily archive of feed events. The pipeline that
wrote it is being retired, and downstream analytics read the archive with
Apache ORC tooling, so the day's batches have to land in that format
byte-correct on the first try: there is no ORC software in this
environment to check against, and a bad file poisons the archive.

Convert every batch under `/app/data/batches/` into an ORC file under
`/app/out/`, one `.orc` per `.jsonl`, same name and nothing else. Also
leave the converter itself at `/app/convert.py`; tomorrow's batches go
through it unattended, so the grader runs it on held-out batches of the
same shape. The data dictionary, the exact file requirements, and what
the grader checks are in `/app/CONTRACT.md`.
