#!/bin/bash
# Oracle. Runs the reference writer inside the agent environment.
set -euo pipefail

mkdir -p /app/out
cp /solution/orcwrite.py /app/convert.py
python3 /app/convert.py /app/data/batches /app/out

# refuse to succeed if nothing was produced
count=$(ls /app/out/*.orc 2>/dev/null | wc -l)
if [ "$count" -lt 1 ]; then
  echo "solve.sh: no archive files were written" >&2
  exit 1
fi
echo "solve.sh: wrote $count archive files"
