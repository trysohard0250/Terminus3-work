#!/bin/bash
# Verifier entrypoint. Runs in the separate verifier container.
#
# Deliberately NOT `set -e`: a failing pytest must still reach the reward write.
set -uo pipefail

mkdir -p /logs/verifier
chmod 700 /logs/verifier
# The submitted converter runs as an unprivileged user: the tests, the
# salt, the verifier's venv (with the ORC reader) and the submitted
# archive are all unreadable to it.
chmod -R go-rwx /tests 2>/dev/null || true
chmod -R go-rwx /venv 2>/dev/null || true
chmod -R go-rwx /app 2>/dev/null || true
# execute-only: the sandbox user can traverse into its own grading
# directory by name but cannot enumerate /work, so it cannot count
# or observe other invocations' directories
chmod 711 /work 2>/dev/null || true

cd /tests
/venv/bin/python -m pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA -p no:cacheprovider
rc=$?

if [ "$rc" -eq 0 ]; then
  echo 1 > /logs/verifier/reward.txt
else
  echo 0 > /logs/verifier/reward.txt
fi

exit 0
