# Verifier image. In separate mode harbor does NOT upload tests/ at verify
# time, so this image owns everything the verifier needs: the tests, the
# batch copies, the pinned Apache Arrow ORC reader and the pinned Apache
# ORC C++ library (pyorc), the independent structural walker, and the
# held-out batch generator with its key. The submitted converter is
# executed only in a fresh unprivileged process (setpriv, no-new-privs)
# on the system interpreter in isolated mode; the verifier's venv with
# the ORC readers is locked away from it.
FROM public.ecr.aws/docker/library/python:3.13-slim-bookworm@sha256:01f42367a0a94ad4bc17111776fd66e3500c1d87c15bbd6055b7371d39c124fb

RUN apt-get update \
    && apt-get install -y --no-install-recommends util-linux \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /venv \
    && /venv/bin/pip install --no-cache-dir \
       pytest==9.1.1 pytest-json-ctrf==0.5.2 pyarrow==25.0.1 pyorc==0.11.0

COPY test.sh test_outputs.py walker.py rlev2.py cppread.py gen_holdout.py salt.txt /tests/
COPY data/ /tests/data/

# Landing directory for the declared artifacts and converter scratch space.
RUN mkdir -p /app/out /work
