# Agent image. Nothing from tests/ or solution/ may ever be copied in here,
# and no ORC, Arrow, Avro or Parquet software is installed: the whole point
# of the task is that the agent cannot check its own output.
FROM public.ecr.aws/docker/library/python:3.13-slim-bookworm@sha256:01f42367a0a94ad4bc17111776fd66e3500c1d87c15bbd6055b7371d39c124fb

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    asciinema \
    ca-certificates \
    curl \
    nodejs \
    npm \
    procps \
    tmux \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN mkdir -p /app/out

COPY app/ /app/
