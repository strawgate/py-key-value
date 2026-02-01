FROM ghcr.io/astral-sh/uv:python3.10-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs npm \
    && rm -rf /var/lib/apt/lists/*