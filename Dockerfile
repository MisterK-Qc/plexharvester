FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLEX_COMPARE_CONFIG_DIR=/config \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gcc \
    libc6-dev \
    mkvtoolnix \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

LABEL org.opencontainers.image.title="PlexHarvester" \
      org.opencontainers.image.description="Compare Plex libraries, index FTP sources, and harvest missing content with MKV tooling" \
      org.opencontainers.image.version="2.1" \
      org.opencontainers.image.source="https://github.com/MisterK-Qc/plex-compare"

EXPOSE 5000

RUN mkdir -p /config /inbox /inbox/FTP

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -fsS http://127.0.0.1:5000/login || exit 1

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "1", "--threads", "4", "--timeout", "600", "--graceful-timeout", "300", "run:app"]