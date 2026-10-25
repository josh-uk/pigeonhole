FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.title="Pigeonhole" \
      org.opencontainers.image.description="Write, run and schedule Python and bash scripts from a web UI" \
      org.opencontainers.image.source="https://github.com/josh-uk/pigeonhole"

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    TZ=UTC

RUN apt-get update \
 && apt-get install -y --no-install-recommends bash curl ca-certificates tzdata procps git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

VOLUME /data
EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
