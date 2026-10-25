FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.title="Pigeonhole" \
      org.opencontainers.image.description="Write, run and schedule Python and bash scripts from a web UI" \
      org.opencontainers.image.source="https://github.com/josh-uk/pigeonhole"

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    DATA_DIR=/data \
    PUID=1000 \
    PGID=1000 \
    TZ=UTC

RUN apt-get update \
 && apt-get install -y --no-install-recommends bash curl ca-certificates tzdata procps git sudo \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -g 1000 pigeonhole \
 && useradd -m -u 1000 -g 1000 -s /bin/bash pigeonhole \
 && printf 'Defaults env_keep += "DEBIAN_FRONTEND"\npigeonhole ALL=(root) NOPASSWD: /usr/bin/apt-get\n' > /etc/sudoers.d/pigeonhole \
 && chmod 0440 /etc/sudoers.d/pigeonhole

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

VOLUME /data
EXPOSE 8080
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
