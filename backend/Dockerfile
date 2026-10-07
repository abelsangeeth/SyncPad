# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

COPY backend/pyproject.toml ./pyproject.toml
COPY backend/syncpad ./syncpad
RUN pip install --no-cache-dir .

# Migrations ship with the image so the platform's release step can apply
# them against the real database before a new instance serves traffic.
COPY backend/alembic.ini ./alembic.ini
COPY backend/alembic ./alembic

# Copied and made executable before the USER switch, while still root.
COPY backend/docker-entrypoint.sh /usr/local/bin/syncpad-entrypoint
RUN chmod +x /usr/local/bin/syncpad-entrypoint

COPY frontend /app/frontend

RUN useradd --create-home --uid 10001 syncpad && chown -R syncpad:syncpad /app
USER syncpad

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
  CMD curl -fsS http://localhost:8000/healthz || exit 1

# "--ws auto" rather than "--ws websockets": the latter is deprecated and is
# due to be repointed at another implementation by uvicorn.
# The gateway is stateless, so every replica runs this identical command and
# any of them can serve any socket for any document.
ENTRYPOINT ["/usr/local/bin/syncpad-entrypoint"]

CMD ["uvicorn", "syncpad.main:app", "--host", "0.0.0.0", "--port", "8000", \
     "--factory", "--ws", "auto", "--proxy-headers", "--forwarded-allow-ips", "*"]
