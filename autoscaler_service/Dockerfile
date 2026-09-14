# Build context is the repository root (see docker-compose.yml) so the code is
# importable as the `autoscaler_service` package, exactly as in local development.
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first so code changes don't invalidate this layer.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# tests/ and dev requirements are excluded via .dockerignore at the repo root.
COPY autoscaler_service /app/autoscaler_service

EXPOSE 8000

# The rules file and manifest are read from S3 at runtime — nothing is bundled.
CMD ["python", "-m", "autoscaler_service.main"]
