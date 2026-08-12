FROM python:3.13-slim

LABEL org.opencontainers.image.source="https://github.com/Loud-Roar/rigpulse" \
      org.opencontainers.image.title="RigPulse" \
      org.opencontainers.image.description="Local ASIC fleet monitoring"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RIGPULSE_DATA_DIR=/data \
    RIGPULSE_POLL_SECONDS=10

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --uid 1000 --create-home --shell /usr/sbin/nologin rigpulse \
    && mkdir -p /data \
    && chown -R 1000:1000 /data /app

COPY --chown=1000:1000 app /app/app

USER 1000:1000

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
