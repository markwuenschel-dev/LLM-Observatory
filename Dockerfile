FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/observatory
COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --no-cache-dir . \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin observatory \
    && mkdir -p /var/lib/observatory \
    && chown -R observatory:observatory /opt/observatory /var/lib/observatory

USER observatory
EXPOSE 8787

CMD ["python", "-m", "observatory.cli", "--state-dir", "/var/lib/observatory", "run-api", "--host", "0.0.0.0", "--port", "8787"]

