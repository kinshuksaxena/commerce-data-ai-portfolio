FROM python:3.11-slim

WORKDIR /app

# Install dependencies using uv for fast builds
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install curl and docker CLI for container log extraction
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \
    curl -fsSL https://download.docker.com/linux/static/stable/aarch64/docker-26.1.4.tgz -o /tmp/docker.tgz || \
    curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-26.1.4.tgz -o /tmp/docker.tgz && \
    (tar -xzf /tmp/docker.tgz -C /usr/local/bin --strip-components=1 docker/docker 2>/dev/null || true) && \
    rm -rf /tmp/docker.tgz /var/lib/apt/lists/*

COPY requirements.txt .
RUN uv pip install --no-cache --system -r requirements.txt

COPY main.py .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
