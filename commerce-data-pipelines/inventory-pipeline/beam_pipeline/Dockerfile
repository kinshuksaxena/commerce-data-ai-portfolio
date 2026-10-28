FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast, reliable package installations
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY requirements.txt .
RUN uv pip install --no-cache --system -r requirements.txt

COPY pipeline.py .

# Unbuffered output so logs stream immediately in docker logs
ENV PYTHONUNBUFFERED=1

CMD ["python", "pipeline.py"]
