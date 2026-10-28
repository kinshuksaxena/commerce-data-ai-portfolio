FROM python:3.11-slim

WORKDIR /app

# Install uv for fast dependency installs
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY requirements.txt .
RUN uv pip install --no-cache --system -r requirements.txt

COPY simulator.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "simulator.py"]
