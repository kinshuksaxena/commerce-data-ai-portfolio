FROM python:3.11-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY requirements.txt .
RUN uv pip install --no-cache --system -r requirements.txt

COPY static/ ./static/
COPY templates/ ./templates/
COPY main.py .

ENV PYTHONUNBUFFERED=1

EXPOSE 3000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3000"]
