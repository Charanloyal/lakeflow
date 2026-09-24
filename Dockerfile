FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501 8000 10000

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
  CMD curl --fail http://localhost:${PORT:-8501}/ || exit 1

ENTRYPOINT ["python", "run_app.py"]
