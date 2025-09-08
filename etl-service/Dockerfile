# etl-service/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# (Optional) build tools for some wheels; harmless if not used
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc && rm -rf /var/lib/apt/lists/*

# Copy and install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY src/ ./src

EXPOSE 8000

# No --reload (so BackgroundTasks are reliable)
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
