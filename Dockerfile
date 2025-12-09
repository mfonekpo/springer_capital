FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Install system dependencies (minimal for slim image)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN python3 -mpip install --upgrade pip setuptools wheel && \
    python3 -mpip install -r requirements.txt

# Copy entire project
COPY . .

# Create reports directory
RUN mkdir -p script/reports

# Set the entry point
ENTRYPOINT ["python", "script/analytics.py"]