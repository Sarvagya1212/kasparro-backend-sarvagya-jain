FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y \
  curl \
  postgresql-client \
  && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 🚨 THIS LINE IS CRITICAL
COPY . /app

EXPOSE 8000
