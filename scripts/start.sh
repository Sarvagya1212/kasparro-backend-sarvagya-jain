
#!/bin/bash
set -e

echo "========================================"
echo "Kasparro ETL Backend - Starting Up"
echo "========================================"

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
until pg_isready -h postgres -U etl_user -d etl_db; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL is ready!"

# Run database migrations
echo "Running database migrations..."
alembic upgrade head

# Run initial ETL job
echo "Running initial ETL job..."
python scripts/run_etl.py || echo "ETL job failed, but continuing..."

# Start Uvicorn server
echo "Starting Uvicorn server..."
exec uvicorn api.main:app \
  --host ${API_HOST:-0.0.0.0} \
  --port ${API_PORT:-8000} \
  --log-level ${LOG_LEVEL:-info} \
  --reload