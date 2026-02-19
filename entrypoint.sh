#!/bin/sh
set -e

# Run Alembic migrations
echo "Running database migrations..."
alembic upgrade head

# Seed on first run (check if carbon_intensity table has data)
RECORD_COUNT=$(python -c "
import psycopg, os
conn = psycopg.connect(os.environ['DATABASE_URL'])
row = conn.execute('SELECT COUNT(*) FROM carbon_intensity').fetchone()
print(row[0])
conn.close()
")

if [ "$RECORD_COUNT" = "0" ]; then
    echo "First run — seeding 7 days of historical data..."
    gridcarbon seed --days 7
fi

# If CMD was provided (e.g. docker-compose command override), run that.
# Otherwise default to the API server.
if [ $# -gt 0 ]; then
    echo "Running command: $@"
    exec "$@"
else
    echo "Starting gridcarbon API server..."
    exec gridcarbon serve --host 0.0.0.0 --port 8000
fi
