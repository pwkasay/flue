FROM python:3.14-slim

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir git+https://github.com/pwkasay/weir.git@7a3ba72

COPY pyproject.toml README.md ./
COPY src/ src/
RUN pip install --no-cache-dir .

COPY alembic.ini ./
COPY alembic/ alembic/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV DATABASE_URL=postgresql://gridcarbon:gridcarbon@postgres:5432/gridcarbon
EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
