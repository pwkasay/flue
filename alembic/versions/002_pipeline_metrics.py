"""Add pipeline_metrics table for weir on_metrics snapshots.

Revision ID: 002
Revises: 001
Create Date: 2026-02-18

Stores periodic stage metrics snapshots from weir's on_metrics() callback
for time-series display on the admin dashboard.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE pipeline_metrics (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            pipeline_name TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            items_in INTEGER NOT NULL,
            items_out INTEGER NOT NULL,
            items_errored INTEGER NOT NULL,
            items_retried INTEGER NOT NULL DEFAULT 0,
            error_rate DOUBLE PRECISION,
            throughput_per_sec DOUBLE PRECISION,
            latency_p50 DOUBLE PRECISION,
            latency_p95 DOUBLE PRECISION,
            latency_p99 DOUBLE PRECISION,
            queue_depth INTEGER,
            queue_utilization DOUBLE PRECISION
        )
    """)
    op.execute("CREATE INDEX idx_pm_timestamp ON pipeline_metrics(timestamp)")
    op.execute("CREATE INDEX idx_pm_pipeline ON pipeline_metrics(pipeline_name)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS pipeline_metrics")
