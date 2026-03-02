"""
Ingestion Scheduler

Runs the daily feed simulator + incremental ETL on a configurable schedule.
This simulates what Airflow, cron, or a cloud scheduler would do in production.

Architecture:
    ┌─────────────┐     ┌────────────────┐     ┌──────────────────┐
    │  Scheduler   │────▶│  Daily Feed     │────▶│  Incremental ETL │
    │  (APScheduler│     │  (simulate API)  │     │  (landing zone)  │
    │   / cron)    │     │  → incoming/*.csv│     │  → DB insert     │
    └─────────────┘     └────────────────┘     └──────────────────┘

Usage:
    python -m pipeline.scheduler                   # Run once now
    python -m pipeline.scheduler --watch           # Keep running on schedule
    python -m pipeline.scheduler --interval 30     # Every 30 seconds (demo)

Production alternatives:
    - Airflow:  @daily DAG with PythonOperator
    - cron:     0 18 * * 1-5 python -m pipeline.scheduler
    - AWS:      EventBridge rule → Lambda → Step Functions
    - Azure:    Data Factory pipeline trigger
"""

import logging
import argparse
import sys
import time
import signal
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.daily_feed import generate_feed
from pipeline.incremental_etl import run_incremental

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Track run history for the status endpoint
run_history: list[dict] = []
_running = True


def record_run(success: bool, records: int = 0, error: str = None):
    """Log each pipeline run for observability."""
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "success": success,
        "records_generated": records,
        "error": error,
    }
    run_history.append(entry)
    # Keep last 100 runs
    if len(run_history) > 100:
        run_history.pop(0)


def run_once(n_days: int = 1):
    """Execute one cycle: generate feed → incremental ETL."""
    logger.info("\n" + "━" * 60)
    logger.info(f"  Pipeline Cycle — {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info("━" * 60)

    try:
        # Step 1: Simulate data arrival
        logger.info("\n[1/2] Generating daily price feed...")
        generate_feed(n_days=n_days)

        # Step 2: Process landing zone
        logger.info("\n[2/2] Running incremental ETL...")
        run_incremental(dry_run=False, archive=True)

        record_run(success=True, records=n_days * 50)  # ~50 tickers per day
        logger.info("\n✓ Pipeline cycle complete")

    except Exception as e:
        logger.error(f"\n✗ Pipeline cycle failed: {e}")
        record_run(success=False, error=str(e))


def watch_mode(interval_seconds: int = 60, days_per_cycle: int = 1):
    """
    Run the pipeline on a repeating schedule.

    In production this would be:
    - Airflow: schedule_interval='@daily'
    - cron: 0 18 * * 1-5
    - APScheduler: CronTrigger(hour=18, day_of_week='mon-fri')
    """
    global _running

    def handle_signal(signum, frame):
        global _running
        logger.info("\nReceived shutdown signal — finishing current cycle...")
        _running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info("=" * 60)
    logger.info("  Pipeline Scheduler — Watch Mode")
    logger.info(f"  Interval: {interval_seconds}s | Days per cycle: {days_per_cycle}")
    logger.info("  Press Ctrl+C to stop gracefully")
    logger.info("=" * 60)

    cycle = 0
    while _running:
        cycle += 1
        logger.info(f"\n{'─' * 40} Cycle {cycle} {'─' * 40}")
        run_once(n_days=days_per_cycle)

        if not _running:
            break

        logger.info(f"\nSleeping {interval_seconds}s until next cycle...")
        # Sleep in small increments so Ctrl+C is responsive
        for _ in range(interval_seconds):
            if not _running:
                break
            time.sleep(1)

    logger.info("\nScheduler stopped.")


def get_run_history() -> list[dict]:
    """Return run history — consumed by the API status endpoint."""
    return list(run_history)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline scheduler")
    parser.add_argument("--watch", action="store_true", help="Keep running on schedule")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between cycles (watch mode)")
    parser.add_argument("--days", type=int, default=1, help="Trading days to generate per cycle")
    args = parser.parse_args()

    if args.watch:
        watch_mode(interval_seconds=args.interval, days_per_cycle=args.days)
    else:
        run_once(n_days=args.days)
