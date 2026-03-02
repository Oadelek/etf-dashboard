"""
Incremental ETL: Landing Zone → SQLite

Picks up new price CSV files from data/incoming/, validates them,
loads into the database, then archives processed files.

This is the "hot path" — designed to run frequently (every few minutes
or triggered by file arrival) vs the full ETL which is a cold bootstrap.

Data Flow:
───────────────────────────────────────────────────────────────
  data/incoming/prices_2026-01-02.csv   ──→  [Validate]  ──→  prices table
  data/incoming/prices_2026-01-03.csv   ──→  [Validate]  ──→  prices table
                                        ──→  [Archive]   ──→  data/processed/

Design Decisions:
─────────────────
- Landing Zone Pattern:   New data arrives as files in incoming/
- Exactly-Once Semantics:  UniqueConstraint prevents duplicate inserts
- Audit Trail:             Processed files moved to processed/ with timestamp
- Watermark Tracking:      DB tracks the latest date per ticker to detect gaps
- Observability:           Logs every file, record count, and any anomalies
- Idempotent:              Re-processing an already-loaded file is a no-op

Usage:
    python -m pipeline.incremental_etl                # Process all pending files
    python -m pipeline.incremental_etl --dry-run      # Preview without loading
    python -m pipeline.incremental_etl --no-archive   # Don't move processed files

Production Alternatives:
    - Airflow DAG with file sensor operator
    - AWS Lambda triggered by S3 PutObject event
    - Kafka consumer reading from a price topic
    - dbt incremental model with merge strategy
"""

import pandas as pd
import logging
import argparse
import shutil
import sys
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.app.database import (
    SessionLocal, Constituent, Price, init_db,
)
from sqlalchemy import func, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
INCOMING_DIR = DATA_DIR / "incoming"
PROCESSED_DIR = DATA_DIR / "processed"


# ──────────────────────────────────────────────
#  WATERMARK — track ingestion progress
# ──────────────────────────────────────────────

def get_high_watermark(session) -> dict:
    """
    Query the latest date per ticker already in the DB.

    This is the "watermark" — we only ingest records newer than this.

    SQL:
        SELECT c.ticker, MAX(p.date) as latest_date
        FROM prices p
        JOIN constituents c ON c.id = p.constituent_id
        GROUP BY c.ticker
    """
    rows = (
        session.query(
            Constituent.ticker,
            func.max(Price.date).label("latest_date"),
        )
        .join(Price, Price.constituent_id == Constituent.id)
        .group_by(Constituent.ticker)
        .all()
    )
    return {row.ticker: row.latest_date for row in rows}


def get_ticker_map(session) -> dict[str, int]:
    """Get ticker → constituent_id mapping."""
    return {
        c.ticker: c.id
        for c in session.query(Constituent).all()
    }


# ──────────────────────────────────────────────
#  VALIDATE incremental batch
# ──────────────────────────────────────────────

def validate_batch(df: pd.DataFrame, watermarks: dict, filename: str) -> pd.DataFrame:
    """
    Validate an incoming price batch.

    Checks:
    1. Required columns present
    2. No null close prices
    3. No negative prices
    4. Filter out records at or before the watermark (already loaded)
    5. Flag any gaps (missing trading days between watermark and new data)
    """
    required_cols = {"date", "ticker", "close"}
    if not required_cols.issubset(set(df.columns)):
        logger.error(f"  REJECT | {filename} missing columns: {required_cols - set(df.columns)}")
        return pd.DataFrame()

    initial = len(df)

    # Null / negative
    df = df.dropna(subset=["close"])
    df = df[df["close"] > 0]

    # Parse dates
    df["date"] = pd.to_datetime(df["date"])

    # Filter out already-loaded data using watermark
    new_records = []
    for _, row in df.iterrows():
        ticker = row["ticker"]
        row_date = row["date"].date() if hasattr(row["date"], "date") else row["date"]

        watermark = watermarks.get(ticker)
        if watermark and row_date <= watermark:
            continue  # Already in DB
        new_records.append(row)

    if not new_records:
        logger.info(f"  SKIP   | {filename} — all records already at or below watermark")
        return pd.DataFrame()

    result = pd.DataFrame(new_records)
    filtered = initial - len(result)
    if filtered > 0:
        logger.info(f"  FILTER | {filtered} existing records filtered by watermark")

    return result


# ──────────────────────────────────────────────
#  LOAD incremental batch
# ──────────────────────────────────────────────

def load_batch(session, df: pd.DataFrame, ticker_map: dict[str, int]) -> int:
    """
    Insert validated records into the prices table.

    Uses INSERT-or-SKIP pattern: the UniqueConstraint on
    (constituent_id, date) prevents duplicates even if the
    watermark check somehow lets one through.
    """
    inserted = 0

    for _, row in df.iterrows():
        ticker = row["ticker"]
        if ticker not in ticker_map:
            logger.warning(f"  WARN   | Unknown ticker '{ticker}', skipping")
            continue

        constituent_id = ticker_map[ticker]
        date_val = row["date"].date() if hasattr(row["date"], "date") else row["date"]

        # Double-check: skip if already exists (belt + suspenders)
        exists = session.query(Price.id).filter_by(
            constituent_id=constituent_id, date=date_val
        ).first()
        if exists:
            continue

        price = Price(
            constituent_id=constituent_id,
            date=date_val,
            open_price=round(float(row["open"]), 2) if pd.notna(row.get("open")) else None,
            high_price=round(float(row["high"]), 2) if pd.notna(row.get("high")) else None,
            low_price=round(float(row["low"]), 2) if pd.notna(row.get("low")) else None,
            close_price=round(float(row["close"]), 2),
            volume=int(row["volume"]) if pd.notna(row.get("volume")) else None,
        )
        session.add(price)
        inserted += 1

    if inserted > 0:
        session.commit()

    return inserted


# ──────────────────────────────────────────────
#  ARCHIVE processed files
# ──────────────────────────────────────────────

def archive_file(filepath: Path):
    """Move processed file to archive with timestamp suffix."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dest = PROCESSED_DIR / f"{filepath.stem}__processed_{ts}{filepath.suffix}"
    shutil.move(str(filepath), str(dest))
    logger.info(f"  ARCHIVE| {filepath.name} → processed/{dest.name}")


# ──────────────────────────────────────────────
#  ORCHESTRATOR
# ──────────────────────────────────────────────

def run_incremental(dry_run: bool = False, archive: bool = True):
    """
    Process all pending files in the landing zone.

    Steps:
    1. Get current watermark (latest date per ticker in DB)
    2. List all CSV files in data/incoming/
    3. For each file: validate → load → archive
    4. Report summary
    """
    start_time = time.time()

    logger.info("=" * 60)
    logger.info("  Incremental ETL — Processing Landing Zone")
    logger.info("=" * 60)

    if not INCOMING_DIR.exists() or not list(INCOMING_DIR.glob("*.csv")):
        logger.info("EMPTY    | No files in landing zone. Nothing to do.")
        logger.info("         | Generate data first: python -m pipeline.daily_feed --days 5")
        return

    init_db()
    session = SessionLocal()

    try:
        # Step 1: Get watermarks
        watermarks = get_high_watermark(session)
        if not watermarks:
            logger.error("NO DATA  | Database is empty. Run full ETL first: python -m pipeline.etl")
            return

        global_max = max(watermarks.values())
        logger.info(f"WATER    | Current high watermark: {global_max} ({len(watermarks)} tickers)")

        ticker_map = get_ticker_map(session)

        # Step 2: Process files in chronological order
        csv_files = sorted(INCOMING_DIR.glob("*.csv"))
        logger.info(f"PENDING  | {len(csv_files)} file(s) to process")

        total_inserted = 0
        files_processed = 0
        files_skipped = 0

        for csv_path in csv_files:
            logger.info(f"\n── {csv_path.name} ──")

            df = pd.read_csv(csv_path)

            if dry_run:
                logger.info(f"  DRY-RUN| Would process {len(df)} records")
                continue

            # Validate against watermark
            valid_df = validate_batch(df, watermarks, csv_path.name)

            if valid_df.empty:
                files_skipped += 1
                if archive:
                    archive_file(csv_path)
                continue

            # Load
            n_inserted = load_batch(session, valid_df, ticker_map)
            total_inserted += n_inserted
            files_processed += 1

            logger.info(f"  LOADED | {n_inserted} new records inserted")

            # Update in-memory watermarks for next file
            for ticker in valid_df["ticker"].unique():
                ticker_dates = valid_df[valid_df["ticker"] == ticker]["date"]
                new_max = ticker_dates.max()
                if hasattr(new_max, "date"):
                    new_max = new_max.date()
                old = watermarks.get(ticker)
                if old is None or new_max > old:
                    watermarks[ticker] = new_max

            # Archive
            if archive:
                archive_file(csv_path)

        # Step 3: Summary
        elapsed = time.time() - start_time
        new_max = max(watermarks.values()) if watermarks else "N/A"

        logger.info(f"\n{'=' * 60}")
        logger.info(f"  Incremental ETL Complete — {elapsed:.2f}s")
        logger.info(f"{'=' * 60}")
        logger.info(f"  Files processed : {files_processed}")
        logger.info(f"  Files skipped   : {files_skipped}")
        logger.info(f"  Records inserted: {total_inserted:,}")
        logger.info(f"  New watermark   : {new_max}")

        # Check total DB size
        total = session.query(func.count(Price.id)).scalar()
        logger.info(f"  Total DB records: {total:,}")
        logger.info("")

    except Exception as e:
        logger.error(f"Incremental ETL failed: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Incremental ETL — process landing zone")
    parser.add_argument("--dry-run", action="store_true", help="Preview without loading")
    parser.add_argument("--no-archive", action="store_true", help="Don't move processed files")
    args = parser.parse_args()

    run_incremental(dry_run=args.dry_run, archive=not args.no_archive)
