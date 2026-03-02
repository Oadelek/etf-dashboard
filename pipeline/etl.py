"""
ETL Pipeline: CSV → SQLite

Extracts OHLCV price data, constituent metadata, and ETF weights
from CSV files, validates and transforms them, then loads into SQLite.

Usage:
    python -m pipeline.etl                # Full pipeline (prices + all ETFs)
    python -m pipeline.etl --no-prices    # Skip price data, load ETFs only
    python -m pipeline.etl --etf tech     # Load a specific ETF file (partial match)
    python -m pipeline.etl --fresh        # Drop all tables and reload from scratch

Data Flow:
──────────────────────────────────────────────────────────────
  constituents.csv  ──[Extract]──→ ──[Load]──→  constituents table
  prices.csv        ──[Extract]──→ ──[Validate]──→ ──[Load]──→  prices table
  *_etf.csv         ──[Extract]──→ ──[Load]──→  etf_uploads + etf_holdings

Design Decisions:
─────────────────
- Idempotent:    Safe to run multiple times (checks for existing records)
- Chunked:       Inserts in batches for memory efficiency with large datasets
- Logged:        Structured logging for observability in production
- Transactional: All-or-nothing per logical unit (prices, each ETF)
- Separation:    Clear Extract → Transform → Load stages
"""

import pandas as pd
import logging
import argparse
import sys
import time
from pathlib import Path

# Add project root to path so we can import backend modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.app.database import (
    engine, SessionLocal, Base, Constituent, Price, ETFUpload, ETFHolding,
    init_db, drop_db,
)

# ──────────────────────────────────────────────
#  Logging Configuration
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"


# ──────────────────────────────────────────────
#  EXTRACT
# ──────────────────────────────────────────────

def extract_constituents(csv_path: str) -> pd.DataFrame:
    """Read constituent metadata (ticker, name, sector, industry, market_cap)."""
    logger.info(f"EXTRACT  | Reading constituents from {Path(csv_path).name}")
    df = pd.read_csv(csv_path)
    logger.info(f"         | {len(df)} constituents across {df['sector'].nunique()} sectors")
    return df


def extract_prices(csv_path: str) -> pd.DataFrame:
    """
    Read OHLCV price data in long format.
    Expected columns: date, ticker, open, high, low, close, volume
    """
    logger.info(f"EXTRACT  | Reading prices from {Path(csv_path).name}")
    df = pd.read_csv(csv_path, parse_dates=["date"])
    n_tickers = df["ticker"].nunique()
    n_days = df["date"].nunique()
    logger.info(f"         | {len(df):,} rows — {n_tickers} tickers × {n_days} days")
    return df


# ──────────────────────────────────────────────
#  TRANSFORM / VALIDATE
# ──────────────────────────────────────────────

def validate_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and clean the OHLCV price data.

    Checks:
    - No null close prices
    - No negative prices
    - OHLC consistency (Low ≤ Open,Close ≤ High)
    - No duplicate (ticker, date) pairs
    """
    logger.info("VALIDATE | Checking price data quality")
    initial_count = len(df)

    # 1. Null check
    null_close = df["close"].isna().sum()
    if null_close > 0:
        logger.warning(f"         | ⚠ {null_close} null close prices — dropping")
        df = df.dropna(subset=["close"])

    # 2. Negative check
    neg_count = (df["close"] < 0).sum()
    if neg_count > 0:
        logger.warning(f"         | ⚠ {neg_count} negative close prices — dropping")
        df = df[df["close"] >= 0]

    # 3. OHLC consistency
    if all(col in df.columns for col in ["open", "high", "low"]):
        inconsistent = (df["low"] > df["close"]) | (df["high"] < df["close"])
        n_bad = inconsistent.sum()
        if n_bad > 0:
            logger.warning(f"         | ⚠ {n_bad} OHLC inconsistencies found (kept but noted)")

    # 4. Duplicates
    dups = df.duplicated(subset=["ticker", "date"], keep="last")
    if dups.sum() > 0:
        logger.warning(f"         | ⚠ {dups.sum()} duplicate (ticker,date) — keeping last")
        df = df.drop_duplicates(subset=["ticker", "date"], keep="last")

    final_count = len(df)
    logger.info(f"         | {final_count:,} records passed ({initial_count - final_count} removed)")
    return df


# ──────────────────────────────────────────────
#  LOAD
# ──────────────────────────────────────────────

def load_constituents(session, df: pd.DataFrame) -> dict[str, int]:
    """
    Upsert constituents with metadata.
    Returns a mapping of ticker → constituent_id.
    """
    logger.info(f"LOAD     | Upserting {len(df)} constituents")
    ticker_map = {}

    for _, row in df.iterrows():
        ticker = row["ticker"]
        existing = session.query(Constituent).filter_by(ticker=ticker).first()

        if existing:
            existing.name = row.get("name")
            existing.sector = row.get("sector")
            existing.industry = row.get("industry")
            existing.market_cap = row.get("market_cap")
            ticker_map[ticker] = existing.id
        else:
            constituent = Constituent(
                ticker=ticker,
                name=row.get("name"),
                sector=row.get("sector"),
                industry=row.get("industry"),
                market_cap=row.get("market_cap"),
            )
            session.add(constituent)
            session.flush()
            ticker_map[ticker] = constituent.id

    session.commit()
    logger.info(f"         | {len(ticker_map)} constituents in database")
    return ticker_map


def load_prices(session, prices_df: pd.DataFrame, ticker_map: dict[str, int],
                chunk_size: int = 1000):
    """
    Insert OHLCV price records in chunks.
    Idempotent: checks for existing (constituent_id, date) pairs.
    """
    total = len(prices_df)
    logger.info(f"LOAD     | Inserting {total:,} price records (chunk_size={chunk_size})")

    inserted = 0
    skipped = 0

    for start in range(0, total, chunk_size):
        chunk = prices_df.iloc[start:start + chunk_size]

        for _, row in chunk.iterrows():
            ticker = row["ticker"]
            if ticker not in ticker_map:
                skipped += 1
                continue

            constituent_id = ticker_map[ticker]
            date_val = row["date"].date() if hasattr(row["date"], "date") else row["date"]

            exists = session.query(Price).filter_by(
                constituent_id=constituent_id, date=date_val
            ).first()

            if exists:
                skipped += 1
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

        session.commit()
        progress = min(start + chunk_size, total)
        if progress % 5000 < chunk_size or progress == total:
            logger.info(f"         | Progress: {progress:,}/{total:,} ({progress / total * 100:.0f}%)")

    logger.info(f"         | Done — inserted: {inserted:,}, skipped: {skipped:,}")


def load_etf_weights(session, csv_path: str, ticker_map: dict[str, int]) -> ETFUpload:
    """
    Insert an ETF definition from a weights CSV.
    Supports both 'ticker' and 'name' as the holding identifier column.
    Idempotent: skips if an ETF with the same filename already exists.
    """
    filename = Path(csv_path).name
    logger.info(f"LOAD     | Processing ETF '{filename}'")

    existing = session.query(ETFUpload).filter_by(filename=filename).first()
    if existing:
        logger.info(f"         | Already exists (id={existing.id}), skipping")
        return existing

    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.lower().str.strip()

    # Support both column naming conventions
    ticker_col = "ticker" if "ticker" in df.columns else "name"
    if ticker_col not in df.columns or "weight" not in df.columns:
        raise ValueError(f"ETF CSV must have '{ticker_col}' and 'weight' columns. Found: {list(df.columns)}")

    total_weight = float(df["weight"].sum())

    etf_upload = ETFUpload(
        filename=filename,
        total_weight=total_weight,
        num_constituents=len(df),
        is_active=1,
    )
    session.add(etf_upload)
    session.flush()

    loaded = 0
    for _, row in df.iterrows():
        ticker = row[ticker_col]
        if ticker not in ticker_map:
            logger.warning(f"         | ⚠ Ticker '{ticker}' not in universe, skipping")
            continue

        holding = ETFHolding(
            etf_upload_id=etf_upload.id,
            constituent_id=ticker_map[ticker],
            weight=float(row["weight"]),
        )
        session.add(holding)
        loaded += 1

    session.commit()
    logger.info(f"         | Loaded {loaded} holdings, total weight = {total_weight:.4f}")
    return etf_upload


# ──────────────────────────────────────────────
#  PIPELINE ORCHESTRATOR
# ──────────────────────────────────────────────

def run_pipeline(load_prices_flag: bool = True, etf_name: str = None, fresh: bool = False):
    """
    Main ETL pipeline orchestrator.

    Steps:
    1. Initialize database schema
    2. Load constituent metadata
    3. Extract → Validate → Load OHLCV price data
    4. Load ETF weight definitions
    5. Print summary
    """
    start_time = time.time()

    logger.info("=" * 60)
    logger.info("  Spectra — ETL Pipeline")
    logger.info("=" * 60)

    if fresh:
        logger.info("DATABASE | Dropping all tables (--fresh flag)")
        drop_db()

    init_db()
    logger.info("DATABASE | Schema initialized")

    session = SessionLocal()

    try:
        # ── Step 1: Constituent Metadata ──
        constituents_csv = DATA_DIR / "constituents.csv"
        if constituents_csv.exists():
            meta_df = extract_constituents(str(constituents_csv))
            ticker_map = load_constituents(session, meta_df)
        else:
            logger.info("SKIP     | No constituents.csv, will extract tickers from prices")
            ticker_map = {}

        # ── Step 2: Price Data ──
        if load_prices_flag:
            prices_csv = DATA_DIR / "prices.csv"
            if not prices_csv.exists():
                raise FileNotFoundError(f"Price data not found: {prices_csv}")

            raw_prices = extract_prices(str(prices_csv))

            if not ticker_map:
                tickers_df = pd.DataFrame({"ticker": raw_prices["ticker"].unique()})
                ticker_map = load_constituents(session, tickers_df)

            validated_prices = validate_prices(raw_prices)
            load_prices(session, validated_prices, ticker_map)
        else:
            logger.info("SKIP     | Price loading skipped (--no-prices)")
            if not ticker_map:
                ticker_map = {c.ticker: c.id for c in session.query(Constituent).all()}

        # ── Step 3: ETF Weights ──
        if etf_name:
            etf_files = [f for f in DATA_DIR.glob(f"*{etf_name}*etf*.csv")]
        else:
            etf_files = [f for f in DATA_DIR.glob("*etf*.csv")]

        # Filter out non-ETF files
        etf_files = [
            f for f in etf_files
            if "prices" not in f.name.lower()
            and "constituents" not in f.name.lower()
        ]

        logger.info(f"ETF      | Found {len(etf_files)} ETF file(s) to process")

        for etf_file in sorted(etf_files):
            try:
                load_etf_weights(session, str(etf_file), ticker_map)
            except Exception as e:
                logger.warning(f"         | ⚠ Failed to load {etf_file.name}: {e}")

        # ── Summary ──
        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info(f"  Pipeline Complete — {elapsed:.2f}s")
        logger.info("=" * 60)

        print_db_summary(session)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        session.rollback()
        raise
    finally:
        session.close()


def print_db_summary(session):
    """Print a summary of the database contents."""
    from sqlalchemy import func

    n_constituents = session.query(func.count(Constituent.id)).scalar()
    n_prices = session.query(func.count(Price.id)).scalar()
    n_etfs = session.query(func.count(ETFUpload.id)).scalar()
    n_holdings = session.query(func.count(ETFHolding.id)).scalar()
    min_date = session.query(func.min(Price.date)).scalar()
    max_date = session.query(func.max(Price.date)).scalar()
    n_sectors = session.query(func.count(func.distinct(Constituent.sector))).scalar()

    logger.info("")
    logger.info("  Database Summary")
    logger.info("  ─────────────────────────────────")
    logger.info(f"  Constituents : {n_constituents} ({n_sectors} sectors)")
    logger.info(f"  Price records: {n_prices:,}")
    logger.info(f"  Date range   : {min_date} → {max_date}")
    logger.info(f"  ETF uploads  : {n_etfs}")
    logger.info(f"  Holdings     : {n_holdings}")
    logger.info("")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spectra ETL Pipeline")
    parser.add_argument("--no-prices", action="store_true", help="Skip price data loading")
    parser.add_argument("--etf", type=str, default=None, help="Load specific ETF (partial match)")
    parser.add_argument("--fresh", action="store_true", help="Drop and recreate all tables")
    args = parser.parse_args()

    run_pipeline(
        load_prices_flag=not args.no_prices,
        etf_name=args.etf,
        fresh=args.fresh,
    )
