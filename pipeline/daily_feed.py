"""
Daily Price Feed — Pluggable Data Source

Fetches new OHLCV records and drops them into the landing zone for
the incremental ETL to pick up.

Data Sources (--source flag):
─────────────────────────────
  simulator  — Generates synthetic prices via Geometric Brownian Motion.
               Good for testing the pipeline without network dependencies.
  yfinance   — Pulls real market data from Yahoo Finance (free, no API key).
               Uses the yfinance library.  pip install yfinance
  csv        — Reads from an external CSV file (e.g., vendor SFTP drop).
               Pass the path with --csv-path.

In production, this module would be replaced or extended with:
  - Bloomberg B-PIPE or DAPI adapter
  - Refinitiv Eikon / Workspace API
  - AWS Data Exchange subscription
  - Kafka consumer reading from a market data topic

Usage:
    python -m pipeline.daily_feed                         # Simulate 1 day
    python -m pipeline.daily_feed --days 5                # Simulate 5 days
    python -m pipeline.daily_feed --source yfinance       # Real data from Yahoo
    python -m pipeline.daily_feed --source csv --csv-path /path/to/data.csv

Landing Zone Pattern:
    data/incoming/prices_YYYY-MM-DD.csv   ← one file per batch
    data/incoming/                         ← incremental ETL picks up files here
"""

import pandas as pd
import numpy as np
import logging
import argparse
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import timedelta, date

sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.app.database import SessionLocal, Constituent, Price, init_db
from sqlalchemy import func

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
INCOMING_DIR = DATA_DIR / "incoming"


# ══════════════════════════════════════════════════════════════
#  Data Provider Interface (Strategy Pattern)
#
#  Swap the data source without changing the pipeline.
#  In production: Bloomberg, Refinitiv, or a Kafka consumer.
# ══════════════════════════════════════════════════════════════

class DataProvider(ABC):
    """
    Abstract base class for market data providers.

    Any new source just implements fetch_prices() and returns
    a DataFrame with columns: date, ticker, open, high, low, close, volume
    """

    @abstractmethod
    def fetch_prices(self, tickers: list[str], start_date: date, end_date: date) -> pd.DataFrame:
        """
        Fetch OHLCV data for the given tickers and date range.

        Returns DataFrame with columns:
            date, ticker, open, high, low, close, volume
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass


class SimulatorProvider(DataProvider):
    """
    Generates synthetic OHLCV data using Geometric Brownian Motion.

    Used for testing the pipeline without network dependencies.
    Produces statistically realistic price movements:
    - µ = 0.0002 daily drift (≈ 5% annual)
    - σ = 0.018 daily volatility (≈ 28% annual)
    """

    name = "simulator"

    def __init__(self, session):
        self.session = session

    def _get_latest_state(self) -> pd.DataFrame:
        """Query DB for most recent price per ticker."""
        subq = (
            self.session.query(
                Price.constituent_id,
                func.max(Price.date).label("max_date"),
            )
            .group_by(Price.constituent_id)
            .subquery()
        )
        rows = (
            self.session.query(
                Constituent.ticker, Price.date,
                Price.close_price, Price.volume,
            )
            .join(Price, Price.constituent_id == Constituent.id)
            .join(subq, (Price.constituent_id == subq.c.constituent_id)
                  & (Price.date == subq.c.max_date))
            .all()
        )
        return pd.DataFrame(rows, columns=["ticker", "date", "close", "volume"])

    def fetch_prices(self, tickers: list[str], start_date: date, end_date: date) -> pd.DataFrame:
        state_df = self._get_latest_state()
        if state_df.empty:
            return pd.DataFrame()

        all_records = []
        current_date = start_date

        while current_date <= end_date:
            if current_date.weekday() >= 5:  # skip weekends
                current_date += timedelta(days=1)
                continue

            records = []
            for _, row in state_df.iterrows():
                if row["ticker"] not in tickers:
                    continue
                prev_close = row["close"]
                prev_volume = row["volume"]

                # GBM: dS = µ·S·dt + σ·S·√dt·Z
                mu, sigma = 0.0002, 0.018
                shock = np.random.normal(0, 1)
                new_close = round(prev_close * (1 + mu + sigma * shock), 2)
                new_close = max(new_close, 0.01)

                intraday_range = prev_close * abs(np.random.normal(0, 0.012))
                new_high = round(max(new_close, prev_close) + abs(np.random.normal(0, 1)) * intraday_range * 0.5, 2)
                new_low = round(min(new_close, prev_close) - abs(np.random.normal(0, 1)) * intraday_range * 0.5, 2)
                new_low = max(new_low, 0.01)
                new_open = round(prev_close * (1 + np.random.normal(0, 0.003)), 2)
                new_open = max(new_open, new_low)
                new_open = min(new_open, new_high)

                vol_change = np.random.normal(0, 0.15)
                new_volume = max(int(prev_volume * (1 + vol_change)), 100_000)

                records.append({
                    "date": current_date.strftime("%Y-%m-%d"),
                    "ticker": row["ticker"],
                    "open": new_open, "high": new_high,
                    "low": new_low, "close": new_close,
                    "volume": new_volume,
                })

            day_df = pd.DataFrame(records)
            all_records.append(day_df)

            # Carry forward state for multi-day generation
            for _, r in day_df.iterrows():
                mask = state_df["ticker"] == r["ticker"]
                state_df.loc[mask, "close"] = r["close"]
                state_df.loc[mask, "volume"] = r["volume"]
                state_df.loc[mask, "date"] = current_date

            current_date += timedelta(days=1)

        return pd.concat(all_records, ignore_index=True) if all_records else pd.DataFrame()


class YFinanceProvider(DataProvider):
    """
    Fetches real market data from Yahoo Finance.

    Requires: pip install yfinance

    This is a free, no-API-key data source suitable for personal projects
    and interviews. Rate-limited but sufficient for daily batch pulls.

    In production you'd use:
    - Bloomberg B-PIPE (institutional)
    - Refinitiv Eikon API
    - Polygon.io / Alpha Vantage (retail with API key)
    """

    name = "yfinance"

    def fetch_prices(self, tickers: list[str], start_date: date, end_date: date) -> pd.DataFrame:
        try:
            import yfinance as yf
        except ImportError:
            raise ImportError(
                "yfinance not installed. Run: pip install yfinance\n"
                "Or use --source simulator for synthetic data."
            )

        logger.info(f"YAHOO    | Fetching {len(tickers)} tickers: {start_date} → {end_date}")

        # yfinance batch download — efficient single HTTP call
        raw = yf.download(
            tickers=tickers,
            start=start_date.strftime("%Y-%m-%d"),
            end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            group_by="ticker",
            auto_adjust=True,
            progress=False,
        )

        if raw.empty:
            logger.warning("YAHOO    | No data returned (market may be closed)")
            return pd.DataFrame()

        # Convert wide multi-index → long format
        records = []
        for ticker in tickers:
            try:
                if len(tickers) == 1:
                    ticker_data = raw
                else:
                    ticker_data = raw[ticker]

                for idx, row in ticker_data.iterrows():
                    if pd.isna(row.get("Close")):
                        continue
                    records.append({
                        "date": idx.strftime("%Y-%m-%d"),
                        "ticker": ticker,
                        "open": round(float(row["Open"]), 2),
                        "high": round(float(row["High"]), 2),
                        "low": round(float(row["Low"]), 2),
                        "close": round(float(row["Close"]), 2),
                        "volume": int(row["Volume"]),
                    })
            except (KeyError, TypeError):
                logger.warning(f"YAHOO    | No data for {ticker}, skipping")

        logger.info(f"YAHOO    | Retrieved {len(records)} records")
        return pd.DataFrame(records)


class CSVProvider(DataProvider):
    """
    Reads from an external CSV file (e.g., vendor SFTP drop, S3 download).

    Expected columns: date, ticker, open, high, low, close, volume

    Usage: --source csv --csv-path /path/to/vendor_data.csv
    """

    name = "csv"

    def __init__(self, csv_path: str):
        self.csv_path = csv_path

    def fetch_prices(self, tickers: list[str], start_date: date, end_date: date) -> pd.DataFrame:
        logger.info(f"CSV      | Reading from {self.csv_path}")
        df = pd.read_csv(self.csv_path, parse_dates=["date"])

        # Filter to requested tickers and date range
        df["date_parsed"] = pd.to_datetime(df["date"])
        mask = (
            df["ticker"].isin(tickers)
            & (df["date_parsed"].dt.date >= start_date)
            & (df["date_parsed"].dt.date <= end_date)
        )
        result = df.loc[mask].drop(columns=["date_parsed"])
        logger.info(f"CSV      | {len(result)} records after filtering")
        return result


# ══════════════════════════════════════════════════════════════
#  Feed Generator — writes to landing zone
# ══════════════════════════════════════════════════════════════

def get_tickers(session) -> list[str]:
    """Get all known tickers from the database."""
    return [c.ticker for c in session.query(Constituent).all()]


def get_last_date(session) -> date:
    """Get the latest date in the prices table."""
    result = session.query(func.max(Price.date)).scalar()
    return result


def generate_feed(n_days: int = 1, source: str = "simulator", csv_path: str = None):
    """
    Fetch n trading days of price data and write to landing zone.

    Each day is a separate CSV file — mimics how real data lands
    from SFTP drops, API pulls, or message queues.
    """
    init_db()
    session = SessionLocal()

    try:
        tickers = get_tickers(session)
        if not tickers:
            logger.error("No tickers in DB. Run the full ETL first.")
            return

        last_db_date = get_last_date(session)
        if not last_db_date:
            logger.error("No price data in DB. Run the full ETL first.")
            return

        logger.info(f"FEED     | Source: {source}")
        logger.info(f"FEED     | Last date in DB: {last_db_date}")
        logger.info(f"FEED     | Generating {n_days} trading day(s) of new data")

        # Create the provider
        if source == "yfinance":
            provider = YFinanceProvider()
        elif source == "csv":
            if not csv_path:
                logger.error("--csv-path required when using --source csv")
                return
            provider = CSVProvider(csv_path)
        else:
            provider = SimulatorProvider(session)

        # Calculate target date range
        start_date = last_db_date + timedelta(days=1)

        # For simulator/csv, overshoot to account for weekends
        end_estimate = start_date + timedelta(days=int(n_days * 1.5) + 5)

        # Fetch data from provider
        df = provider.fetch_prices(tickers, start_date, end_estimate)

        if df.empty:
            logger.warning("FEED     | No new data returned by provider")
            return

        # Ensure we only take up to n_days of trading days
        df["date_parsed"] = pd.to_datetime(df["date"])
        unique_days = sorted(df["date_parsed"].dt.date.unique())
        target_days = unique_days[:n_days]

        INCOMING_DIR.mkdir(parents=True, exist_ok=True)

        total_written = 0
        for day in target_days:
            day_df = df[df["date_parsed"].dt.date == day].drop(columns=["date_parsed"])

            filename = f"prices_{day.strftime('%Y-%m-%d')}.csv"
            filepath = INCOMING_DIR / filename
            day_df.to_csv(filepath, index=False)
            total_written += len(day_df)

            logger.info(f"FEED     | Generated {filename} ({len(day_df)} records)")

        logger.info(f"FEED     | Done — {len(target_days)} file(s), {total_written} records in {INCOMING_DIR}/")

    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily price feed")
    parser.add_argument("--days", type=int, default=1, help="Number of trading days to generate")
    parser.add_argument("--source", choices=["simulator", "yfinance", "csv"],
                        default="simulator", help="Data source to use")
    parser.add_argument("--csv-path", type=str, default=None,
                        help="Path to external CSV (required for --source csv)")
    args = parser.parse_args()

    generate_feed(n_days=args.days, source=args.source, csv_path=args.csv_path)
