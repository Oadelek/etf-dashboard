"""
Realistic Market Data Generator

Generates synthetic but realistic financial data:
- 50 S&P 500 tickers with sector/industry/market cap metadata
- 5+ years of daily OHLCV prices using geometric Brownian motion
- Realistic ETF portfolios (Tech Growth, Dividend Value, etc.)

The generated data is large enough to be credible in a data engineering
context (~65,000 price rows, multiple dimensions) while still fitting
in a local SQLite/Spark dev environment.

Usage:
    python pipeline/generate_data.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import json

np.random.seed(42)  # Reproducible

DATA_DIR = Path(__file__).parent.parent / "data"

# ─────────────────────────────────────────────────────────────────
#  UNIVERSE: 50 Real S&P 500 Tickers
# ─────────────────────────────────────────────────────────────────

UNIVERSE = [
    # Technology
    {"ticker": "AAPL",  "name": "Apple Inc.",                  "sector": "Technology",      "industry": "Consumer Electronics",   "market_cap": "Large",  "base_price": 130.0,  "volatility": 0.022},
    {"ticker": "MSFT",  "name": "Microsoft Corp.",             "sector": "Technology",      "industry": "Software",               "market_cap": "Large",  "base_price": 250.0,  "volatility": 0.020},
    {"ticker": "NVDA",  "name": "NVIDIA Corp.",                "sector": "Technology",      "industry": "Semiconductors",         "market_cap": "Large",  "base_price": 45.0,   "volatility": 0.035},
    {"ticker": "GOOGL", "name": "Alphabet Inc.",               "sector": "Technology",      "industry": "Internet Services",      "market_cap": "Large",  "base_price": 95.0,   "volatility": 0.023},
    {"ticker": "META",  "name": "Meta Platforms Inc.",         "sector": "Technology",      "industry": "Social Media",           "market_cap": "Large",  "base_price": 175.0,  "volatility": 0.030},
    {"ticker": "AMZN",  "name": "Amazon.com Inc.",             "sector": "Technology",      "industry": "E-Commerce",             "market_cap": "Large",  "base_price": 105.0,  "volatility": 0.025},
    {"ticker": "CRM",   "name": "Salesforce Inc.",             "sector": "Technology",      "industry": "Cloud Computing",        "market_cap": "Large",  "base_price": 160.0,  "volatility": 0.026},
    {"ticker": "ORCL",  "name": "Oracle Corp.",                "sector": "Technology",      "industry": "Enterprise Software",    "market_cap": "Large",  "base_price": 80.0,   "volatility": 0.021},
    {"ticker": "ADBE",  "name": "Adobe Inc.",                  "sector": "Technology",      "industry": "Software",               "market_cap": "Large",  "base_price": 350.0,  "volatility": 0.025},
    {"ticker": "INTC",  "name": "Intel Corp.",                 "sector": "Technology",      "industry": "Semiconductors",         "market_cap": "Large",  "base_price": 28.0,   "volatility": 0.028},

    # Healthcare
    {"ticker": "JNJ",   "name": "Johnson & Johnson",          "sector": "Healthcare",      "industry": "Pharmaceuticals",        "market_cap": "Large",  "base_price": 155.0,  "volatility": 0.012},
    {"ticker": "UNH",   "name": "UnitedHealth Group",         "sector": "Healthcare",      "industry": "Health Insurance",       "market_cap": "Large",  "base_price": 470.0,  "volatility": 0.016},
    {"ticker": "PFE",   "name": "Pfizer Inc.",                "sector": "Healthcare",      "industry": "Pharmaceuticals",        "market_cap": "Large",  "base_price": 38.0,   "volatility": 0.018},
    {"ticker": "ABBV",  "name": "AbbVie Inc.",                "sector": "Healthcare",      "industry": "Biotechnology",          "market_cap": "Large",  "base_price": 140.0,  "volatility": 0.017},
    {"ticker": "MRK",   "name": "Merck & Co.",                "sector": "Healthcare",      "industry": "Pharmaceuticals",        "market_cap": "Large",  "base_price": 105.0,  "volatility": 0.015},
    {"ticker": "TMO",   "name": "Thermo Fisher Scientific",   "sector": "Healthcare",      "industry": "Life Sciences",          "market_cap": "Large",  "base_price": 530.0,  "volatility": 0.019},

    # Financials
    {"ticker": "JPM",   "name": "JPMorgan Chase & Co.",       "sector": "Financials",      "industry": "Banking",                "market_cap": "Large",  "base_price": 140.0,  "volatility": 0.019},
    {"ticker": "BAC",   "name": "Bank of America Corp.",      "sector": "Financials",      "industry": "Banking",                "market_cap": "Large",  "base_price": 32.0,   "volatility": 0.022},
    {"ticker": "GS",    "name": "Goldman Sachs Group",        "sector": "Financials",      "industry": "Investment Banking",     "market_cap": "Large",  "base_price": 340.0,  "volatility": 0.023},
    {"ticker": "MS",    "name": "Morgan Stanley",             "sector": "Financials",      "industry": "Investment Banking",     "market_cap": "Large",  "base_price": 85.0,   "volatility": 0.024},
    {"ticker": "BLK",   "name": "BlackRock Inc.",             "sector": "Financials",      "industry": "Asset Management",       "market_cap": "Large",  "base_price": 700.0,  "volatility": 0.020},
    {"ticker": "V",     "name": "Visa Inc.",                  "sector": "Financials",      "industry": "Payments",               "market_cap": "Large",  "base_price": 230.0,  "volatility": 0.015},

    # Consumer
    {"ticker": "PG",    "name": "Procter & Gamble Co.",       "sector": "Consumer Staples", "industry": "Household Products",    "market_cap": "Large",  "base_price": 145.0,  "volatility": 0.011},
    {"ticker": "KO",    "name": "Coca-Cola Co.",              "sector": "Consumer Staples", "industry": "Beverages",             "market_cap": "Large",  "base_price": 58.0,   "volatility": 0.010},
    {"ticker": "PEP",   "name": "PepsiCo Inc.",               "sector": "Consumer Staples", "industry": "Beverages",             "market_cap": "Large",  "base_price": 170.0,  "volatility": 0.011},
    {"ticker": "COST",  "name": "Costco Wholesale",           "sector": "Consumer Staples", "industry": "Retail",                "market_cap": "Large",  "base_price": 510.0,  "volatility": 0.016},
    {"ticker": "WMT",   "name": "Walmart Inc.",               "sector": "Consumer Staples", "industry": "Retail",                "market_cap": "Large",  "base_price": 155.0,  "volatility": 0.013},
    {"ticker": "MCD",   "name": "McDonald's Corp.",           "sector": "Consumer Disc.",   "industry": "Restaurants",            "market_cap": "Large",  "base_price": 265.0,  "volatility": 0.013},
    {"ticker": "NKE",   "name": "Nike Inc.",                  "sector": "Consumer Disc.",   "industry": "Apparel",                "market_cap": "Large",  "base_price": 110.0,  "volatility": 0.022},
    {"ticker": "SBUX",  "name": "Starbucks Corp.",            "sector": "Consumer Disc.",   "industry": "Restaurants",            "market_cap": "Large",  "base_price": 98.0,   "volatility": 0.020},

    # Energy
    {"ticker": "XOM",   "name": "Exxon Mobil Corp.",          "sector": "Energy",          "industry": "Oil & Gas",              "market_cap": "Large",  "base_price": 95.0,   "volatility": 0.020},
    {"ticker": "CVX",   "name": "Chevron Corp.",              "sector": "Energy",          "industry": "Oil & Gas",              "market_cap": "Large",  "base_price": 150.0,  "volatility": 0.019},
    {"ticker": "COP",   "name": "ConocoPhillips",             "sector": "Energy",          "industry": "Oil & Gas",              "market_cap": "Large",  "base_price": 105.0,  "volatility": 0.025},
    {"ticker": "SLB",   "name": "Schlumberger Ltd.",          "sector": "Energy",          "industry": "Oilfield Services",      "market_cap": "Large",  "base_price": 48.0,   "volatility": 0.027},

    # Industrials
    {"ticker": "CAT",   "name": "Caterpillar Inc.",           "sector": "Industrials",     "industry": "Machinery",              "market_cap": "Large",  "base_price": 240.0,  "volatility": 0.021},
    {"ticker": "BA",    "name": "Boeing Co.",                 "sector": "Industrials",     "industry": "Aerospace",              "market_cap": "Large",  "base_price": 195.0,  "volatility": 0.028},
    {"ticker": "UNP",   "name": "Union Pacific Corp.",        "sector": "Industrials",     "industry": "Railroads",              "market_cap": "Large",  "base_price": 210.0,  "volatility": 0.018},
    {"ticker": "HON",   "name": "Honeywell International",   "sector": "Industrials",     "industry": "Conglomerates",          "market_cap": "Large",  "base_price": 195.0,  "volatility": 0.017},
    {"ticker": "DE",    "name": "Deere & Company",            "sector": "Industrials",     "industry": "Machinery",              "market_cap": "Large",  "base_price": 370.0,  "volatility": 0.022},

    # Real Estate / Utilities
    {"ticker": "AMT",   "name": "American Tower Corp.",       "sector": "Real Estate",     "industry": "REITs",                  "market_cap": "Large",  "base_price": 210.0,  "volatility": 0.018},
    {"ticker": "NEE",   "name": "NextEra Energy Inc.",        "sector": "Utilities",       "industry": "Electric Utilities",     "market_cap": "Large",  "base_price": 72.0,   "volatility": 0.016},
    {"ticker": "DUK",   "name": "Duke Energy Corp.",          "sector": "Utilities",       "industry": "Electric Utilities",     "market_cap": "Large",  "base_price": 98.0,   "volatility": 0.012},
    {"ticker": "SO",    "name": "Southern Company",           "sector": "Utilities",       "industry": "Electric Utilities",     "market_cap": "Large",  "base_price": 70.0,   "volatility": 0.011},

    # Materials / Comms
    {"ticker": "LIN",   "name": "Linde plc",                  "sector": "Materials",       "industry": "Chemicals",              "market_cap": "Large",  "base_price": 360.0,  "volatility": 0.016},
    {"ticker": "APD",   "name": "Air Products & Chemicals",   "sector": "Materials",       "industry": "Chemicals",              "market_cap": "Large",  "base_price": 280.0,  "volatility": 0.017},
    {"ticker": "DIS",   "name": "Walt Disney Co.",            "sector": "Communication",   "industry": "Entertainment",          "market_cap": "Large",  "base_price": 95.0,   "volatility": 0.024},
    {"ticker": "NFLX",  "name": "Netflix Inc.",               "sector": "Communication",   "industry": "Streaming",              "market_cap": "Large",  "base_price": 320.0,  "volatility": 0.032},
    {"ticker": "T",     "name": "AT&T Inc.",                  "sector": "Communication",   "industry": "Telecom",                "market_cap": "Large",  "base_price": 18.0,   "volatility": 0.015},
    {"ticker": "TMUS",  "name": "T-Mobile US Inc.",           "sector": "Communication",   "industry": "Telecom",                "market_cap": "Large",  "base_price": 145.0,  "volatility": 0.018},
    {"ticker": "PYPL",  "name": "PayPal Holdings Inc.",       "sector": "Technology",      "industry": "Fintech",                "market_cap": "Large",  "base_price": 65.0,   "volatility": 0.030},
]


# ─────────────────────────────────────────────────────────────────
#  PRICE GENERATION: Geometric Brownian Motion
# ─────────────────────────────────────────────────────────────────

def generate_trading_days(start: str = "2019-01-02", end: str = "2025-12-31") -> list:
    """Generate realistic US trading days (skip weekends + major holidays)."""
    all_days = pd.bdate_range(start=start, end=end)  # Business days

    # Remove major US holidays (approximate)
    holidays = set()
    for year in range(2019, 2026):
        holidays.update([
            f"{year}-01-01",  # New Year
            f"{year}-01-20" if year >= 2020 else f"{year}-01-21",  # MLK
            f"{year}-07-04",  # Independence Day
            f"{year}-12-25",  # Christmas
            f"{year}-11-28" if year == 2019 else f"{year}-11-26",  # Thanksgiving (approx)
        ])

    trading_days = [d for d in all_days if d.strftime("%Y-%m-%d") not in holidays]
    return trading_days


def generate_ohlcv(ticker_info: dict, trading_days: list) -> pd.DataFrame:
    """
    Generate realistic OHLCV data using Geometric Brownian Motion.

    GBM formula: S(t+1) = S(t) * exp((mu - sigma²/2)*dt + sigma*sqrt(dt)*Z)

    Also generates:
    - Open/High/Low from the close with realistic intraday ranges
    - Volume with realistic patterns (higher on volatile days)
    """
    n_days = len(trading_days)
    ticker = ticker_info["ticker"]
    base_price = ticker_info["base_price"]
    sigma = ticker_info["volatility"]

    # Drift: slight upward bias (0-8% annualized, varies by ticker)
    mu = np.random.uniform(0.0, 0.08) / 252  # Daily drift

    # Generate daily returns using GBM
    dt = 1.0 / 252
    Z = np.random.standard_normal(n_days)
    log_returns = (mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * Z

    # Add occasional market shocks (fat tails, ~2% of days)
    shock_mask = np.random.random(n_days) < 0.02
    shocks = np.random.normal(0, sigma * 3, n_days)
    log_returns[shock_mask] += shocks[shock_mask]

    # Add sector-correlated moves (market-wide moves ~40% of variance)
    market_factor = np.random.standard_normal(n_days) * sigma * 0.4
    log_returns += market_factor * 0.5

    # Compute closing prices
    close_prices = np.zeros(n_days)
    close_prices[0] = base_price
    for i in range(1, n_days):
        close_prices[i] = close_prices[i - 1] * np.exp(log_returns[i])

    # Ensure no negative prices (floor at $1)
    close_prices = np.maximum(close_prices, 1.0)

    # Generate OHLV from Close
    intraday_range = sigma * np.random.uniform(0.3, 1.5, n_days)

    high_prices = close_prices * (1 + np.abs(np.random.normal(0, intraday_range)))
    low_prices = close_prices * (1 - np.abs(np.random.normal(0, intraday_range)))
    open_prices = low_prices + (high_prices - low_prices) * np.random.uniform(0.2, 0.8, n_days)

    # Ensure OHLC consistency: Low <= Open,Close <= High
    low_prices = np.minimum(low_prices, np.minimum(open_prices, close_prices))
    high_prices = np.maximum(high_prices, np.maximum(open_prices, close_prices))

    # Volume: base volume scaled by volatility
    avg_volume = np.random.uniform(5_000_000, 80_000_000)
    volume_noise = np.random.lognormal(0, 0.5, n_days)
    # Higher volume on big move days
    move_magnitude = np.abs(log_returns)
    volume_spike = 1 + move_magnitude * 20
    volumes = (avg_volume * volume_noise * volume_spike).astype(int)

    return pd.DataFrame({
        "date": trading_days,
        "ticker": ticker,
        "open": np.round(open_prices, 2),
        "high": np.round(high_prices, 2),
        "low": np.round(low_prices, 2),
        "close": np.round(close_prices, 2),
        "volume": volumes,
    })


# ─────────────────────────────────────────────────────────────────
#  ETF DEFINITIONS: Realistic themed portfolios
# ─────────────────────────────────────────────────────────────────

ETFS = {
    "tech_growth_etf.csv": {
        "description": "Technology Growth ETF — concentrated in high-growth tech names",
        "holdings": {
            "AAPL": 0.15, "MSFT": 0.14, "NVDA": 0.12, "GOOGL": 0.10, "META": 0.09,
            "AMZN": 0.08, "CRM": 0.06, "ADBE": 0.05, "NFLX": 0.05, "PYPL": 0.04,
            "ORCL": 0.04, "INTC": 0.03, "TMUS": 0.03, "DIS": 0.02,
        },
    },
    "dividend_value_etf.csv": {
        "description": "Dividend Value ETF — stable, income-generating blue chips",
        "holdings": {
            "JNJ": 0.10, "PG": 0.09, "KO": 0.08, "PEP": 0.08, "XOM": 0.07,
            "CVX": 0.07, "V": 0.06, "MCD": 0.06, "WMT": 0.05, "DUK": 0.05,
            "SO": 0.05, "T": 0.05, "NEE": 0.05, "JPM": 0.05, "MRK": 0.04,
            "HON": 0.03, "AMT": 0.02,
        },
    },
    "sp500_top20_etf.csv": {
        "description": "S&P 500 Top 20 — market-cap weighted top 20 holdings",
        "holdings": {
            "AAPL": 0.12, "MSFT": 0.11, "AMZN": 0.07, "NVDA": 0.07, "GOOGL": 0.06,
            "META": 0.05, "UNH": 0.04, "JNJ": 0.04, "JPM": 0.04, "V": 0.04,
            "PG": 0.04, "XOM": 0.03, "BAC": 0.03, "MRK": 0.03, "ABBV": 0.03,
            "KO": 0.03, "PFE": 0.03, "COST": 0.03, "WMT": 0.03, "CAT": 0.03,
            "TMO": 0.02, "LIN": 0.02,
        },
    },
    "energy_sector_etf.csv": {
        "description": "Energy Sector ETF — focused on oil, gas, and energy infrastructure",
        "holdings": {
            "XOM": 0.25, "CVX": 0.22, "COP": 0.18, "SLB": 0.12,
            "NEE": 0.08, "DUK": 0.08, "SO": 0.07,
        },
    },
    "balanced_multi_sector_etf.csv": {
        "description": "Balanced Multi-Sector ETF — diversified across all sectors",
        "holdings": {
            "AAPL": 0.05, "MSFT": 0.05, "GOOGL": 0.04, "JNJ": 0.04, "UNH": 0.04,
            "JPM": 0.04, "BAC": 0.03, "V": 0.04, "PG": 0.04, "KO": 0.03,
            "XOM": 0.03, "CVX": 0.03, "CAT": 0.03, "BA": 0.03, "UNP": 0.03,
            "HON": 0.03, "AMT": 0.03, "NEE": 0.03, "LIN": 0.03, "DIS": 0.03,
            "NKE": 0.03, "SBUX": 0.02, "COST": 0.03, "MCD": 0.03, "NFLX": 0.02,
            "GS": 0.02, "BLK": 0.02, "DE": 0.02, "APD": 0.02, "PYPL": 0.02,
            "T": 0.02, "PFE": 0.02,
        },
    },
}


# ─────────────────────────────────────────────────────────────────
#  MAIN GENERATOR
# ─────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Generating Realistic Market Data")
    print("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Generate constituent metadata
    print("\n[1/4] Writing constituent metadata...")
    meta_df = pd.DataFrame([
        {
            "ticker": s["ticker"],
            "name": s["name"],
            "sector": s["sector"],
            "industry": s["industry"],
            "market_cap": s["market_cap"],
        }
        for s in UNIVERSE
    ])
    meta_path = DATA_DIR / "constituents.csv"
    meta_df.to_csv(meta_path, index=False)
    print(f"       {len(meta_df)} constituents → {meta_path.name}")

    # 2. Generate OHLCV price data
    print("\n[2/4] Generating OHLCV price data (5+ years)...")
    trading_days = generate_trading_days("2019-01-02", "2025-12-31")
    print(f"       {len(trading_days)} trading days (2019-01-02 → 2025-12-31)")

    all_prices = []
    for i, stock in enumerate(UNIVERSE):
        ticker_df = generate_ohlcv(stock, trading_days)
        all_prices.append(ticker_df)
        if (i + 1) % 10 == 0:
            print(f"       Generated {i + 1}/{len(UNIVERSE)} tickers...")

    prices_df = pd.concat(all_prices, ignore_index=True)
    prices_path = DATA_DIR / "prices.csv"
    prices_df.to_csv(prices_path, index=False)
    print(f"       {len(prices_df):,} total price records → {prices_path.name}")
    print(f"       File size: {prices_path.stat().st_size / (1024*1024):.1f} MB")

    # 3. Generate ETF weight files
    print("\n[3/4] Writing ETF portfolio files...")
    for filename, etf_info in ETFS.items():
        etf_df = pd.DataFrame([
            {"ticker": ticker, "weight": weight}
            for ticker, weight in etf_info["holdings"].items()
        ])
        etf_path = DATA_DIR / filename
        etf_df.to_csv(etf_path, index=False)
        total_w = etf_df["weight"].sum()
        print(f"       {filename}: {len(etf_df)} holdings, weight={total_w:.2f}")

    # 4. Summary
    print(f"\n{'=' * 60}")
    print(f"  Generation Complete!")
    print(f"{'=' * 60}")
    print(f"  Constituents : {len(UNIVERSE)}")
    print(f"  Trading days : {len(trading_days)}")
    print(f"  Price records: {len(prices_df):,}")
    print(f"  ETF files    : {len(ETFS)}")
    print(f"  Output dir   : {DATA_DIR}")
    print()


if __name__ == "__main__":
    main()
