"""
PySpark Analytics for ETF Price Data

Demonstrates distributed data processing patterns using PySpark.
While this dataset is small enough for pandas, the code patterns
here scale to billions of rows on Hadoop/Spark clusters.

Data Format:
────────────
Input CSV (long format): date, ticker, open, high, low, close, volume

Analytics performed:
────────────────────
1. Data profiling & schema inspection
2. Moving averages (5-day, 20-day) using Window functions
3. Daily returns and volatility analysis
4. Constituent performance ranking (total return)
5. Volume analysis — average daily volume, volume spikes
6. Sector-level aggregation (if constituents.csv present)
7. Price correlation matrix (sample pairs)

Usage:
    python -m pipeline.spark_analysis

Requires: pyspark (pip install pyspark)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "spark_output"


def create_spark_session() -> SparkSession:
    """Create a local Spark session for development."""
    return (
        SparkSession.builder
        .appName("Spectra-Analytics")
        .master("local[*]")  # Use all available cores
        .config("spark.sql.shuffle.partitions", "4")   # Small dataset
        .config("spark.driver.memory", "1g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def load_prices(spark: SparkSession):
    """
    Load the long-format OHLCV price CSV into a Spark DataFrame.

    Expected columns: date, ticker, open, high, low, close, volume
    """
    csv_path = str(DATA_DIR / "prices.csv")

    df = (
        spark.read.csv(csv_path, header=True, inferSchema=True)
        .withColumn("date", F.to_date("date"))
    )

    print(f"\n{'=' * 60}")
    print("  1. Raw Price Data (Long Format OHLCV)")
    print(f"{'=' * 60}")
    print(f"  Rows: {df.count():,}")
    print(f"  Tickers: {df.select('ticker').distinct().count()}")
    date_range = df.agg(F.min("date"), F.max("date")).collect()[0]
    print(f"  Date Range: {date_range[0]} → {date_range[1]}")
    df.printSchema()
    df.show(10, truncate=False)
    return df


def load_constituents(spark: SparkSession):
    """Load constituent metadata (ticker, name, sector, industry, market_cap)."""
    csv_path = str(DATA_DIR / "constituents.csv")
    if not Path(csv_path).exists():
        print("  [WARN] constituents.csv not found — skipping sector analysis")
        return None

    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    print(f"\n  Loaded {df.count()} constituents with sector metadata")
    return df


def compute_moving_averages(long_df):
    """
    Compute 5-day and 20-day moving averages per constituent on close price.

    Uses Spark Window functions — same concept as SQL's
    AVG(...) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN).
    """
    print(f"\n{'=' * 60}")
    print("  2. Moving Averages (5-day & 20-day)")
    print(f"{'=' * 60}")

    window_5 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-4, 0)
    )
    window_20 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-19, 0)
    )

    ma_df = (
        long_df
        .withColumn("ma_5", F.round(F.avg("close").over(window_5), 2))
        .withColumn("ma_20", F.round(F.avg("close").over(window_20), 2))
    )

    sample_ticker = long_df.select("ticker").distinct().first()[0]
    print(f"  Example: Ticker {sample_ticker}")
    ma_df.filter(F.col("ticker") == sample_ticker).show(15)
    return ma_df


def compute_daily_returns(long_df):
    """
    Compute daily returns and aggregate volatility per constituent.

    daily_return = (close_today − close_yesterday) / close_yesterday × 100
    volatility   = standard deviation of daily returns (higher = riskier)
    """
    print(f"\n{'=' * 60}")
    print("  3. Daily Returns & Volatility")
    print(f"{'=' * 60}")

    window = Window.partitionBy("ticker").orderBy("date")

    returns_df = (
        long_df
        .withColumn("prev_close", F.lag("close").over(window))
        .withColumn(
            "daily_return",
            F.round(
                (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100,
                2,
            ),
        )
        .filter(F.col("prev_close").isNotNull())
    )

    volatility_df = (
        returns_df.groupBy("ticker")
        .agg(
            F.round(F.stddev("daily_return"), 4).alias("volatility"),
            F.round(F.avg("daily_return"), 4).alias("avg_daily_return"),
            F.round(F.min("daily_return"), 2).alias("worst_day_pct"),
            F.round(F.max("daily_return"), 2).alias("best_day_pct"),
            F.count("daily_return").alias("trading_days"),
        )
        .orderBy("volatility", ascending=False)
    )

    print("  Volatility Ranking (highest risk → lowest):")
    volatility_df.show(20)

    return returns_df, volatility_df


def compute_performance_ranking(long_df):
    """
    Rank all constituents by total return over the full period.

    total_return = (last_close − first_close) / first_close × 100

    Uses ROW_NUMBER window function to identify first/last rows.
    """
    print(f"\n{'=' * 60}")
    print("  4. Performance Ranking (Total Return)")
    print(f"{'=' * 60}")

    window_asc = Window.partitionBy("ticker").orderBy("date")
    window_desc = Window.partitionBy("ticker").orderBy(F.col("date").desc())

    perf_df = (
        long_df
        .withColumn("row_asc", F.row_number().over(window_asc))
        .withColumn("row_desc", F.row_number().over(window_desc))
    )

    first_prices = perf_df.filter(F.col("row_asc") == 1).select(
        "ticker", F.col("close").alias("first_price")
    )
    last_prices = perf_df.filter(F.col("row_desc") == 1).select(
        "ticker", F.col("close").alias("last_price")
    )

    ranking = (
        first_prices.join(last_prices, "ticker")
        .withColumn(
            "total_return_pct",
            F.round(
                (F.col("last_price") - F.col("first_price"))
                / F.col("first_price")
                * 100,
                2,
            ),
        )
        .withColumn(
            "rank",
            F.row_number().over(
                Window.orderBy(F.col("total_return_pct").desc())
            ),
        )
        .orderBy("rank")
    )

    print("  All constituents ranked by total return:")
    ranking.show(50)
    return ranking


def compute_volume_analysis(long_df):
    """
    Analyse trading volume — average daily volume per ticker,
    identify volume spikes (days > 2× the 20-day average).
    """
    print(f"\n{'=' * 60}")
    print("  5. Volume Analysis — Spikes & Leaders")
    print(f"{'=' * 60}")

    avg_vol = (
        long_df.groupBy("ticker")
        .agg(
            F.round(F.avg("volume"), 0).alias("avg_daily_volume"),
            F.round(F.max("volume"), 0).alias("max_daily_volume"),
        )
        .orderBy("avg_daily_volume", ascending=False)
    )

    print("  Average Daily Volume Leaders:")
    avg_vol.show(15)

    vol_window = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-19, 0)
    )
    spikes_df = (
        long_df
        .withColumn("vol_ma_20", F.avg("volume").over(vol_window))
        .withColumn("vol_ratio", F.round(F.col("volume") / F.col("vol_ma_20"), 2))
        .filter(F.col("vol_ratio") > 2.0)
        .select("date", "ticker", "volume", "vol_ma_20", "vol_ratio", "close")
        .orderBy(F.col("vol_ratio").desc())
    )

    spike_count = spikes_df.count()
    print(f"  Volume Spikes (>2× 20-day avg): {spike_count:,} occurrences")
    spikes_df.show(15)

    return avg_vol


def compute_sector_aggregation(long_df, constituents_df):
    """
    Sector-level analysis: average return, volatility, volume by sector.

    Demonstrates Spark JOIN + GROUP BY with multiple aggregations.
    """
    print(f"\n{'=' * 60}")
    print("  6. Sector-Level Aggregation")
    print(f"{'=' * 60}")

    if constituents_df is None:
        print("  [SKIP] No constituent metadata available")
        return None

    window = Window.partitionBy("ticker").orderBy("date")
    returns_df = (
        long_df
        .withColumn("prev_close", F.lag("close").over(window))
        .withColumn(
            "daily_return",
            (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100,
        )
        .filter(F.col("prev_close").isNotNull())
    )

    enriched = returns_df.join(constituents_df, "ticker", "inner")

    sector_stats = (
        enriched.groupBy("sector")
        .agg(
            F.countDistinct("ticker").alias("num_tickers"),
            F.round(F.avg("daily_return"), 4).alias("avg_daily_return"),
            F.round(F.stddev("daily_return"), 4).alias("avg_volatility"),
            F.round(F.avg("volume"), 0).alias("avg_daily_volume"),
            F.round(F.sum("volume"), 0).alias("total_volume"),
        )
        .orderBy("avg_daily_return", ascending=False)
    )

    print("  Sector Performance Summary:")
    sector_stats.show(20, truncate=False)
    return sector_stats


def compute_correlation_matrix(long_df, spark):
    """
    Compute price correlations between select pairs of real tickers.

    In production you'd compute the full N×N matrix using
    Spark ML's Correlation class; here we show pairwise.
    """
    print(f"\n{'=' * 60}")
    print("  7. Price Correlations (sample pairs)")
    print(f"{'=' * 60}")

    wide_df = long_df.groupBy("date").pivot("ticker").agg(F.first("close"))

    tickers = sorted([col for col in wide_df.columns if col != "date"])

    pairs = [
        ("AAPL", "MSFT"),
        ("AAPL", "XOM"),
        ("JPM", "GS"),
        ("JNJ", "PFE"),
        ("AMZN", "WMT"),
        ("TSLA", "F"),
        ("GOOGL", "META"),
        ("CVX", "XOM"),
    ]

    print(f"\n  {'Ticker A':>10} {'Ticker B':>10} {'Correlation':>15}")
    print(f"  {'-' * 40}")

    for t1, t2 in pairs:
        if t1 in tickers and t2 in tickers:
            corr = wide_df.stat.corr(t1, t2)
            print(f"  {t1:>10} {t2:>10} {corr:>15.4f}")


def save_results(volatility_df, ranking_df, volume_df):
    """Save analysis results to CSV for downstream consumption."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    vol_path = str(OUTPUT_DIR / "volatility_analysis.csv")
    rank_path = str(OUTPUT_DIR / "performance_ranking.csv")
    volume_path = str(OUTPUT_DIR / "volume_leaders.csv")

    volatility_df.toPandas().to_csv(vol_path, index=False)
    ranking_df.toPandas().to_csv(rank_path, index=False)
    volume_df.toPandas().to_csv(volume_path, index=False)

    print(f"\n  Results saved to {OUTPUT_DIR}/")


def main():
    """Run the full PySpark analytics pipeline."""
    print("\n" + "=" * 60)
    print("  Spectra — PySpark Analytics")
    print("  Data: Long-format OHLCV with real S&P 500 tickers")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # Load data
        prices_df = load_prices(spark)
        constituents_df = load_constituents(spark)

        # Analytics
        compute_moving_averages(prices_df)
        returns_df, volatility_df = compute_daily_returns(prices_df)
        ranking_df = compute_performance_ranking(prices_df)
        volume_df = compute_volume_analysis(prices_df)
        compute_sector_aggregation(prices_df, constituents_df)
        compute_correlation_matrix(prices_df, spark)

        # Save outputs
        save_results(volatility_df, ranking_df, volume_df)

        print(f"\n{'=' * 60}")
        print("  Analysis Complete!")
        print(f"{'=' * 60}\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
