"""
Database-Powered ETF Service

This service demonstrates SQL proficiency by implementing ETF analytics
using both SQLAlchemy ORM queries and raw SQL.

SQL concepts demonstrated throughout this module:
─────────────────────────────────────────────────
  • JOINs          — INNER JOIN across 3 tables
  • Window funcs   — ROW_NUMBER, LAG, AVG OVER (ROWS BETWEEN)
  • CTEs           — WITH ... AS for readable multi-step queries
  • Aggregations   — SUM, AVG, MIN, MAX, COUNT, STDDEV
  • GROUP BY       — with HAVING, multi-column grouping
  • Subqueries     — correlated and non-correlated
  • Date filtering  — range-based WHERE clauses
  • CASE           — conditional logic in SELECT
"""

from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Optional

from ..database import Constituent, Price, ETFUpload, ETFHolding


class DBService:
    """Spectra SQL service — ETF analytics powered by raw SQL."""

    def __init__(self, session: Session):
        self.session = session

    # ──────────────────────────────────────────────
    #  ORM-BASED QUERIES
    # ──────────────────────────────────────────────

    def get_active_etf(self) -> Optional[ETFUpload]:
        """Get the most recently uploaded active ETF."""
        return (
            self.session.query(ETFUpload)
            .filter(ETFUpload.is_active == 1)
            .order_by(ETFUpload.uploaded_at.desc())
            .first()
        )

    def get_all_etfs(self) -> list[dict]:
        """List all uploaded ETFs with metadata."""
        etfs = (
            self.session.query(ETFUpload)
            .order_by(ETFUpload.uploaded_at.desc())
            .all()
        )
        return [
            {
                "id": e.id,
                "filename": e.filename,
                "uploaded_at": e.uploaded_at.isoformat() if e.uploaded_at else None,
                "total_weight": e.total_weight,
                "num_constituents": e.num_constituents,
                "is_active": bool(e.is_active),
            }
            for e in etfs
        ]

    # ──────────────────────────────────────────────
    #  RAW SQL QUERIES — Core Dashboard Data
    # ──────────────────────────────────────────────

    def get_holdings_sql(self, etf_id: int) -> list[dict]:
        """
        Get holdings with latest prices using a SQL JOIN.

        SQL: INNER JOIN (3 tables), correlated subquery for latest date, ORDER BY
        """
        sql = text("""
            SELECT
                c.ticker                             AS name,
                c.name                               AS company_name,
                c.sector,
                h.weight,
                p.close_price                        AS latest_price,
                ROUND(h.weight * p.close_price, 2)   AS holding_value,
                p.volume,
                p.date                               AS price_date
            FROM etf_holdings h
            INNER JOIN constituents c  ON c.id = h.constituent_id
            INNER JOIN prices p        ON p.constituent_id = c.id
            WHERE h.etf_upload_id = :etf_id
              AND p.date = (SELECT MAX(date) FROM prices)
            ORDER BY h.weight DESC
        """)

        rows = self.session.execute(sql, {"etf_id": etf_id}).fetchall()
        return [
            {
                "name": row.name,
                "company_name": row.company_name,
                "sector": row.sector,
                "weight": row.weight,
                "latest_price": row.latest_price,
                "holding_value": row.holding_value,
                "volume": row.volume,
                "price_date": str(row.price_date),
            }
            for row in rows
        ]

    def get_etf_time_series_sql(self, etf_id: int) -> list[dict]:
        """
        Compute weighted ETF price per day using SQL aggregation.

        ETF_price(t) = SUM(weight_i * price_i(t))

        SQL: INNER JOIN, SUM + GROUP BY, ROUND, ORDER BY
        """
        sql = text("""
            SELECT
                p.date,
                ROUND(SUM(h.weight * p.close_price), 2) AS price
            FROM etf_holdings h
            INNER JOIN constituents c  ON c.id = h.constituent_id
            INNER JOIN prices p        ON p.constituent_id = c.id
            WHERE h.etf_upload_id = :etf_id
            GROUP BY p.date
            ORDER BY p.date ASC
        """)

        rows = self.session.execute(sql, {"etf_id": etf_id}).fetchall()
        return [{"date": str(row.date), "price": row.price} for row in rows]

    def get_top_holdings_sql(self, etf_id: int, n: int = 5) -> list[dict]:
        """
        Top N holdings ranked by value using a CTE + window function.

        SQL: CTE (WITH), ROW_NUMBER() OVER, subquery, LIMIT equivalent
        """
        sql = text("""
            WITH ranked_holdings AS (
                SELECT
                    c.ticker                             AS name,
                    h.weight,
                    p.close_price                        AS latest_price,
                    ROUND(h.weight * p.close_price, 2)   AS holding_value,
                    ROW_NUMBER() OVER (
                        ORDER BY h.weight * p.close_price DESC
                    ) AS rank
                FROM etf_holdings h
                INNER JOIN constituents c  ON c.id = h.constituent_id
                INNER JOIN prices p        ON p.constituent_id = c.id
                WHERE h.etf_upload_id = :etf_id
                  AND p.date = (SELECT MAX(date) FROM prices)
            )
            SELECT name, weight, latest_price, holding_value, rank
            FROM ranked_holdings
            WHERE rank <= :n
            ORDER BY rank
        """)

        rows = self.session.execute(sql, {"etf_id": etf_id, "n": n}).fetchall()
        return [
            {
                "name": row.name,
                "weight": row.weight,
                "latest_price": row.latest_price,
                "holding_value": row.holding_value,
            }
            for row in rows
        ]

    # ──────────────────────────────────────────────
    #  RAW SQL QUERIES — Analytics
    # ──────────────────────────────────────────────

    def get_price_summary_sql(self) -> list[dict]:
        """
        Aggregate price statistics per constituent with sector data.

        SQL: GROUP BY, aggregate functions (AVG, MIN, MAX, COUNT), ROUND,
             additional JOIN for sector metadata, volume aggregation
        """
        sql = text("""
            SELECT
                c.ticker,
                c.name       AS company_name,
                c.sector,
                c.industry,
                COUNT(p.id)                                              AS trading_days,
                ROUND(MIN(p.close_price), 2)                             AS min_price,
                ROUND(MAX(p.close_price), 2)                             AS max_price,
                ROUND(AVG(p.close_price), 2)                             AS avg_price,
                ROUND(MAX(p.close_price) - MIN(p.close_price), 2)        AS price_range,
                ROUND(
                    (MAX(p.close_price) - MIN(p.close_price))
                    / NULLIF(MIN(p.close_price), 0) * 100,
                    2
                ) AS range_pct,
                CAST(AVG(p.volume) AS INTEGER)                           AS avg_volume
            FROM constituents c
            INNER JOIN prices p ON p.constituent_id = c.id
            GROUP BY c.ticker, c.name, c.sector, c.industry
            HAVING COUNT(p.id) > 0
            ORDER BY avg_price DESC
        """)

        rows = self.session.execute(sql).fetchall()
        return [
            {
                "ticker": row.ticker,
                "company_name": row.company_name,
                "sector": row.sector,
                "industry": row.industry,
                "trading_days": row.trading_days,
                "min_price": row.min_price,
                "max_price": row.max_price,
                "avg_price": row.avg_price,
                "price_range": row.price_range,
                "range_pct": row.range_pct,
                "avg_volume": row.avg_volume,
            }
            for row in rows
        ]

    def get_moving_averages_sql(self, ticker: str, window: int = 5) -> list[dict]:
        """
        Calculate moving averages and daily returns using window functions.

        SQL: AVG OVER (ROWS BETWEEN), LAG, window frame specification
        """
        sql = text("""
            SELECT
                p.date,
                p.close_price AS price,
                ROUND(
                    AVG(p.close_price) OVER (
                        ORDER BY p.date
                        ROWS BETWEEN :window_back PRECEDING AND CURRENT ROW
                    ), 2
                ) AS moving_avg,
                ROUND(
                    p.close_price - LAG(p.close_price) OVER (ORDER BY p.date),
                    2
                ) AS daily_change,
                ROUND(
                    (p.close_price - LAG(p.close_price) OVER (ORDER BY p.date))
                    / NULLIF(LAG(p.close_price) OVER (ORDER BY p.date), 0) * 100,
                    2
                ) AS daily_return_pct
            FROM prices p
            INNER JOIN constituents c ON c.id = p.constituent_id
            WHERE c.ticker = :ticker
            ORDER BY p.date
        """)

        rows = self.session.execute(
            sql, {"ticker": ticker, "window_back": window - 1}
        ).fetchall()
        return [
            {
                "date": str(row.date),
                "price": row.price,
                "moving_avg": row.moving_avg,
                "daily_change": row.daily_change,
                "daily_return_pct": row.daily_return_pct,
            }
            for row in rows
        ]

    def get_best_worst_days_sql(self, etf_id: int, n: int = 5) -> dict:
        """
        Find best and worst performing days for an ETF.

        SQL: Two CTEs chained, LAG for daily returns, ordering tricks
        """
        sql = text("""
            WITH daily_prices AS (
                SELECT
                    p.date,
                    ROUND(SUM(h.weight * p.close_price), 2) AS etf_price
                FROM etf_holdings h
                INNER JOIN prices p ON p.constituent_id = h.constituent_id
                WHERE h.etf_upload_id = :etf_id
                GROUP BY p.date
            ),
            daily_returns AS (
                SELECT
                    date,
                    etf_price,
                    LAG(etf_price) OVER (ORDER BY date) AS prev_price,
                    ROUND(
                        (etf_price - LAG(etf_price) OVER (ORDER BY date))
                        / NULLIF(LAG(etf_price) OVER (ORDER BY date), 0) * 100,
                        2
                    ) AS return_pct
                FROM daily_prices
            )
            SELECT date, etf_price, prev_price, return_pct
            FROM daily_returns
            WHERE return_pct IS NOT NULL
            ORDER BY return_pct DESC
        """)

        rows = self.session.execute(sql, {"etf_id": etf_id}).fetchall()
        all_returns = [
            {
                "date": str(row.date),
                "etf_price": row.etf_price,
                "prev_price": row.prev_price,
                "return_pct": row.return_pct,
            }
            for row in rows
        ]

        return {
            "best_days": all_returns[:n],
            "worst_days": list(reversed(all_returns[-n:])) if len(all_returns) >= n else list(reversed(all_returns)),
        }

    def get_constituent_correlation_sql(self, ticker_a: str, ticker_b: str) -> list[dict]:
        """
        Get paired daily prices for two constituents (for correlation analysis).

        SQL: Self-JOIN on the prices table with multiple join conditions
        """
        sql = text("""
            SELECT
                pa.date,
                pa.close_price AS price_a,
                pb.close_price AS price_b
            FROM prices pa
            INNER JOIN constituents ca ON ca.id = pa.constituent_id
            INNER JOIN prices pb       ON pb.date = pa.date
            INNER JOIN constituents cb ON cb.id = pb.constituent_id
            WHERE ca.ticker = :ticker_a
              AND cb.ticker = :ticker_b
            ORDER BY pa.date
        """)

        rows = self.session.execute(
            sql, {"ticker_a": ticker_a, "ticker_b": ticker_b}
        ).fetchall()
        return [
            {
                "date": str(row.date),
                "price_a": row.price_a,
                "price_b": row.price_b,
            }
            for row in rows
        ]

    def get_etf_performance_comparison_sql(self) -> list[dict]:
        """
        Compare performance across all uploaded ETFs.

        SQL: Multi-table JOIN, GROUP BY with aggregate, CASE expression,
             subqueries for first/last date
        """
        sql = text("""
            WITH etf_first_last AS (
                SELECT
                    h.etf_upload_id,
                    u.filename,
                    MIN(p.date) AS first_date,
                    MAX(p.date) AS last_date
                FROM etf_holdings h
                INNER JOIN prices p      ON p.constituent_id = h.constituent_id
                INNER JOIN etf_uploads u ON u.id = h.etf_upload_id
                GROUP BY h.etf_upload_id, u.filename
            ),
            etf_start AS (
                SELECT
                    fl.etf_upload_id,
                    fl.filename,
                    ROUND(SUM(h.weight * p.close_price), 2) AS start_price
                FROM etf_first_last fl
                INNER JOIN etf_holdings h ON h.etf_upload_id = fl.etf_upload_id
                INNER JOIN prices p       ON p.constituent_id = h.constituent_id
                                          AND p.date = fl.first_date
                GROUP BY fl.etf_upload_id, fl.filename
            ),
            etf_end AS (
                SELECT
                    fl.etf_upload_id,
                    ROUND(SUM(h.weight * p.close_price), 2) AS end_price
                FROM etf_first_last fl
                INNER JOIN etf_holdings h ON h.etf_upload_id = fl.etf_upload_id
                INNER JOIN prices p       ON p.constituent_id = h.constituent_id
                                          AND p.date = fl.last_date
                GROUP BY fl.etf_upload_id
            )
            SELECT
                s.etf_upload_id AS id,
                s.filename,
                s.start_price,
                e.end_price,
                ROUND(e.end_price - s.start_price, 2) AS absolute_return,
                ROUND(
                    (e.end_price - s.start_price) / NULLIF(s.start_price, 0) * 100,
                    2
                ) AS return_pct,
                CASE
                    WHEN e.end_price > s.start_price THEN 'POSITIVE'
                    WHEN e.end_price < s.start_price THEN 'NEGATIVE'
                    ELSE 'FLAT'
                END AS performance
            FROM etf_start s
            INNER JOIN etf_end e ON e.etf_upload_id = s.etf_upload_id
            ORDER BY return_pct DESC
        """)

        rows = self.session.execute(sql).fetchall()
        return [
            {
                "id": row.id,
                "filename": row.filename,
                "start_price": row.start_price,
                "end_price": row.end_price,
                "absolute_return": row.absolute_return,
                "return_pct": row.return_pct,
                "performance": row.performance,
            }
            for row in rows
        ]

    def get_sector_breakdown_sql(self, etf_id: int) -> list[dict]:
        """
        Aggregate ETF exposure by sector.

        SQL: GROUP BY with SUM, COUNT, ROUND, INNER JOIN across 3 tables
        """
        sql = text("""
            SELECT
                c.sector,
                COUNT(*)                       AS num_holdings,
                ROUND(SUM(h.weight), 4)        AS total_weight,
                ROUND(SUM(h.weight) * 100, 2)  AS weight_pct
            FROM etf_holdings h
            INNER JOIN constituents c ON c.id = h.constituent_id
            WHERE h.etf_upload_id = :etf_id
            GROUP BY c.sector
            ORDER BY total_weight DESC
        """)

        rows = self.session.execute(sql, {"etf_id": etf_id}).fetchall()
        return [
            {
                "sector": row.sector,
                "num_holdings": row.num_holdings,
                "total_weight": row.total_weight,
                "weight_pct": row.weight_pct,
            }
            for row in rows
        ]

    def get_volume_leaders_sql(self, n: int = 10) -> list[dict]:
        """
        Top N constituents by average daily trading volume.

        SQL: GROUP BY, AVG aggregate, ORDER BY with LIMIT, number formatting
        """
        sql = text("""
            SELECT
                c.ticker,
                c.name AS company_name,
                c.sector,
                CAST(AVG(p.volume) AS INTEGER)  AS avg_volume,
                CAST(MAX(p.volume) AS INTEGER)  AS max_volume,
                CAST(MIN(p.volume) AS INTEGER)  AS min_volume
            FROM constituents c
            INNER JOIN prices p ON p.constituent_id = c.id
            WHERE p.volume IS NOT NULL
            GROUP BY c.ticker, c.name, c.sector
            ORDER BY avg_volume DESC
            LIMIT :n
        """)

        rows = self.session.execute(sql, {"n": n}).fetchall()
        return [
            {
                "ticker": row.ticker,
                "company_name": row.company_name,
                "sector": row.sector,
                "avg_volume": row.avg_volume,
                "max_volume": row.max_volume,
                "min_volume": row.min_volume,
            }
            for row in rows
        ]

    def get_ohlcv_data_sql(self, ticker: str, limit: int = 60) -> list[dict]:
        """
        Get full OHLCV candle data for a constituent (most recent N days).

        SQL: INNER JOIN, ORDER BY DESC with LIMIT, subquery reversal
        """
        sql = text("""
            SELECT * FROM (
                SELECT
                    p.date,
                    c.ticker,
                    p.open_price,
                    p.high_price,
                    p.low_price,
                    p.close_price,
                    p.volume
                FROM prices p
                INNER JOIN constituents c ON c.id = p.constituent_id
                WHERE c.ticker = :ticker
                ORDER BY p.date DESC
                LIMIT :limit
            ) sub
            ORDER BY date ASC
        """)

        rows = self.session.execute(sql, {"ticker": ticker, "limit": limit}).fetchall()
        return [
            {
                "date": str(row.date),
                "ticker": row.ticker,
                "open": row.open_price,
                "high": row.high_price,
                "low": row.low_price,
                "close": row.close_price,
                "volume": row.volume,
            }
            for row in rows
        ]

    def get_database_stats(self) -> dict:
        """
        Overview statistics about what's in the database.

        SQL: Multiple scalar subqueries, COUNT DISTINCT
        """
        sql = text("""
            SELECT
                (SELECT COUNT(*)                       FROM constituents)  AS total_constituents,
                (SELECT COUNT(DISTINCT sector)         FROM constituents)  AS total_sectors,
                (SELECT COUNT(*)                       FROM prices)        AS total_price_records,
                (SELECT COUNT(*)                       FROM etf_uploads)   AS total_etf_uploads,
                (SELECT COUNT(*)                       FROM etf_holdings)  AS total_holdings,
                (SELECT MIN(date)                      FROM prices)        AS earliest_date,
                (SELECT MAX(date)                      FROM prices)        AS latest_date,
                (SELECT COUNT(DISTINCT date)           FROM prices)        AS trading_days
        """)

        row = self.session.execute(sql).fetchone()
        return {
            "total_constituents": row.total_constituents,
            "total_sectors": row.total_sectors,
            "total_price_records": row.total_price_records,
            "total_etf_uploads": row.total_etf_uploads,
            "total_holdings": row.total_holdings,
            "earliest_date": str(row.earliest_date) if row.earliest_date else None,
            "latest_date": str(row.latest_date) if row.latest_date else None,
            "trading_days": row.trading_days,
        }
