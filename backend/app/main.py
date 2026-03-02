"""
Spectra — ETF Analytics Engine

FastAPI application providing REST endpoints for the Spectra platform.

Endpoints:
- POST /api/upload - Upload an ETF weights CSV file
- GET /api/holdings - Get current holdings with latest prices
- GET /api/etf-prices - Get reconstructed ETF price time series
- GET /api/top-holdings - Get top 5 holdings by value

Design Decisions:
- CORS enabled for local development (React on different port)
- Price data loaded once at startup for efficiency
- ETF weights stored in memory per session (stateless alternative would use request-scoped storage)
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from pathlib import Path

from app.services.etf_service import ETFService
from app.services.db_service import DBService
from app.database import init_db, get_db, SessionLocal, Price, Constituent
from sqlalchemy import func, text

# Initialize FastAPI app
app = FastAPI(
    title="Spectra API",
    description="ETF analytics engine — prices, holdings, pipelines & insights",
    version="1.0.0"
)

# Enable CORS for frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],  # Vite default ports
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize ETF service with price data
# Path is relative to where the server is started (project root)
PRICES_CSV_PATH = Path(__file__).parent.parent.parent / "data" / "prices.csv"
etf_service = ETFService(str(PRICES_CSV_PATH))

# Initialize database on startup (creates tables if they don't exist)
init_db()


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "message": "Spectra API is running",
        "etf_loaded": etf_service.has_etf_loaded()
    }


# Maximum file size: 1MB
MAX_FILE_SIZE = 1 * 1024 * 1024


@app.post("/api/upload")
async def upload_etf(file: UploadFile = File(...)):
    """
    Upload an ETF weights CSV file.
    
    Expected CSV format:
    - Column 'name': Constituent identifier (A, B, C, etc.)
    - Column 'weight': Decimal weight (e.g., 0.15 for 15%)
    
    Returns summary of uploaded ETF and all dashboard data.
    """
    # Validate file extension
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV file")
    
    # Validate content type (if provided)
    if file.content_type and file.content_type not in ['text/csv', 'application/csv', 'application/vnd.ms-excel', 'text/plain']:
        raise HTTPException(status_code=400, detail=f"Invalid content type: {file.content_type}. Expected CSV.")
    
    try:
        content = await file.read()
        
        # Validate file size
        if len(content) == 0:
            raise HTTPException(status_code=400, detail="File is empty")
        
        if len(content) > MAX_FILE_SIZE:
            raise HTTPException(status_code=400, detail=f"File too large. Maximum size is {MAX_FILE_SIZE // 1024}KB")
        
        # Validate file is valid UTF-8
        try:
            content.decode('utf-8')
        except UnicodeDecodeError:
            raise HTTPException(status_code=400, detail="File must be valid UTF-8 encoded text")
        
        summary = etf_service.load_etf_weights(content)
        
        # Return all data needed for the dashboard in one response
        # This minimizes round trips after upload
        return {
            "success": True,
            "filename": file.filename,
            "summary": summary,
            "holdings": etf_service.get_holdings_table(),
            "etf_prices": etf_service.get_etf_time_series(),
            "top_holdings": etf_service.get_top_holdings(5),
            "latest_date": etf_service.get_latest_date()
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.get("/api/holdings")
async def get_holdings():
    """
    Get current ETF holdings with latest prices.
    
    Returns list of holdings with: name, weight, latest_price
    """
    if not etf_service.has_etf_loaded():
        raise HTTPException(status_code=400, detail="No ETF loaded. Please upload an ETF CSV first.")
    
    return {
        "holdings": etf_service.get_holdings_table(),
        "latest_date": etf_service.get_latest_date()
    }


@app.get("/api/etf-prices")
async def get_etf_prices():
    """
    Get reconstructed ETF price time series.
    
    The ETF price is calculated as the weighted sum of constituent prices.
    """
    if not etf_service.has_etf_loaded():
        raise HTTPException(status_code=400, detail="No ETF loaded. Please upload an ETF CSV first.")
    
    return {
        "prices": etf_service.get_etf_time_series()
    }


@app.get("/api/top-holdings")
async def get_top_holdings():
    """
    Get top 5 holdings by value (weight × latest price).
    
    Returns holdings sorted by contribution to ETF value.
    """
    if not etf_service.has_etf_loaded():
        raise HTTPException(status_code=400, detail="No ETF loaded. Please upload an ETF CSV first.")
    
    return {
        "top_holdings": etf_service.get_top_holdings(5),
        "latest_date": etf_service.get_latest_date()
    }


# ═══════════════════════════════════════════════════════════════
#  v2 API — SQL-Powered Endpoints
#
#  These endpoints use the database (populated by the ETL pipeline)
#  and demonstrate SQL proficiency: JOINs, window functions, CTEs,
#  aggregations, subqueries, and more.
# ═══════════════════════════════════════════════════════════════


@app.get("/api/v2/db-stats")
def get_db_stats(db: Session = Depends(get_db)):
    """Database overview statistics."""
    svc = DBService(db)
    return svc.get_database_stats()


@app.get("/api/v2/etfs")
def list_etfs(db: Session = Depends(get_db)):
    """List all uploaded ETFs with metadata."""
    svc = DBService(db)
    return svc.get_all_etfs()


@app.get("/api/v2/etfs/{etf_id}/holdings")
def get_etf_holdings_sql(etf_id: int, db: Session = Depends(get_db)):
    """Get holdings for an ETF using SQL JOINs."""
    svc = DBService(db)
    holdings = svc.get_holdings_sql(etf_id)
    if not holdings:
        raise HTTPException(status_code=404, detail=f"ETF {etf_id} not found or has no holdings")
    return {"etf_id": etf_id, "holdings": holdings}


@app.get("/api/v2/etfs/{etf_id}/prices")
def get_etf_prices_sql(etf_id: int, db: Session = Depends(get_db)):
    """Get ETF time series computed via SQL aggregation."""
    svc = DBService(db)
    prices = svc.get_etf_time_series_sql(etf_id)
    if not prices:
        raise HTTPException(status_code=404, detail=f"ETF {etf_id} not found")
    return {"etf_id": etf_id, "prices": prices}


@app.get("/api/v2/etfs/{etf_id}/top-holdings")
def get_etf_top_holdings_sql(
    etf_id: int,
    n: int = Query(default=5, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Top N holdings ranked by value using SQL window functions (ROW_NUMBER)."""
    svc = DBService(db)
    return {"etf_id": etf_id, "top_holdings": svc.get_top_holdings_sql(etf_id, n)}


@app.get("/api/v2/etfs/{etf_id}/best-worst-days")
def get_best_worst_days(
    etf_id: int,
    n: int = Query(default=5, ge=1, le=20),
    db: Session = Depends(get_db),
):
    """Best and worst performing days using CTEs + LAG window function."""
    svc = DBService(db)
    return svc.get_best_worst_days_sql(etf_id, n)


@app.get("/api/v2/etfs/compare")
def compare_etfs(db: Session = Depends(get_db)):
    """Compare performance across all ETFs using multi-CTE SQL with CASE."""
    svc = DBService(db)
    return svc.get_etf_performance_comparison_sql()


@app.get("/api/v2/analytics/price-summary")
def get_price_summary(db: Session = Depends(get_db)):
    """Aggregate price stats per constituent (AVG, MIN, MAX, COUNT)."""
    svc = DBService(db)
    return svc.get_price_summary_sql()


@app.get("/api/v2/analytics/moving-average")
def get_moving_average(
    ticker: str = Query(..., min_length=1, max_length=10),
    window: int = Query(default=5, ge=2, le=50),
    db: Session = Depends(get_db),
):
    """Moving averages + daily returns using AVG OVER and LAG window functions."""
    svc = DBService(db)
    return svc.get_moving_averages_sql(ticker.upper(), window)


@app.get("/api/v2/analytics/correlation")
def get_correlation(
    ticker_a: str = Query(..., min_length=1, max_length=10),
    ticker_b: str = Query(..., min_length=1, max_length=10),
    db: Session = Depends(get_db),
):
    """Paired daily prices for two constituents using a self-JOIN."""
    svc = DBService(db)
    return svc.get_constituent_correlation_sql(ticker_a.upper(), ticker_b.upper())


@app.get("/api/v2/analytics/sector-breakdown")
def get_sector_breakdown(db: Session = Depends(get_db)):
    """Global sector breakdown of all constituents using GROUP BY."""
    sql = text("""
        SELECT
            c.sector,
            COUNT(*)                       AS ticker_count,
            ROUND(AVG(p.close_price), 2)   AS avg_price
        FROM constituents c
        INNER JOIN prices p ON p.constituent_id = c.id
        WHERE p.date = (SELECT MAX(date) FROM prices)
        GROUP BY c.sector
        ORDER BY ticker_count DESC
    """)
    rows = db.execute(sql).fetchall()
    return [
        {"sector": r.sector, "ticker_count": r.ticker_count, "avg_price": r.avg_price}
        for r in rows
    ]


@app.get("/api/v2/analytics/volume-leaders")
def get_volume_leaders(
    n: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Top N tickers by average trading volume with sector info."""
    svc = DBService(db)
    return svc.get_volume_leaders_sql(n)


@app.get("/api/v2/analytics/ohlcv")
def get_ohlcv(
    ticker: str = Query(..., min_length=1, max_length=10),
    limit: int = Query(default=60, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Full OHLCV candlestick data for a single ticker."""
    svc = DBService(db)
    return svc.get_ohlcv_data_sql(ticker.upper(), limit)


# ═══════════════════════════════════════════════════════════════
#  Ingestion / Pipeline Status Endpoints
#
#  Monitor the health of the data pipeline: watermarks,
#  freshness, landing zone status, and record counts.
# ═══════════════════════════════════════════════════════════════


@app.get("/api/v2/ingestion/status")
def get_ingestion_status(db: Session = Depends(get_db)):
    """
    Pipeline health check — shows data freshness and watermarks.

    This is what an on-call engineer would check to see if
    the daily data feed is running and up to date.
    """
    from pathlib import Path
    from datetime import date

    incoming_dir = Path(__file__).parent.parent.parent / "data" / "incoming"
    processed_dir = Path(__file__).parent.parent.parent / "data" / "processed"

    # High watermark: latest date in the prices table
    watermark = db.query(func.max(Price.date)).scalar()

    # Per-ticker watermarks (detect lagging tickers)
    ticker_watermarks = (
        db.query(
            Constituent.ticker,
            func.max(Price.date).label("latest_date"),
        )
        .join(Price, Price.constituent_id == Constituent.id)
        .group_by(Constituent.ticker)
        .all()
    )
    lagging = [
        {"ticker": t, "latest_date": str(d)}
        for t, d in ticker_watermarks
        if d and watermark and d < watermark
    ]

    # Landing zone status
    pending_files = list(incoming_dir.glob("*.csv")) if incoming_dir.exists() else []
    processed_files = list(processed_dir.glob("*.csv")) if processed_dir.exists() else []

    total_records = db.query(func.count(Price.id)).scalar()
    n_tickers = db.query(func.count(func.distinct(Price.constituent_id))).scalar()
    n_days = db.query(func.count(func.distinct(Price.date))).scalar()
    min_date = db.query(func.min(Price.date)).scalar()

    return {
        "pipeline_healthy": len(pending_files) == 0,
        "high_watermark": str(watermark) if watermark else None,
        "date_range": {
            "min": str(min_date) if min_date else None,
            "max": str(watermark) if watermark else None,
        },
        "total_records": total_records,
        "total_tickers": n_tickers,
        "total_trading_days": n_days,
        "landing_zone": {
            "pending_files": len(pending_files),
            "pending_filenames": [f.name for f in sorted(pending_files)],
        },
        "processed_files_count": len(processed_files),
        "lagging_tickers": lagging,
        "freshness_note": (
            f"Data is current through {watermark}. "
            f"Run 'python -m pipeline.scheduler' to generate and ingest new data."
            if watermark else "No data loaded yet."
        ),
    }


@app.get("/api/v2/ingestion/watermarks")
def get_watermarks(db: Session = Depends(get_db)):
    """
    Per-ticker high watermarks — the latest date loaded for each ticker.

    Useful for diagnosing partial loads or ticker-level data gaps.

    SQL:
        SELECT c.ticker, c.sector, MAX(p.date), COUNT(p.id)
        FROM prices p JOIN constituents c ...
        GROUP BY c.ticker
    """
    rows = (
        db.query(
            Constituent.ticker,
            Constituent.sector,
            func.max(Price.date).label("latest_date"),
            func.min(Price.date).label("earliest_date"),
            func.count(Price.id).label("record_count"),
        )
        .join(Price, Price.constituent_id == Constituent.id)
        .group_by(Constituent.ticker, Constituent.sector)
        .order_by(Constituent.ticker)
        .all()
    )
    return [
        {
            "ticker": r.ticker,
            "sector": r.sector,
            "earliest_date": str(r.earliest_date),
            "latest_date": str(r.latest_date),
            "record_count": r.record_count,
        }
        for r in rows
    ]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
