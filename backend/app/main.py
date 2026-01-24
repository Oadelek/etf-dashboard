"""
ETF Dashboard API

FastAPI application providing REST endpoints for the ETF Dashboard.

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

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path

from app.services.etf_service import ETFService

# Initialize FastAPI app
app = FastAPI(
    title="ETF Dashboard API",
    description="API for viewing ETF historical prices and holdings",
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
PRICES_CSV_PATH = Path(__file__).parent.parent.parent / "data" / "bankofmontreal-e134q-1arsjzss-prices.csv"
etf_service = ETFService(str(PRICES_CSV_PATH))


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "message": "ETF Dashboard API is running",
        "etf_loaded": etf_service.has_etf_loaded()
    }


@app.post("/api/upload")
async def upload_etf(file: UploadFile = File(...)):
    """
    Upload an ETF weights CSV file.
    
    Expected CSV format:
    - Column 'name': Constituent identifier (A, B, C, etc.)
    - Column 'weight': Decimal weight (e.g., 0.15 for 15%)
    
    Returns summary of uploaded ETF and all dashboard data.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    try:
        content = await file.read()
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
