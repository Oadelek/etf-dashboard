# Spectra — ETF Analytics Engine

A full-stack data engineering platform for ingesting, transforming, and analyzing Exchange-Traded Fund data. Demonstrates the complete data lifecycle: **ingestion → ETL → storage → SQL analytics → visualization**, with both batch and incremental pipelines, pluggable data sources, and a multi-page React dashboard.

**Tech stack:** Python · FastAPI · Pandas · PySpark · SQLAlchemy · SQLite · React 18 · Vite · Recharts · Tailwind CSS · Docker

---

## Quick Start

**Prerequisites:** Python 3.11+, Node.js 18+

```bash
# Clone and enter the repo
git clone https://github.com/Oadelek/etf-dashboard.git
cd etf-dashboard

# Windows:
setup.bat

# Mac/Linux:
chmod +x setup.sh && ./setup.sh
```

Start both servers:

```bash
# Terminal 1 — Backend API
cd backend
venv\Scripts\activate      # Windows (or source venv/bin/activate on Mac/Linux)
uvicorn app.main:app --reload --port 8000

# Terminal 2 — Frontend
cd frontend
npm run dev
```

Open **http://localhost:5173** — you'll see the Spectra dashboard with live data.

### Load the Database (first time)

```bash
cd backend && venv\Scripts\activate
python -m pipeline.etl --fresh
```

This runs the batch ETL pipeline: generates 50 S&P 500 tickers × 5 years of OHLCV data → validates → loads into SQLite. Takes ~30 seconds, produces **90,000+ price records**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     FRONTEND  (React 18)                        │
│  Vite · Tailwind · Recharts · React Router 6                   │
│  Pages: Overview │ ETF Explorer │ Analytics │ Pipeline │ Upload │
└─────────────────────┬───────────────────────────────────────────┘
                      │ HTTP (Axios)
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                     BACKEND  (FastAPI)                           │
│  V1 API (in-memory Pandas)  ·  V2 API (SQL-powered)            │
│  Ingestion API (pipeline monitoring)                            │
│  Services: etf_service.py (Pandas) · db_service.py (raw SQL)   │
└─────────────────────┬───────────────────────────────────────────┘
                      │ SQLAlchemy
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATABASE  (SQLite)                           │
│  etf_uploads · constituents · prices  (3NF normalized)          │
│  50 tickers · 90,000+ OHLCV records · 5 ETFs · 92 holdings     │
└─────────────────────▲───────────────────────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────────────────────┐
│                     DATA PIPELINE                                │
│  Batch ETL (etl.py) · Incremental ETL (daily_feed + incr_etl)  │
│  PySpark Analytics (spark_analysis.py)                          │
│  Pluggable sources: Simulator · YFinance · CSV                  │
└─────────────────────────────────────────────────────────────────┘
```

### Database Schema (Star-Schema-Lite)

| Table | Role | Key Columns |
|-------|------|-------------|
| `etf_uploads` | Source tracking dim | id, filename, uploaded_at, is_active |
| `constituents` | Dimension table | id, etf_id (FK), ticker, company_name, sector, industry, weight |
| `prices` | Fact table (90K+ rows) | id, constituent_id (FK), date, open/high/low/close_price, volume |

---

## Data Pipeline

### Batch ETL (`pipeline/etl.py`)

Full initial load — generates realistic financial data and bootstraps the database:

```
generate_data.py → 50 S&P 500 tickers, 5 ETF portfolios, 5 years of OHLCV
        │
        ▼
   etl.py → Validate schema → Map tickers to sectors → Dedup
        │
        ▼
   SQLite DB → etf_uploads + constituents + prices tables
```

```bash
python -m pipeline.etl              # Normal run
python -m pipeline.etl --fresh      # Drop & recreate all tables
python -m pipeline.etl --etf tech   # Load specific ETF only
```

### Incremental Pipeline (`pipeline/daily_feed.py` + `pipeline/incremental_etl.py`)

Production-style daily ingestion with watermark tracking:

```
DataProvider (pluggable) → Landing Zone (CSV files)
                                    │
                              incremental_etl.py
                                    │
                    Validate → Watermark check → Dedup → Load → Archive
```

**Pluggable Data Source Pattern (Strategy Pattern):**

```python
class DataProvider(ABC):
    @abstractmethod
    def fetch(self, tickers, start_date, end_date) -> pd.DataFrame

class SimulatorProvider(DataProvider)   # Synthetic OHLCV for testing
class YFinanceProvider(DataProvider)    # Real market data via yfinance
class CSVProvider(DataProvider)         # Read from CSV files
```

```bash
python -m pipeline.daily_feed --provider simulator  # Generate synthetic data
python -m pipeline.incremental_etl                   # Load from landing zone
python -m pipeline.scheduler                         # Run both in sequence
```

### PySpark Analytics (`pipeline/spark_analysis.py`)

Distributed analytics on the price dataset:

- Moving averages (5-day, 20-day window functions)
- Daily returns & volatility ranking
- Performance ranking (total return via ROW_NUMBER)
- Volume spike detection (>2× 20-day average)
- Sector-level aggregation (JOIN + GROUP BY)
- Price correlation matrix (pivot + pairwise stat.corr)

```bash
python -m pipeline.spark_analysis
```

---

## API Reference

### V1 — In-Memory (Pandas)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check |
| `/api/upload` | POST | Upload ETF CSV → instant Pandas analysis |
| `/api/holdings` | GET | Holdings with latest prices |
| `/api/etf-prices` | GET | Reconstructed ETF price series |
| `/api/top-holdings` | GET | Top N holdings by value |

### V2 — SQL-Powered (Database)

| Endpoint | Method | SQL Concepts |
|----------|--------|--------------|
| `/api/v2/db-stats` | GET | COUNT, aggregate functions |
| `/api/v2/etfs` | GET | JOIN, subquery |
| `/api/v2/etfs/{id}/holdings` | GET | JOIN + correlated subquery (latest prices) |
| `/api/v2/etfs/{id}/prices` | GET | CTE + window function (weighted sum) |
| `/api/v2/etfs/{id}/top-holdings` | GET | ORDER BY + LIMIT |
| `/api/v2/etfs/{id}/best-worst-days` | GET | LAG() window function |
| `/api/v2/analytics/moving-averages` | GET | AVG() OVER (ROWS BETWEEN) |
| `/api/v2/analytics/ohlcv-data` | GET | Multi-table JOIN |
| `/api/v2/analytics/correlation` | GET | Self-JOIN on date |
| `/api/v2/analytics/sector-breakdown` | GET | GROUP BY + aggregate |
| `/api/v2/analytics/volume-leaders` | GET | GROUP BY + multiple aggs |
| `/api/v2/analytics/price-summary` | GET | Full statistics per ticker |

### Ingestion Monitoring

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v2/ingestion/status` | GET | Pipeline health, watermarks, lag detection |
| `/api/v2/ingestion/run` | POST | Trigger incremental pipeline |

---

## Frontend — 5 Dashboard Pages

| Page | Route | What It Shows |
|------|-------|---------------|
| **Overview** | `/` | KPI cards, sector pie chart, volume leaders, data freshness |
| **ETF Explorer** | `/etfs` | ETF selector, price time series, holdings table, best/worst days |
| **Analytics** | `/analytics` | Moving averages, OHLCV candles, correlation analysis, price summary |
| **Pipeline** | `/pipeline` | Health status, ingestion stats, architecture diagram, watermark table |
| **Upload** | `/upload` | V1 upload flow — drop a CSV, get instant analysis |

Sidebar navigation with dark mode toggle and collapsible layout.

---

## Testing

43 automated tests covering API endpoints, services, and data validation:

```bash
cd backend
venv\Scripts\activate
python -m pytest tests/ -v
```

---

## Docker

```bash
docker-compose up --build
```

Runs three containers:
- **backend** — Python 3.11 + FastAPI + uvicorn
- **frontend** — Node 18 + Vite build → served via nginx
- **nginx** — Reverse proxy (`/api` → backend, `/` → frontend)

---

## Project Structure

```
spectra/
├── backend/
│   ├── app/
│   │   ├── main.py                 # FastAPI routes (V1 + V2 + Ingestion)
│   │   ├── database.py             # SQLAlchemy models & schema
│   │   └── services/
│   │       ├── etf_service.py      # Pandas-based analytics (V1)
│   │       └── db_service.py       # SQL-based analytics (V2) — 15+ queries
│   ├── tests/                      # 43 pytest tests
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── App.jsx                 # Router + layout
│   │   ├── components/
│   │   │   ├── Sidebar.jsx         # Navigation + dark mode
│   │   │   ├── StatsCard.jsx       # Reusable KPI card
│   │   │   ├── FileUpload.jsx      # Drag-drop CSV upload
│   │   │   ├── HoldingsTable.jsx   # Sortable table
│   │   │   ├── PriceChart.jsx      # Time series with zoom
│   │   │   └── TopHoldingsChart.jsx
│   │   └── pages/
│   │       ├── OverviewPage.jsx    # Dashboard overview
│   │       ├── ETFExplorerPage.jsx # ETF deep-dive
│   │       ├── AnalyticsPage.jsx   # SQL analytics explorer
│   │       ├── PipelinePage.jsx    # Pipeline monitoring
│   │       └── UploadPage.jsx      # V1 upload flow
│   └── package.json
├── pipeline/
│   ├── etl.py                      # Batch ETL pipeline
│   ├── daily_feed.py               # Pluggable data providers
│   ├── incremental_etl.py          # Watermark-based incremental load
│   ├── scheduler.py                # Pipeline orchestrator
│   ├── generate_data.py            # Realistic S&P 500 data generator
│   └── spark_analysis.py           # PySpark analytics (7 analyses)
├── data/                           # CSVs + SQLite database
├── docker-compose.yml
├── setup.sh / setup.bat
└── README.md
```

---

## Key SQL Patterns Demonstrated

| Pattern | Where Used | Example |
|---------|-----------|---------|
| **Window Functions** | Moving averages, best/worst days | `AVG(price) OVER (ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)` |
| **LAG()** | Daily returns | `LAG(price) OVER (ORDER BY date)` |
| **ROW_NUMBER()** | Latest price per ticker | `ROW_NUMBER() OVER (PARTITION BY constituent_id ORDER BY date DESC)` |
| **CTEs** | ETF price series | `WITH daily_prices AS (...) SELECT ...` |
| **Self-JOIN** | Correlation analysis | `prices p1 JOIN prices p2 ON p1.date = p2.date` |
| **Subqueries** | Holdings with latest price | `WHERE date = (SELECT MAX(date) ...)` |
| **GROUP BY + HAVING** | Sector breakdown | `GROUP BY sector HAVING COUNT(*) > 1` |

---

## What I'd Add Next

- [ ] WebSocket for real-time pipeline status updates
- [ ] PostgreSQL support (swap SQLAlchemy connection string)
- [ ] Airflow DAG definitions for production scheduling
- [ ] dbt models for transformation layer
- [ ] Grafana dashboard for pipeline metrics
- [ ] CI/CD pipeline with GitHub Actions
