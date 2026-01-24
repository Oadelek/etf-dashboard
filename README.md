# ETF Dashboard

A web app for viewing historical prices of uploaded ETFs. Upload a CSV with stock weights, and it shows you a price chart, holdings table, and top holdings breakdown.

---

## Quick Setup

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

Then start both servers (in separate terminals):

```bash
# Terminal 1 - Backend
cd backend
venv\Scripts\activate      # Windows
# source venv/bin/activate  # Mac/Linux
uvicorn app.main:app --reload --port 8000

# Terminal 2 - Frontend
cd frontend
npm run dev
```

Open http://localhost:5173 and upload one of the sample ETF files from `/data`.

---

## How It Works

### The Data

There are two types of CSV files:

1. **Price data** (`prices.csv`) - Historical daily prices for stocks A through Z. This is loaded once when the backend starts. Think of it as "all the stocks that exist in this universe."

2. **ETF weights** (what you upload) - Defines which stocks are in your ETF and how much of each:
   ```csv
   name,weight
   M,0.30
   S,0.25
   W,0.25
   G,0.20
   ```
   This ETF holds 4 stocks with weights that sum to 1.0 (100%).

### The Calculation

ETF price on any given day = weighted sum of constituent prices

```
ETF_price = (0.30 × M_price) + (0.25 × S_price) + (0.25 × W_price) + (0.20 × G_price)
```

This is done for every day in the price history, giving us a time series to chart.

---

## Why I Picked These Technologies

### Frontend: React + Vite + Recharts + Tailwind

**React** - React has better options for charting libraries and the component model clicked for this dashboard layout.

**Vite** - Way faster than Create React App. Hot reload is basically instant which matters when tweaking chart styling.

**Recharts** - Built specifically for React. Has zoom/brush components out of the box. Tried D3 first but Recharts was simpler for standard charts.

**Tailwind** - I wasn't sure at first ("utility classes look ugly") but it's actually great for prototyping. No context-switching to CSS files.

### Backend: Python + FastAPI + Pandas

**Python + Pandas** - The weighted price calculation is literally one line in pandas. Doing matrix math in JavaScript would be painful and slow.

**FastAPI** - Modern Python web framework. Gives you automatic API docs at `/docs`, good type hints, and async support. Flask would work too but FastAPI feels cleaner.

### The Alternative I Considered

Could have done everything client-side with PapaParse (CSV parsing) and math in JS. But:
- File uploads are easier to validate server-side
- Pandas is much faster for the calculations
- Wanted to show full-stack ability

---

## Assumptions I Made

1. **The price data is static** - It's loaded once at startup. No live feeds or WebSocket updates.

2. **ETF weights should sum to ~1.0** - The app warns if they don't, but still works. Real ETFs might have small rounding differences.

3. **All tickers in the ETF must exist in the price data** - If you upload an ETF with ticker "ZZZ" that doesn't exist, it'll reject it with a helpful error.

4. **No authentication** - This is a demo. In production you'd add auth, especially for file uploads.

5. **In-memory state** - The uploaded ETF is stored in server memory. Restarting the backend clears it. A real app would use a database or session storage.

6. **All dates are trading days** - No logic to skip weekends/holidays. The sample data only has weekdays anyway.

---

## Project Structure

```
etf-dashboard/
├── backend/
│   ├── app/
│   │   ├── main.py              # API routes
│   │   └── services/
│   │       └── etf_service.py   # All the business logic
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── FileUpload.jsx      # Drag-drop upload
│   │   │   ├── HoldingsTable.jsx   # Sortable table
│   │   │   ├── PriceChart.jsx      # Time series with zoom
│   │   │   └── TopHoldingsChart.jsx
│   │   └── App.jsx
│   └── package.json
├── data/                        # Sample CSVs to test with
├── setup.sh                     # One-command setup (Mac/Linux)
├── setup.bat                    # One-command setup (Windows)
└── README.md
```

---

## API Endpoints

| Endpoint | Method | What it does |
|----------|--------|--------------|
| `/` | GET | Health check |
| `/api/upload` | POST | Upload ETF CSV → returns all dashboard data |
| `/api/holdings` | GET | Current holdings with latest prices |
| `/api/etf-prices` | GET | Time series for the chart |
| `/api/top-holdings` | GET | Top 5 by value |

The `/api/upload` endpoint returns everything at once so the frontend only needs one API call after upload.

---

## Validation

The backend validates uploads pretty thoroughly:

- File must be CSV, under 1MB, valid UTF-8
- Must have `name` and `weight` columns (case-insensitive)
- No duplicate tickers
- Weights must be numeric, non-negative, ≤ 1.0
- All tickers must exist in the price data
- Warns (but allows) if weights don't sum to 1.0

---

## Features

- **Dark mode** - Toggle in header, remembers your preference
- **Time range selector** - 1W / 1M / 3M / ALL buttons on the chart
- **Zoom** - Brush at bottom of chart to zoom into a date range
- **Dynamic chart color** - Green if price went up over the selected period, red if down
- **Sortable table** - Click column headers to sort holdings
- **Responsive** - Works on mobile (header stacks, table scrolls)

---

## Sample Files

The `/data` folder has files to test with:

- `bankofmontreal-e134q-1arsjzss-prices.csv` - Price history for stocks A-Z
- `bankofmontreal-e134q-5osaq2zk-ETF1.csv` - Sample ETF with 15 holdings
- `bankofmontreal-e134q-tf6omf1g-ETF2.csv` - Another sample ETF

---

## What I'd Add With More Time

- [ ] Multiple ETF comparison (overlay charts)
- [ ] Constituent breakdown chart (pie chart of weights)
- [ ] Export functionality (download chart as PNG, data as CSV)
- [ ] Date range picker (calendar UI instead of just buttons)
- [ ] Real ticker symbols instead of A-Z
- [ ] Unit tests for edge cases
- [ ] Docker compose for easier deployment
