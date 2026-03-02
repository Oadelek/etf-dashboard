"""
Shared pytest fixtures for the Spectra test suite.
"""

import pytest
import sys
from pathlib import Path
from fastapi.testclient import TestClient

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.app.main import app
from backend.app.services.etf_service import ETFService
from backend.app.database import init_db, SessionLocal, Base, engine


@pytest.fixture
def prices_csv_path():
    """Path to the prices CSV in the data/ folder."""
    return str(
        Path(__file__).parent.parent.parent / "data" / "prices.csv"
    )


@pytest.fixture
def etf_service(prices_csv_path):
    """An ETFService instance pre-loaded with price data."""
    return ETFService(prices_csv_path)


@pytest.fixture
def valid_etf_csv():
    """Valid ETF weights CSV content as bytes."""
    return b"ticker,weight\nAAPL,0.30\nMSFT,0.25\nGOOGL,0.25\nAMZN,0.20\n"


@pytest.fixture
def client():
    """FastAPI TestClient for integration testing."""
    return TestClient(app)


@pytest.fixture
def db_session():
    """Database session for DB-layer tests."""
    init_db()
    session = SessionLocal()
    yield session
    session.close()
