"""
Database Module

SQLAlchemy models and database configuration for Spectra.

Uses SQLite for local development (easily swappable to PostgreSQL or SQL Server
in production by changing DATABASE_URL).

Schema Design (3NF Normalized):
─────────────────────────────────────────────────────
  constituents     1 ──── * prices
       │
       │ 1
       │
       * 
  etf_holdings     * ──── 1 etf_uploads
─────────────────────────────────────────────────────

- constituents: One row per tradeable security (A-Z)
- prices: One row per constituent per trading day (normalized from wide CSV)
- etf_uploads: Metadata per uploaded ETF file (audit trail)
- etf_holdings: Links an ETF to its constituent weights (junction table)

Indexes on frequently queried columns (date, ticker) for fast lookups.
Foreign key constraints ensure referential integrity.
"""

from sqlalchemy import (
    create_engine, Column, Integer, Float, String, DateTime, Date,
    ForeignKey, UniqueConstraint, Index, text
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent.parent.parent / "data" / "etf_dashboard.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

# check_same_thread=False required for SQLite with FastAPI (multi-threaded)
engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Constituent(Base):
    """
    A tradeable security in the universe.
    Real S&P 500 tickers (AAPL, MSFT, etc.) with sector/industry metadata.
    """
    __tablename__ = "constituents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(10), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=True)         # Company name
    sector = Column(String(100), nullable=True)        # GICS sector
    industry = Column(String(100), nullable=True)      # Sub-industry
    market_cap = Column(String(20), nullable=True)     # Large/Mid/Small
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    prices = relationship("Price", back_populates="constituent", cascade="all, delete-orphan")
    holdings = relationship("ETFHolding", back_populates="constituent")

    def __repr__(self):
        return f"<Constituent(ticker='{self.ticker}', sector='{self.sector}')>"


class Price(Base):
    """
    Daily OHLCV price data for a constituent.
    Full candlestick data (Open, High, Low, Close) plus Volume.
    Already in long format (one row per ticker per day).
    """
    __tablename__ = "prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    constituent_id = Column(Integer, ForeignKey("constituents.id"), nullable=False)
    date = Column(Date, nullable=False)
    open_price = Column(Float, nullable=True)
    high_price = Column(Float, nullable=True)
    low_price = Column(Float, nullable=True)
    close_price = Column(Float, nullable=False)
    volume = Column(Integer, nullable=True)

    # Relationships
    constituent = relationship("Constituent", back_populates="prices")

    # Composite unique constraint: one price per constituent per day
    # Indexes for common query patterns
    __table_args__ = (
        UniqueConstraint("constituent_id", "date", name="uq_constituent_date"),
        Index("ix_prices_date", "date"),
        Index("ix_prices_constituent_date", "constituent_id", "date"),
    )

    def __repr__(self):
        return f"<Price(constituent_id={self.constituent_id}, date={self.date}, close={self.close_price})>"


class ETFUpload(Base):
    """
    Metadata for each ETF file upload.
    Tracks upload history for audit trail — who uploaded what and when.
    is_active flag allows soft-delete / deactivation without losing history.
    """
    __tablename__ = "etf_uploads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    total_weight = Column(Float, nullable=False)
    num_constituents = Column(Integer, nullable=False)
    is_active = Column(Integer, default=1)  # SQLite has no native boolean

    # Relationships
    holdings = relationship("ETFHolding", back_populates="etf_upload", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<ETFUpload(id={self.id}, filename='{self.filename}')>"


class ETFHolding(Base):
    """
    Individual holding within an uploaded ETF.
    Junction table linking etf_uploads ↔ constituents with a weight.
    """
    __tablename__ = "etf_holdings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    etf_upload_id = Column(Integer, ForeignKey("etf_uploads.id"), nullable=False)
    constituent_id = Column(Integer, ForeignKey("constituents.id"), nullable=False)
    weight = Column(Float, nullable=False)

    # Relationships
    etf_upload = relationship("ETFUpload", back_populates="holdings")
    constituent = relationship("Constituent", back_populates="holdings")

    __table_args__ = (
        UniqueConstraint("etf_upload_id", "constituent_id", name="uq_upload_constituent"),
    )

    def __repr__(self):
        return f"<ETFHolding(etf={self.etf_upload_id}, ticker_id={self.constituent_id}, weight={self.weight})>"


# ---------------------------------------------------------------------------
# Database Utilities
# ---------------------------------------------------------------------------

def init_db():
    """Create all tables if they don't exist. Safe to call multiple times."""
    Base.metadata.create_all(bind=engine)


def drop_db():
    """Drop all tables. Use with caution — for testing/reset only."""
    Base.metadata.drop_all(bind=engine)


def get_db():
    """
    FastAPI dependency — yields a database session per request.

    Usage in endpoint:
        @app.get("/example")
        def example(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
