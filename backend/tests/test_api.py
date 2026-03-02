"""
Tests for FastAPI Endpoints — Integration Layer

Covers:
- Health check
- File upload (valid and invalid)
- Dashboard data endpoints
- v2 SQL-powered endpoints
- Error responses
"""

import pytest


class TestHealthCheck:
    """Root endpoint tests."""

    def test_health_check_returns_200(self, client):
        response = client.get("/")
        assert response.status_code == 200

    def test_health_check_has_status(self, client):
        data = client.get("/").json()
        assert data["status"] == "healthy"
        assert "etf_loaded" in data


class TestUploadEndpoint:
    """POST /api/upload tests."""

    def test_upload_valid_csv(self, client):
        csv = b"ticker,weight\nAAPL,0.30\nMSFT,0.25\nGOOGL,0.25\nAMZN,0.20\n"
        response = client.post(
            "/api/upload",
            files={"file": ("etf.csv", csv, "text/csv")},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["holdings"]) == 4
        assert len(data["etf_prices"]) > 0
        assert len(data["top_holdings"]) > 0

    def test_upload_non_csv_rejected(self, client):
        response = client.post(
            "/api/upload",
            files={"file": ("data.txt", b"hello", "text/plain")},
        )
        assert response.status_code == 400

    def test_upload_empty_file_rejected(self, client):
        response = client.post(
            "/api/upload",
            files={"file": ("empty.csv", b"", "text/csv")},
        )
        assert response.status_code == 400

    def test_upload_invalid_columns_rejected(self, client):
        response = client.post(
            "/api/upload",
            files={"file": ("bad.csv", b"foo,bar\n1,2\n", "text/csv")},
        )
        assert response.status_code == 400

    def test_upload_returns_filename(self, client):
        csv = b"ticker,weight\nAAPL,0.50\nMSFT,0.50\n"
        data = client.post(
            "/api/upload",
            files={"file": ("my_etf.csv", csv, "text/csv")},
        ).json()
        assert data["filename"] == "my_etf.csv"


class TestDashboardEndpoints:
    """GET endpoints for dashboard data."""

    def _upload_sample(self, client):
        """Helper: upload a sample ETF so endpoints work."""
        csv = b"ticker,weight\nAAPL,0.30\nMSFT,0.25\nGOOGL,0.25\nAMZN,0.10\nTSLA,0.10\n"
        client.post(
            "/api/upload",
            files={"file": ("test.csv", csv, "text/csv")},
        )

    def test_holdings_after_upload(self, client):
        self._upload_sample(client)
        response = client.get("/api/holdings")
        assert response.status_code == 200
        data = response.json()
        assert "holdings" in data
        assert "latest_date" in data

    def test_etf_prices_after_upload(self, client):
        self._upload_sample(client)
        response = client.get("/api/etf-prices")
        assert response.status_code == 200
        assert "prices" in response.json()

    def test_top_holdings_after_upload(self, client):
        self._upload_sample(client)
        response = client.get("/api/top-holdings")
        assert response.status_code == 200
        data = response.json()
        assert len(data["top_holdings"]) <= 5
        assert len(data["top_holdings"]) > 0


class TestV2Endpoints:
    """SQL-powered v2 endpoints."""

    def test_db_stats(self, client):
        response = client.get("/api/v2/db-stats")
        assert response.status_code == 200
        data = response.json()
        assert "total_constituents" in data

    def test_etf_list(self, client):
        response = client.get("/api/v2/etfs")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_price_summary(self, client):
        response = client.get("/api/v2/analytics/price-summary")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_moving_averages_requires_ticker(self, client):
        response = client.get("/api/v2/analytics/moving-average")
        # Should get 422 (validation error) without required query param
        assert response.status_code == 422

    def test_moving_averages_with_ticker(self, client):
        response = client.get("/api/v2/analytics/moving-average?ticker=AAPL")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_correlation_requires_tickers(self, client):
        response = client.get("/api/v2/analytics/correlation")
        assert response.status_code == 422

    def test_correlation_with_tickers(self, client):
        response = client.get("/api/v2/analytics/correlation?ticker_a=AAPL&ticker_b=MSFT")
        assert response.status_code == 200
