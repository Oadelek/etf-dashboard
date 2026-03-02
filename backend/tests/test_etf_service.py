"""
Tests for ETF Service — Business Logic Layer

Covers:
- Service initialization and price loading
- ETF weight loading with validation
- Holdings, time series, and top holdings calculations
- Edge cases and error handling
"""

import pytest


class TestETFServiceInit:
    """Tests for service initialization."""

    def test_loads_price_data(self, etf_service):
        assert etf_service.prices_df is not None
        assert len(etf_service.prices_df) > 0

    def test_prices_have_date_column(self, etf_service):
        assert "DATE" in etf_service.prices_df.columns

    def test_prices_have_all_tickers(self, etf_service):
        tickers = set(etf_service.prices_df.columns) - {"DATE"}
        assert len(tickers) == 50  # 50 S&P 500 tickers
        assert "AAPL" in tickers
        assert "MSFT" in tickers

    def test_prices_sorted_by_date(self, etf_service):
        dates = etf_service.prices_df["DATE"].tolist()
        assert dates == sorted(dates)

    def test_no_etf_loaded_initially(self, etf_service):
        assert not etf_service.has_etf_loaded()


class TestETFWeightLoading:
    """Tests for ETF weight parsing and validation."""

    def test_valid_upload(self, etf_service, valid_etf_csv):
        result = etf_service.load_etf_weights(valid_etf_csv)
        assert result["constituents"] == 4
        assert abs(result["total_weight"] - 1.0) < 0.01

    def test_etf_loaded_after_upload(self, etf_service, valid_etf_csv):
        etf_service.load_etf_weights(valid_etf_csv)
        assert etf_service.has_etf_loaded()

    def test_weights_not_summing_to_one_warns(self, etf_service):
        csv = b"ticker,weight\nAAPL,0.50\nMSFT,0.20\n"
        result = etf_service.load_etf_weights(csv)
        assert "warning" in result
        assert "warning" in result

    def test_empty_csv_raises(self, etf_service):
        with pytest.raises(ValueError, match="empty"):
            etf_service.load_etf_weights(b"")

    def test_missing_columns_raises(self, etf_service):
        with pytest.raises(ValueError, match="Missing required columns"):
            etf_service.load_etf_weights(b"foo,pct\nAAPL,0.5\n")

    def test_negative_weight_raises(self, etf_service):
        with pytest.raises(ValueError, match="negative"):
            etf_service.load_etf_weights(b"ticker,weight\nAAPL,-0.5\n")

    def test_weight_over_one_raises(self, etf_service):
        with pytest.raises(ValueError, match="not percentages"):
            etf_service.load_etf_weights(b"ticker,weight\nAAPL,50\n")

    def test_duplicate_tickers_raises(self, etf_service):
        with pytest.raises(ValueError, match="Duplicate"):
            etf_service.load_etf_weights(b"ticker,weight\nAAPL,0.3\nAAPL,0.7\n")

    def test_unknown_ticker_raises(self, etf_service):
        with pytest.raises(ValueError, match="not found"):
            etf_service.load_etf_weights(b"ticker,weight\nZZZZZ,0.5\n")

    def test_all_zero_weights_raises(self, etf_service):
        with pytest.raises(ValueError, match="zero"):
            etf_service.load_etf_weights(b"ticker,weight\nAAPL,0\nMSFT,0\n")

    def test_case_insensitive_columns(self, etf_service):
        csv = b"Ticker,Weight\nAAPL,0.50\nMSFT,0.50\n"
        result = etf_service.load_etf_weights(csv)
        assert result["constituents"] == 2


class TestETFCalculations:
    """Tests for ETF price and holdings calculations."""

    def test_holdings_table_returns_data(self, etf_service, valid_etf_csv):
        etf_service.load_etf_weights(valid_etf_csv)
        holdings = etf_service.get_holdings_table()
        assert len(holdings) == 4
        assert all("name" in h for h in holdings)
        assert all("weight" in h for h in holdings)
        assert all("latest_price" in h for h in holdings)

    def test_holdings_sorted_by_weight_desc(self, etf_service, valid_etf_csv):
        etf_service.load_etf_weights(valid_etf_csv)
        holdings = etf_service.get_holdings_table()
        weights = [h["weight"] for h in holdings]
        assert weights == sorted(weights, reverse=True)

    def test_time_series_has_data(self, etf_service, valid_etf_csv):
        etf_service.load_etf_weights(valid_etf_csv)
        ts = etf_service.get_etf_time_series()
        assert len(ts) > 0
        assert all("date" in d for d in ts)
        assert all("price" in d for d in ts)

    def test_time_series_prices_positive(self, etf_service, valid_etf_csv):
        etf_service.load_etf_weights(valid_etf_csv)
        ts = etf_service.get_etf_time_series()
        assert all(d["price"] > 0 for d in ts)

    def test_time_series_dates_sorted(self, etf_service, valid_etf_csv):
        etf_service.load_etf_weights(valid_etf_csv)
        ts = etf_service.get_etf_time_series()
        dates = [d["date"] for d in ts]
        assert dates == sorted(dates)

    def test_top_holdings_respects_n(self, etf_service, valid_etf_csv):
        etf_service.load_etf_weights(valid_etf_csv)
        top_3 = etf_service.get_top_holdings(3)
        assert len(top_3) == 3

    def test_top_holdings_sorted_by_value(self, etf_service, valid_etf_csv):
        etf_service.load_etf_weights(valid_etf_csv)
        top = etf_service.get_top_holdings(5)
        values = [h["holding_value"] for h in top]
        assert values == sorted(values, reverse=True)

    def test_empty_results_when_no_etf(self, etf_service):
        assert etf_service.get_holdings_table() == []
        assert etf_service.get_etf_time_series() == []
        assert etf_service.get_top_holdings() == []

    def test_latest_date_format(self, etf_service):
        date = etf_service.get_latest_date()
        assert len(date) == 10  # YYYY-MM-DD
        assert date.count("-") == 2

    def test_weighted_price_calculation(self, etf_service):
        """Verify the weighted price math is correct."""
        csv = b"ticker,weight\nAAPL,1.0\n"  # 100% in AAPL
        etf_service.load_etf_weights(csv)
        ts = etf_service.get_etf_time_series()

        # ETF price should equal AAPL's price when 100% weight on AAPL
        first_etf_price = ts[0]["price"]
        first_aapl_price = float(etf_service.prices_df.iloc[0]["AAPL"])
        assert abs(first_etf_price - round(first_aapl_price, 2)) < 0.01
