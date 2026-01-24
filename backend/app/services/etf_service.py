"""
ETF Service Module

This module handles all ETF-related calculations:
- Loading and caching price data
- Computing weighted ETF prices (reconstructed time series)
- Computing holdings with latest prices
- Computing top holdings by market value

Assumptions:
- Price data is static and loaded once at startup
- ETF weights are uploaded per session and stored in memory
- All dates in price data are trading days (no holiday filtering)
- Weight values represent percentage of portfolio (should sum close to 1.0)
"""

import pandas as pd
from pathlib import Path
from typing import Optional
import io


class ETFService:
    """Service class for ETF calculations and data management."""
    
    def __init__(self, prices_csv_path: str):
        """
        Initialize the ETF service with price data.
        
        Args:
            prices_csv_path: Path to the CSV file containing historical prices
        """
        self.prices_df = self._load_prices(prices_csv_path)
        self.current_etf_weights: Optional[pd.DataFrame] = None
    
    def _load_prices(self, csv_path: str) -> pd.DataFrame:
        """
        Load price data from CSV and parse dates.
        
        The prices CSV is expected to have:
        - First column: DATE (YYYY-MM-DD format)
        - Remaining columns: Constituent tickers (A, B, C, etc.)
        """
        df = pd.read_csv(csv_path, parse_dates=['DATE'])
        df = df.sort_values('DATE').reset_index(drop=True)
        return df
    
    def load_etf_weights(self, csv_content: bytes) -> dict:
        """
        Load ETF weights from uploaded CSV content.
        
        Args:
            csv_content: Raw bytes of the uploaded CSV file
            
        Returns:
            dict with parsed weight information
            
        Expected CSV format:
        - name: Constituent identifier (A, B, C, etc.)
        - weight: Decimal weight (e.g., 0.15 for 15%)
        """
        self.current_etf_weights = pd.read_csv(io.BytesIO(csv_content))
        
        # Validate that all constituents exist in price data
        available_tickers = set(self.prices_df.columns) - {'DATE'}
        etf_tickers = set(self.current_etf_weights['name'].values)
        missing = etf_tickers - available_tickers
        
        if missing:
            raise ValueError(f"Constituents not found in price data: {missing}")
        
        return {
            "constituents": len(self.current_etf_weights),
            "total_weight": float(self.current_etf_weights['weight'].sum())
        }
    
    def get_holdings_table(self) -> list[dict]:
        """
        Get holdings table with latest prices.
        
        Returns:
            List of dicts with: name, weight, latest_price
        """
        if self.current_etf_weights is None:
            return []
        
        # Get the most recent date's prices
        latest_row = self.prices_df.iloc[-1]
        
        holdings = []
        for _, row in self.current_etf_weights.iterrows():
            name = row['name']
            weight = row['weight']
            latest_price = float(latest_row[name])
            
            holdings.append({
                "name": name,
                "weight": float(weight),
                "latest_price": round(latest_price, 2)
            })
        
        # Sort by weight descending for better UX
        holdings.sort(key=lambda x: x['weight'], reverse=True)
        return holdings
    
    def get_etf_time_series(self) -> list[dict]:
        """
        Calculate the reconstructed ETF price time series.
        
        The ETF price is computed as the weighted sum of constituent prices:
        ETF_price(t) = Σ (weight_i × price_i(t))
        
        Returns:
            List of dicts with: date, price
        """
        if self.current_etf_weights is None:
            return []
        
        # Create a weight lookup
        weights = dict(zip(
            self.current_etf_weights['name'],
            self.current_etf_weights['weight']
        ))
        
        time_series = []
        for _, row in self.prices_df.iterrows():
            weighted_price = 0.0
            for ticker, weight in weights.items():
                weighted_price += weight * row[ticker]
            
            time_series.append({
                "date": row['DATE'].strftime('%Y-%m-%d'),
                "price": round(weighted_price, 2)
            })
        
        return time_series
    
    def get_top_holdings(self, n: int = 5) -> list[dict]:
        """
        Get top N holdings by market value (weight × latest price).
        
        This represents the dollar value contribution of each holding
        assuming a $1 investment in the ETF.
        
        Args:
            n: Number of top holdings to return
            
        Returns:
            List of dicts with: name, weight, latest_price, holding_value
        """
        if self.current_etf_weights is None:
            return []
        
        # Get the most recent date's prices
        latest_row = self.prices_df.iloc[-1]
        
        holdings = []
        for _, row in self.current_etf_weights.iterrows():
            name = row['name']
            weight = float(row['weight'])
            latest_price = float(latest_row[name])
            holding_value = weight * latest_price  # Dollar contribution per unit
            
            holdings.append({
                "name": name,
                "weight": weight,
                "latest_price": round(latest_price, 2),
                "holding_value": round(holding_value, 2)
            })
        
        # Sort by holding value descending and take top N
        holdings.sort(key=lambda x: x['holding_value'], reverse=True)
        return holdings[:n]
    
    def get_latest_date(self) -> str:
        """Get the most recent date in the price data."""
        return self.prices_df['DATE'].iloc[-1].strftime('%Y-%m-%d')
    
    def has_etf_loaded(self) -> bool:
        """Check if an ETF has been loaded."""
        return self.current_etf_weights is not None
