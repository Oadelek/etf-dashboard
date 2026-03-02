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
        
        Supports two formats:
        - Wide format: DATE, A, B, C, ... (legacy)
        - Long format: date, ticker, open, high, low, close, volume (new)
        
        Always returns wide format for calculations.
        """
        df = pd.read_csv(csv_path)
        
        # Detect format: long (has 'ticker' column) vs wide (columns are tickers)
        if 'ticker' in df.columns:
            # Long format → pivot to wide on close price
            df['date'] = pd.to_datetime(df['date'])
            wide = df.pivot(index='date', columns='ticker', values='close').reset_index()
            wide = wide.rename(columns={'date': 'DATE'})
            wide = wide.sort_values('DATE').reset_index(drop=True)
            return wide
        else:
            # Legacy wide format
            df['DATE'] = pd.to_datetime(df['DATE'])
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
        try:
            df = pd.read_csv(io.BytesIO(csv_content))
        except pd.errors.EmptyDataError:
            raise ValueError("CSV file is empty or contains no data")
        except pd.errors.ParserError as e:
            raise ValueError(f"Invalid CSV format: {str(e)}")
        
        # Normalize column names (case-insensitive)
        df.columns = df.columns.str.lower().str.strip()
        
        # Support both 'name' and 'ticker' as the identifier column
        if 'ticker' in df.columns and 'name' not in df.columns:
            df = df.rename(columns={'ticker': 'name'})
        
        # Validate required columns exist
        required_columns = {'name', 'weight'}
        actual_columns = set(df.columns)
        
        missing_columns = required_columns - actual_columns
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}. CSV must have 'name' and 'weight' columns.")
        
        # Remove any completely empty rows
        df = df.dropna(how='all')
        
        # Validate we have at least one holding
        if len(df) == 0:
            raise ValueError("CSV contains no holdings data")
        
        # Validate 'name' column
        if df['name'].isna().any():
            raise ValueError("'name' column contains empty values")
        
        # Check for duplicate constituents
        duplicates = df['name'].duplicated()
        if duplicates.any():
            dup_names = df.loc[duplicates, 'name'].tolist()
            raise ValueError(f"Duplicate constituent names found: {dup_names}")
        
        # Validate 'weight' column - must be numeric
        if df['weight'].isna().any():
            raise ValueError("'weight' column contains empty values")
        
        try:
            df['weight'] = pd.to_numeric(df['weight'])
        except (ValueError, TypeError):
            raise ValueError("'weight' column must contain numeric values")
        
        # Validate weight values are reasonable
        if (df['weight'] < 0).any():
            raise ValueError("Weights cannot be negative")
        
        if (df['weight'] > 1).any():
            raise ValueError("Weights should be decimal values (e.g., 0.15 for 15%), not percentages. Found values > 1.")
        
        if (df['weight'] == 0).all():
            raise ValueError("All weights are zero")
        
        total_weight = df['weight'].sum()
        if total_weight <= 0:
            raise ValueError("Total weight must be greater than zero")
        
        # Warn if weights don't sum to approximately 1 (allow 0.001 tolerance)
        # This is a soft validation - we still accept it
        weight_warning = None
        if abs(total_weight - 1.0) > 0.01:
            weight_warning = f"Weights sum to {total_weight:.4f}, not 1.0"
        
        # Validate that all constituents exist in price data
        available_tickers = set(self.prices_df.columns) - {'DATE'}
        etf_tickers = set(df['name'].values)
        missing = etf_tickers - available_tickers
        
        if missing:
            raise ValueError(f"Constituents not found in price data: {sorted(missing)}. Available: {sorted(available_tickers)}")
        
        # Limit number of holdings (sanity check)
        if len(df) > 1000:
            raise ValueError(f"Too many holdings ({len(df)}). Maximum allowed is 1000.")
        
        # All validations passed - store the weights
        self.current_etf_weights = df
        
        result = {
            "constituents": len(self.current_etf_weights),
            "total_weight": float(total_weight)
        }
        
        if weight_warning:
            result["warning"] = weight_warning
        
        return result
    
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
