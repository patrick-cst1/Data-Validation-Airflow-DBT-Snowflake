"""
Unit tests for data quality macros and validation logic.

Author: Patrick Cheung
Date: October 2025
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta


class TestDataQualityValidations:
    """Test data quality validation functions."""
    
    def test_completeness_calculation(self):
        """Test completeness score calculation."""
        # Create test dataframe
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10, None, 30, None, 50]
        })
        
        completeness = df['value'].notna().sum() / len(df)
        assert completeness == 0.6, "Completeness should be 60%"
    
    def test_duplicate_detection(self):
        """Test duplicate detection logic."""
        df = pd.DataFrame({
            'id': [1, 2, 2, 3, 4],
            'name': ['A', 'B', 'B', 'C', 'D']
        })
        
        duplicates = df[df.duplicated(subset=['id', 'name'], keep=False)]
        assert len(duplicates) == 2, "Should find 2 duplicate rows"
    
    def test_value_range_validation(self):
        """Test value range validation."""
        df = pd.DataFrame({
            'amount': [10, 20, 150, 5, -10]
        })
        
        min_val, max_val = 0, 100
        out_of_range = df[(df['amount'] < min_val) | (df['amount'] > max_val)]
        assert len(out_of_range) == 2, "Should find 2 out-of-range values"
    
    def test_freshness_check(self):
        """Test data freshness validation."""
        now = datetime.now()
        df = pd.DataFrame({
            'timestamp': [
                now - timedelta(hours=1),
                now - timedelta(hours=12),
                now - timedelta(hours=48)
            ]
        })
        
        max_hours = 24
        stale_records = df[
            (now - df['timestamp']) > timedelta(hours=max_hours)
        ]
        assert len(stale_records) == 1, "Should find 1 stale record"
    
    def test_null_percentage_calculation(self):
        """Test null percentage calculation."""
        df = pd.DataFrame({
            'col1': [1, 2, None, 4, 5],
            'col2': [None, None, None, 4, 5]
        })
        
        null_pct_col1 = df['col1'].isna().sum() / len(df)
        null_pct_col2 = df['col2'].isna().sum() / len(df)
        
        assert null_pct_col1 == 0.2, "Col1 should have 20% nulls"
        assert null_pct_col2 == 0.6, "Col2 should have 60% nulls"


class TestFeatureValidations:
    """Test ML feature validation logic."""
    
    def test_rfm_calculation(self):
        """Test RFM metrics calculation."""
        # Sample customer data
        last_order_date = datetime(2025, 10, 1)
        first_order_date = datetime(2025, 1, 1)
        current_date = datetime(2025, 10, 31)
        
        recency = (current_date - last_order_date).days
        lifetime = (last_order_date - first_order_date).days
        
        assert recency == 30, "Recency should be 30 days"
        assert lifetime == 273, "Lifetime should be 273 days"
    
    def test_conversion_rate_calculation(self):
        """Test conversion rate calculation."""
        purchase_events = 10
        total_sessions = 100
        
        conversion_rate = purchase_events / total_sessions
        
        assert conversion_rate == 0.1, "Conversion rate should be 10%"
        assert 0 <= conversion_rate <= 1, "Conversion rate should be between 0 and 1"
    
    def test_quality_score_calculation(self):
        """Test data quality score calculation."""
        total_records = 100
        invalid_records = 5
        
        quality_score = 1 - (invalid_records / total_records)
        
        assert quality_score == 0.95, "Quality score should be 95%"
        assert 0 <= quality_score <= 1, "Quality score should be between 0 and 1"


class TestDataTransformations:
    """Test data transformation logic."""
    
    def test_email_normalization(self):
        """Test email normalization."""
        emails = [
            "Test@Example.com",
            " user@domain.com ",
            "ADMIN@SITE.COM"
        ]
        
        normalized = [email.lower().strip() for email in emails]
        
        assert normalized[0] == "test@example.com"
        assert normalized[1] == "user@domain.com"
        assert normalized[2] == "admin@site.com"
    
    def test_date_parsing(self):
        """Test date parsing logic."""
        date_str = "2025-10-31"
        parsed = pd.to_datetime(date_str)
        
        assert parsed.year == 2025
        assert parsed.month == 10
        assert parsed.day == 31
    
    def test_aggregation_logic(self):
        """Test aggregation calculations."""
        df = pd.DataFrame({
            'customer_id': ['C1', 'C1', 'C2', 'C2', 'C2'],
            'order_amount': [10, 20, 15, 25, 10]
        })
        
        agg = df.groupby('customer_id').agg({
            'order_amount': ['count', 'sum', 'mean']
        })
        
        assert agg.loc['C1', ('order_amount', 'count')] == 2
        assert agg.loc['C1', ('order_amount', 'sum')] == 30
        assert agg.loc['C2', ('order_amount', 'mean')] == pytest.approx(16.67, 0.01)


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
