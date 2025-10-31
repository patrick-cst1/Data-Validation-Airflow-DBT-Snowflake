"""
Great Expectations configuration for ML feature validation.

This module sets up expectations for:
- Statistical validation of features
- Data drift detection
- Anomaly detection
- Distribution checks

Author: Patrick Cheung
Date: October 2025
"""

import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
import os


class MLFeatureValidator:
    """Validator for ML features using Great Expectations."""
    
    def __init__(self, context_root_dir="great_expectations"):
        """Initialize Great Expectations context."""
        self.context = gx.get_context(context_root_dir=context_root_dir)
    
    def create_customer_features_expectations(self):
        """
        Create expectations for customer_features mart table.
        Validates features used for ML modeling.
        """
        
        suite_name = "customer_features_suite"
        
        suite = self.context.add_expectation_suite(
            expectation_suite_name=suite_name
        )
        
        # Basic completeness checks
        expectations = [
            # Primary key
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "customer_id"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "customer_id"}
            },
            
            # Numeric features - range validation
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "total_orders",
                    "min_value": 0,
                    "max_value": 1000
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "total_spend",
                    "min_value": 0,
                    "max_value": 100000
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "avg_order_value",
                    "min_value": 0,
                    "max_value": 10000
                }
            },
            
            # Rate/ratio features - should be between 0 and 1
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "conversion_rate",
                    "min_value": 0,
                    "max_value": 1
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "cancellation_rate",
                    "min_value": 0,
                    "max_value": 1
                }
            },
            
            # RFM features
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "recency_days",
                    "min_value": 0,
                    "max_value": 1095  # 3 years
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "frequency",
                    "min_value": 0,
                    "max_value": 1000
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "monetary_value",
                    "min_value": 0,
                    "max_value": 100000
                }
            },
            
            # Statistical distribution checks
            {
                "expectation_type": "expect_column_mean_to_be_between",
                "kwargs": {
                    "column": "total_orders",
                    "min_value": 1,
                    "max_value": 50
                }
            },
            {
                "expectation_type": "expect_column_stdev_to_be_between",
                "kwargs": {
                    "column": "total_spend",
                    "min_value": 0,
                    "max_value": 10000
                }
            },
            
            # Completeness - critical features should have high non-null rate
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "total_orders",
                    "mostly": 1.0
                }
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "recency_days",
                    "mostly": 0.95  # Allow 5% null for customers with no orders
                }
            },
        ]
        
        for expectation in expectations:
            suite.add_expectation(**expectation)
        
        self.context.save_expectation_suite(suite)
        print(f"✓ Created expectation suite: {suite_name}")
        
        return suite_name
    
    def create_daily_metrics_expectations(self):
        """
        Create expectations for daily_metrics mart table.
        Monitors data quality trends over time.
        """
        
        suite_name = "daily_metrics_suite"
        
        suite = self.context.add_expectation_suite(
            expectation_suite_name=suite_name
        )
        
        expectations = [
            # Date uniqueness
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "metric_date"}
            },
            
            # Data quality scores should be high
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "order_amount_quality_score",
                    "min_value": 0.90,  # 90% threshold
                    "max_value": 1.0,
                    "mostly": 0.95  # 95% of days should meet this
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "order_status_quality_score",
                    "min_value": 0.90,
                    "max_value": 1.0,
                    "mostly": 0.95
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "event_type_quality_score",
                    "min_value": 0.90,
                    "max_value": 1.0,
                    "mostly": 0.95
                }
            },
            
            # Business metrics sanity checks
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "total_orders",
                    "min_value": 0,
                    "max_value": 10000
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "total_revenue",
                    "min_value": 0,
                    "max_value": 1000000
                }
            },
            
            # Detect anomalies - revenue shouldn't drop to zero
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "total_revenue",
                    "min_value": 100,  # At least $100/day
                    "mostly": 0.90
                }
            },
        ]
        
        for expectation in expectations:
            suite.add_expectation(**expectation)
        
        self.context.save_expectation_suite(suite)
        print(f"✓ Created expectation suite: {suite_name}")
        
        return suite_name
    
    def run_validation(self, suite_name: str, batch_request: dict):
        """
        Run validation checkpoint for a given suite.
        """
        
        checkpoint_name = f"{suite_name}_checkpoint"
        
        checkpoint_config = {
            "name": checkpoint_name,
            "config_version": 1.0,
            "class_name": "SimpleCheckpoint",
            "run_name_template": f"{suite_name}_%Y%m%d-%H%M%S",
        }
        
        self.context.add_checkpoint(**checkpoint_config)
        
        result = self.context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            batch_request=batch_request,
            expectation_suite_name=suite_name,
        )
        
        return result


def setup_great_expectations():
    """
    Initialize Great Expectations and create all expectation suites.
    """
    
    print("Setting up Great Expectations...")
    print("=" * 60)
    
    validator = MLFeatureValidator()
    
    # Create expectation suites
    validator.create_customer_features_expectations()
    validator.create_daily_metrics_expectations()
    
    print("\n" + "=" * 60)
    print("Great Expectations setup complete!")
    print("=" * 60)
    print("\nExpectation suites created:")
    print("  - customer_features_suite")
    print("  - daily_metrics_suite")
    print("\nThese suites validate:")
    print("  • Feature value ranges")
    print("  • Statistical distributions")
    print("  • Data quality scores")
    print("  • Anomaly detection")


if __name__ == "__main__":
    setup_great_expectations()
