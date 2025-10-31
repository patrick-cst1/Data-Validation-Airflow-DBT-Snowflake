"""
Main Airflow DAG for E-Commerce Data Quality Pipeline.

This DAG orchestrates:
1. Data ingestion from CSV to Snowflake
2. DBT transformation and validation
3. Data quality checks with Great Expectations
4. Alerting on quality issues

Author: Patrick Cheung
Date: October 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup
import pandas as pd
import os

# Default arguments
default_args = {
    'owner': 'patrick_cheung',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ecommerce_data_quality_pipeline',
    default_args=default_args,
    description='E-commerce data quality pipeline with Airflow, DBT, and Snowflake',
    schedule_interval='@daily',
    catchup=False,
    tags=['data-quality', 'ml', 'ecommerce'],
)


def ingest_csv_to_snowflake(table_name: str, csv_path: str, **context):
    """
    Ingest CSV data into Snowflake raw tables.
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Read CSV
    df = pd.read_csv(csv_path)
    
    # Get connection
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Create table if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join([f"{col} VARCHAR" for col in df.columns])}
            )
        """)
        
        # Write dataframe to Snowflake
        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.split('.')[-1],
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema='RAW'
        )
        
        print(f"✓ Loaded {nrows} rows into {table_name}")
        
    finally:
        cursor.close()
        conn.close()


def validate_data_quality(**context):
    """
    Run custom data quality validations and push results to XCom.
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    quality_checks = {
        'customers_completeness': """
            SELECT 
                COUNT(*) as total_rows,
                COUNT(customer_id) as non_null_customer_id,
                COUNT(email) as non_null_email,
                COUNT(email)::FLOAT / COUNT(*) as email_completeness
            FROM staging.stg_customers
        """,
        'orders_validity': """
            SELECT 
                COUNT(*) as total_orders,
                SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END) as negative_amounts,
                SUM(CASE WHEN invalid_status_flag = 1 THEN 1 ELSE 0 END) as invalid_statuses
            FROM staging.stg_orders
        """,
        'events_quality': """
            SELECT 
                COUNT(*) as total_events,
                SUM(CASE WHEN invalid_event_type_flag = 1 THEN 1 ELSE 0 END) as invalid_event_types,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM staging.stg_events
        """
    }
    
    results = {}
    for check_name, query in quality_checks.items():
        result = hook.get_first(query)
        results[check_name] = result
        print(f"✓ Quality check '{check_name}': {result}")
    
    # Push results to XCom
    context['ti'].xcom_push(key='quality_results', value=results)
    
    return results


def alert_on_quality_issues(**context):
    """
    Check quality results and alert if thresholds are breached.
    """
    results = context['ti'].xcom_pull(key='quality_results', task_ids='validate_quality')
    
    issues = []
    
    # Check email completeness
    if results['customers_completeness'][3] < 0.95:  # 95% threshold
        issues.append(f"Customer email completeness: {results['customers_completeness'][3]:.2%}")
    
    # Check for negative amounts
    if results['orders_validity'][1] > 0:
        issues.append(f"Found {results['orders_validity'][1]} orders with negative amounts")
    
    # Check for invalid statuses
    if results['orders_validity'][2] > 0:
        issues.append(f"Found {results['orders_validity'][2]} orders with invalid status")
    
    # Check for invalid event types
    if results['events_quality'][1] > 0:
        issues.append(f"Found {results['events_quality'][1]} events with invalid types")
    
    if issues:
        print("⚠️  DATA QUALITY ISSUES DETECTED:")
        for issue in issues:
            print(f"  - {issue}")
        # In production: send to Slack, PagerDuty, etc.
    else:
        print("✓ All data quality checks passed!")
    
    return issues


# Task: Initialize Snowflake schema
init_snowflake = SnowflakeOperator(
    task_id='init_snowflake',
    snowflake_conn_id='snowflake_default',
    sql="""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS mart;
    """,
    dag=dag,
)

# Task Group: Data Ingestion
with TaskGroup('ingest_data', tooltip='Ingest CSV data to Snowflake', dag=dag) as ingest_group:
    
    ingest_customers = PythonOperator(
        task_id='ingest_customers',
        python_callable=ingest_csv_to_snowflake,
        op_kwargs={
            'table_name': 'raw.customers',
            'csv_path': '/opt/airflow/data/raw/customers.csv'
        },
    )
    
    ingest_orders = PythonOperator(
        task_id='ingest_orders',
        python_callable=ingest_csv_to_snowflake,
        op_kwargs={
            'table_name': 'raw.orders',
            'csv_path': '/opt/airflow/data/raw/orders.csv'
        },
    )
    
    ingest_events = PythonOperator(
        task_id='ingest_events',
        python_callable=ingest_csv_to_snowflake,
        op_kwargs={
            'table_name': 'raw.events',
            'csv_path': '/opt/airflow/data/raw/events.csv'
        },
    )

# Task Group: DBT transformation
with TaskGroup('dbt_transformation', tooltip='Run DBT models', dag=dag) as dbt_group:
    
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/dbt && dbt deps',
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/dbt',
            **{k: v for k, v in os.environ.items() if k.startswith('SNOWFLAKE_')}
        },
    )
    
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /opt/airflow/dbt && dbt run --models staging.*',
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/dbt',
            **{k: v for k, v in os.environ.items() if k.startswith('SNOWFLAKE_')}
        },
    )
    
    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command='cd /opt/airflow/dbt && dbt test --models staging.*',
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/dbt',
            **{k: v for k, v in os.environ.items() if k.startswith('SNOWFLAKE_')}
        },
    )
    
    dbt_run_mart = BashOperator(
        task_id='dbt_run_mart',
        bash_command='cd /opt/airflow/dbt && dbt run --models mart.*',
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/dbt',
            **{k: v for k, v in os.environ.items() if k.startswith('SNOWFLAKE_')}
        },
    )
    
    dbt_test_mart = BashOperator(
        task_id='dbt_test_mart',
        bash_command='cd /opt/airflow/dbt && dbt test --models mart.*',
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/dbt',
            **{k: v for k, v in os.environ.items() if k.startswith('SNOWFLAKE_')}
        },
    )
    
    dbt_deps >> dbt_run_staging >> dbt_test_staging >> dbt_run_mart >> dbt_test_mart

# Task: Data quality validation
validate_quality = PythonOperator(
    task_id='validate_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# Task: Alert on issues
alert_task = PythonOperator(
    task_id='alert_on_issues',
    python_callable=alert_on_quality_issues,
    dag=dag,
)

# Task: Generate quality report
generate_report = SnowflakeOperator(
    task_id='generate_quality_report',
    snowflake_conn_id='snowflake_default',
    sql="""
        CREATE OR REPLACE TABLE mart.data_quality_report AS
        SELECT
            CURRENT_TIMESTAMP() as report_timestamp,
            'DAILY_PIPELINE' as pipeline_name,
            (SELECT COUNT(*) FROM staging.stg_customers) as customers_count,
            (SELECT COUNT(*) FROM staging.stg_orders) as orders_count,
            (SELECT COUNT(*) FROM staging.stg_events) as events_count,
            (SELECT AVG(order_amount_quality_score) FROM mart.daily_metrics) as avg_order_quality,
            (SELECT AVG(event_type_quality_score) FROM mart.daily_metrics) as avg_event_quality
    """,
    dag=dag,
)

# Define task dependencies
init_snowflake >> ingest_group >> dbt_group >> validate_quality >> [alert_task, generate_report]
