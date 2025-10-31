"""
Airflow connection setup utility.
Programmatically create Snowflake connection in Airflow.

Author: Patrick Cheung
Date: October 2025
"""

import os
from dotenv import load_dotenv
from airflow.models import Connection
from airflow import settings

load_dotenv()


def create_snowflake_connection():
    """Create Snowflake connection in Airflow."""
    
    conn_id = 'snowflake_default'
    
    # Check if connection already exists
    session = settings.Session()
    existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if existing:
        print(f"Connection '{conn_id}' already exists. Updating...")
        session.delete(existing)
        session.commit()
    
    # Create new connection
    conn = Connection(
        conn_id=conn_id,
        conn_type='snowflake',
        login=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        schema=os.getenv('SNOWFLAKE_SCHEMA', 'RAW'),
        extra={
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'region': '',
        }
    )
    
    session.add(conn)
    session.commit()
    session.close()
    
    print(f"✓ Snowflake connection '{conn_id}' created successfully")


if __name__ == "__main__":
    print("Creating Airflow connections...")
    create_snowflake_connection()
    print("\nConnection setup complete!")
