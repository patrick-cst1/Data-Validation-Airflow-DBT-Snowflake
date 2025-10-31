"""
Snowflake initialization script.
Creates necessary database objects for the data quality pipeline.

Author: Patrick Cheung
Date: October 2025
"""

import os
import sys
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import DictCursor

# Load environment variables
load_dotenv()

def get_snowflake_connection():
    """Create Snowflake connection."""
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    )


def init_database():
    """Initialize Snowflake database and schemas."""
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        database = os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DWH')
        
        print(f"Initializing Snowflake database: {database}")
        
        # Create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        print(f"✓ Database '{database}' created/verified")
        
        # Use database
        cursor.execute(f"USE DATABASE {database}")
        
        # Create schemas
        schemas = ['RAW', 'STAGING', 'MART']
        for schema in schemas:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            print(f"✓ Schema '{schema}' created/verified")
        
        # Create raw tables
        print("\nCreating raw tables...")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.customers (
                customer_id VARCHAR(20),
                email VARCHAR(255),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                date_of_birth DATE,
                country VARCHAR(2),
                city VARCHAR(100),
                signup_date TIMESTAMP,
                customer_segment VARCHAR(20),
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        print("✓ Table 'raw.customers' created")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.orders (
                order_id VARCHAR(20),
                customer_id VARCHAR(20),
                order_date TIMESTAMP,
                order_status VARCHAR(20),
                total_amount DECIMAL(10,2),
                payment_method VARCHAR(50),
                shipping_cost DECIMAL(10,2),
                discount_amount DECIMAL(10,2),
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        print("✓ Table 'raw.orders' created")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.events (
                event_id VARCHAR(20),
                customer_id VARCHAR(20),
                event_type VARCHAR(50),
                event_timestamp TIMESTAMP,
                page_url VARCHAR(500),
                product_id VARCHAR(20),
                session_id VARCHAR(50),
                device_type VARCHAR(20),
                _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        print("✓ Table 'raw.events' created")
        
        print("\n" + "="*60)
        print("Snowflake initialization complete!")
        print("="*60)
        print(f"\nDatabase: {database}")
        print(f"Schemas: {', '.join(schemas)}")
        print(f"Raw tables: customers, orders, events")
        
    except Exception as e:
        print(f"❌ Error during initialization: {e}")
        sys.exit(1)
    
    finally:
        cursor.close()
        conn.close()


def verify_connection():
    """Verify Snowflake connection."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"✓ Connected to Snowflake (version: {version})")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    print("Snowflake Initialization Script")
    print("=" * 60)
    
    # Verify connection
    if not verify_connection():
        print("\n⚠️  Please check your Snowflake credentials in .env file")
        sys.exit(1)
    
    # Initialize database
    init_database()
