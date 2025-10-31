"""
Generate synthetic e-commerce data for testing data quality pipeline.

Author: Patrick Cheung
Date: October 2025
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# Configuration
NUM_CUSTOMERS = 1000
NUM_ORDERS = 5000
NUM_EVENTS = 15000
OUTPUT_DIR = "data/raw"

# Data quality issues to introduce (for testing)
NULL_RATE = 0.03  # 3% null values
DUPLICATE_RATE = 0.02  # 2% duplicates
INVALID_STATUS_RATE = 0.01  # 1% invalid statuses


def generate_customers(n=NUM_CUSTOMERS):
    """Generate customer data with intentional quality issues."""
    
    customers = []
    customer_ids = [f"CUST{str(i).zfill(6)}" for i in range(1, n + 1)]
    
    for customer_id in customer_ids:
        # Intentionally introduce nulls
        email = fake.email() if random.random() > NULL_RATE else None
        first_name = fake.first_name() if random.random() > NULL_RATE else None
        last_name = fake.last_name() if random.random() > NULL_RATE else None
        dob = fake.date_of_birth(minimum_age=18, maximum_age=80) if random.random() > NULL_RATE else None
        
        customers.append({
            'customer_id': customer_id,
            'email': email,
            'first_name': first_name,
            'last_name': last_name,
            'date_of_birth': dob,
            'country': fake.country_code(),
            'city': fake.city(),
            'signup_date': fake.date_time_between(start_date='-3y', end_date='now'),
            'customer_segment': random.choice(['VIP', 'REGULAR', 'NEW', 'INACTIVE']),
            '_loaded_at': datetime.now()
        })
    
    df = pd.DataFrame(customers)
    
    # Add some duplicates
    num_duplicates = int(len(df) * DUPLICATE_RATE)
    if num_duplicates > 0:
        duplicate_rows = df.sample(n=num_duplicates)
        df = pd.concat([df, duplicate_rows], ignore_index=True)
    
    return df


def generate_orders(customers_df, n=NUM_ORDERS):
    """Generate order data with intentional quality issues."""
    
    orders = []
    customer_ids = customers_df['customer_id'].tolist()
    
    for i in range(1, n + 1):
        order_id = f"ORD{str(i).zfill(8)}"
        customer_id = random.choice(customer_ids)
        
        # Intentionally introduce invalid data
        total_amount = round(random.uniform(10, 500), 2)
        if random.random() < 0.005:  # 0.5% negative amounts
            total_amount = -total_amount
        
        status_options = ['PENDING', 'COMPLETED', 'CANCELLED', 'REFUNDED']
        if random.random() < INVALID_STATUS_RATE:
            order_status = random.choice(['PROCESSING', 'SHIPPED', 'UNKNOWN'])
        else:
            order_status = random.choice(status_options)
        
        orders.append({
            'order_id': order_id,
            'customer_id': customer_id if random.random() > NULL_RATE else None,
            'order_date': fake.date_time_between(start_date='-1y', end_date='now'),
            'order_status': order_status,
            'total_amount': total_amount if random.random() > NULL_RATE else None,
            'payment_method': random.choice(['CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER']),
            'shipping_cost': round(random.uniform(0, 20), 2),
            'discount_amount': round(random.uniform(0, 50), 2) if random.random() > 0.7 else 0,
            '_loaded_at': datetime.now()
        })
    
    df = pd.DataFrame(orders)
    
    # Add duplicates
    num_duplicates = int(len(df) * DUPLICATE_RATE)
    if num_duplicates > 0:
        duplicate_rows = df.sample(n=num_duplicates)
        df = pd.concat([df, duplicate_rows], ignore_index=True)
    
    return df


def generate_events(customers_df, n=NUM_EVENTS):
    """Generate user event data with intentional quality issues."""
    
    events = []
    customer_ids = customers_df['customer_id'].tolist()
    event_types = ['PAGE_VIEW', 'ADD_TO_CART', 'PURCHASE', 'SEARCH', 'CLICK']
    device_types = ['DESKTOP', 'MOBILE', 'TABLET']
    
    for i in range(1, n + 1):
        event_id = f"EVT{str(i).zfill(10)}"
        customer_id = random.choice(customer_ids)
        
        # Introduce invalid event types
        if random.random() < INVALID_STATUS_RATE:
            event_type = random.choice(['LOGIN', 'LOGOUT', 'UNKNOWN'])
        else:
            event_type = random.choice(event_types)
        
        events.append({
            'event_id': event_id,
            'customer_id': customer_id if random.random() > NULL_RATE else None,
            'event_type': event_type,
            'event_timestamp': fake.date_time_between(start_date='-30d', end_date='now'),
            'page_url': f"/page/{random.randint(1, 100)}",
            'product_id': f"PROD{random.randint(1, 500):04d}" if random.random() > 0.3 else None,
            'session_id': f"SESS{random.randint(1, 1000):06d}",
            'device_type': random.choice(device_types),
            '_loaded_at': datetime.now()
        })
    
    df = pd.DataFrame(events)
    
    # Add duplicates
    num_duplicates = int(len(df) * DUPLICATE_RATE)
    if num_duplicates > 0:
        duplicate_rows = df.sample(n=num_duplicates)
        df = pd.concat([df, duplicate_rows], ignore_index=True)
    
    return df


def main():
    """Generate all datasets and save to CSV."""
    
    print("Generating synthetic e-commerce data...")
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Generate customers
    print(f"Generating {NUM_CUSTOMERS} customers...")
    customers_df = generate_customers(NUM_CUSTOMERS)
    customers_df.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
    print(f"  ✓ Saved {len(customers_df)} customer records (includes {int(len(customers_df) * DUPLICATE_RATE)} duplicates)")
    
    # Generate orders
    print(f"Generating {NUM_ORDERS} orders...")
    orders_df = generate_orders(customers_df, NUM_ORDERS)
    orders_df.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)
    print(f"  ✓ Saved {len(orders_df)} order records (includes {int(len(orders_df) * DUPLICATE_RATE)} duplicates)")
    
    # Generate events
    print(f"Generating {NUM_EVENTS} events...")
    events_df = generate_events(customers_df, NUM_EVENTS)
    events_df.to_csv(f"{OUTPUT_DIR}/events.csv", index=False)
    print(f"  ✓ Saved {len(events_df)} event records (includes {int(len(events_df) * DUPLICATE_RATE)} duplicates)")
    
    print("\n" + "="*60)
    print("Data generation complete!")
    print("="*60)
    print(f"\nData quality issues introduced:")
    print(f"  • {NULL_RATE*100}% null values")
    print(f"  • {DUPLICATE_RATE*100}% duplicate records")
    print(f"  • {INVALID_STATUS_RATE*100}% invalid status values")
    print(f"  • 0.5% negative order amounts")
    print(f"\nFiles saved to: {OUTPUT_DIR}/")
    print(f"  - customers.csv ({len(customers_df)} rows)")
    print(f"  - orders.csv ({len(orders_df)} rows)")
    print(f"  - events.csv ({len(events_df)} rows)")


if __name__ == "__main__":
    main()
