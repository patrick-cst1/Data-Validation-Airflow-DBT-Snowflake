#!/bin/bash

# Setup script for E-Commerce Data Quality Pipeline
# Author: Patrick Cheung
# Date: October 2025

set -e  # Exit on error

echo "=================================================="
echo "E-Commerce Data Quality Pipeline - Setup"
echo "=================================================="
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please edit .env file with your Snowflake credentials before continuing."
    echo "   Required fields:"
    echo "   - SNOWFLAKE_ACCOUNT"
    echo "   - SNOWFLAKE_USER"
    echo "   - SNOWFLAKE_PASSWORD"
    echo ""
    read -p "Press enter after updating .env file..."
fi

# Load environment variables
source .env

echo "Step 1: Generating sample data..."
python scripts/generate_sample_data.py

echo ""
echo "Step 2: Initializing Snowflake..."
python scripts/init_snowflake.py

echo ""
echo "Step 3: Setting up Great Expectations..."
python scripts/setup_great_expectations.py

echo ""
echo "Step 4: Building Docker images..."
docker-compose build

echo ""
echo "Step 5: Starting services..."
docker-compose up -d

echo ""
echo "=================================================="
echo "Setup Complete!"
echo "=================================================="
echo ""
echo "Services:"
echo "  • Airflow UI: http://localhost:8080"
echo "    Username: admin"
echo "    Password: admin"
echo ""
echo "  • PostgreSQL: localhost:5432"
echo ""
echo "Next steps:"
echo "  1. Wait ~30 seconds for Airflow to initialize"
echo "  2. Open http://localhost:8080 in your browser"
echo "  3. Enable the 'ecommerce_data_quality_pipeline' DAG"
echo "  4. Trigger the DAG manually or wait for scheduled run"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f"
echo ""
echo "To stop services:"
echo "  docker-compose down"
echo ""
