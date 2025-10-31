# Setup script for E-Commerce Data Quality Pipeline
# Author: Patrick Cheung
# Date: October 2025

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "E-Commerce Data Quality Pipeline - Setup" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

# Check if .env exists
if (-not (Test-Path .env)) {
    Write-Host "Creating .env file from template..." -ForegroundColor Yellow
    Copy-Item .env.example .env
    Write-Host "⚠️  Please edit .env file with your Snowflake credentials before continuing." -ForegroundColor Yellow
    Write-Host "   Required fields:" -ForegroundColor Yellow
    Write-Host "   - SNOWFLAKE_ACCOUNT" -ForegroundColor Yellow
    Write-Host "   - SNOWFLAKE_USER" -ForegroundColor Yellow
    Write-Host "   - SNOWFLAKE_PASSWORD" -ForegroundColor Yellow
    Write-Host ""
    Read-Host "Press enter after updating .env file"
}

Write-Host "Step 1: Generating sample data..." -ForegroundColor Green
python scripts/generate_sample_data.py

Write-Host ""
Write-Host "Step 2: Initializing Snowflake..." -ForegroundColor Green
python scripts/init_snowflake.py

Write-Host ""
Write-Host "Step 3: Setting up Great Expectations..." -ForegroundColor Green
python scripts/setup_great_expectations.py

Write-Host ""
Write-Host "Step 4: Building Docker images..." -ForegroundColor Green
docker-compose build

Write-Host ""
Write-Host "Step 5: Starting services..." -ForegroundColor Green
docker-compose up -d

Write-Host ""
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "Setup Complete!" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Services:" -ForegroundColor Green
Write-Host "  • Airflow UI: http://localhost:8080"
Write-Host "    Username: admin"
Write-Host "    Password: admin"
Write-Host ""
Write-Host "  • PostgreSQL: localhost:5432"
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Wait ~30 seconds for Airflow to initialize"
Write-Host "  2. Open http://localhost:8080 in your browser"
Write-Host "  3. Enable the 'ecommerce_data_quality_pipeline' DAG"
Write-Host "  4. Trigger the DAG manually or wait for scheduled run"
Write-Host ""
Write-Host "To view logs:" -ForegroundColor Cyan
Write-Host "  docker-compose logs -f"
Write-Host ""
Write-Host "To stop services:" -ForegroundColor Cyan
Write-Host "  docker-compose down"
Write-Host ""
