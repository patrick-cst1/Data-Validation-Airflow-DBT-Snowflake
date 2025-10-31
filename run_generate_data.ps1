# Navigate to project directory
Set-Location "C:\Users\B712MU\Git\Data-Validation-Airflow-DBT-Snowflake"

# Run the Python script
& "C:/Program Files/Python313/python.exe" "scripts\generate_sample_data.py"

# List the generated files
Write-Host "`nGenerated files:"
Get-ChildItem "data\raw\*.csv" | ForEach-Object {
    $lines = (Get-Content $_.FullName | Measure-Object -Line).Lines
    Write-Host "  $($_.Name): $lines lines"
}
