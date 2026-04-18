param(
    [string]$PythonExe = "python",
    [string]$SparkSubmit = "spark-submit"
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot

Write-Host "Creating output directories..."
New-Item -ItemType Directory -Force -Path (Join-Path $projectRoot "output\\parquet") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $projectRoot "output\\exports") | Out-Null

if (Get-Command $SparkSubmit -ErrorAction SilentlyContinue) {
    $runner = $SparkSubmit
} else {
    $runner = $PythonExe
}

Write-Host "Running CSV to Parquet..."
& $runner (Join-Path $projectRoot "jobs\\csv_to_parquet.py")

Write-Host "Running cleansing..."
& $runner (Join-Path $projectRoot "jobs\\cleanse_data.py")

Write-Host "Running indicators..."
& $runner (Join-Path $projectRoot "jobs\\calculate_indicators.py")

Write-Host "Running portfolio selection..."
& $runner (Join-Path $projectRoot "jobs\\select_portfolio.py")

Write-Host "Running export..."
& $runner (Join-Path $projectRoot "jobs\\export_portfolio.py")

Write-Host "Building market dashboard data..."
& $PythonExe (Join-Path $projectRoot "jobs\\build_market_dashboard_data.py")

Write-Host "Building investment insights..."
& $PythonExe (Join-Path $projectRoot "jobs\\build_investment_insights.py")

Write-Host "Pipeline completed."
