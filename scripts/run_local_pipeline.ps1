param(
    [string]$PythonExe = "python",
    [string]$SparkSubmit = ""
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot

function Resolve-CommandPath {
    param([string]$CommandName)

    $command = Get-Command $CommandName -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }
    return $null
}

function Invoke-Step {
    param(
        [string]$Label,
        [string]$CommandPath,
        [string[]]$Arguments
    )

    Write-Host $Label
    & $CommandPath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE"
    }
}

Write-Host "Creating output directories..."
New-Item -ItemType Directory -Force -Path (Join-Path $projectRoot "output\\parquet") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $projectRoot "output\\exports") | Out-Null

$pythonCommand = Resolve-CommandPath $PythonExe
if (-not $pythonCommand) {
    throw "Python executable not found: $PythonExe"
}

$sparkCommand = $null
if ($SparkSubmit) {
    $sparkCommand = Resolve-CommandPath $SparkSubmit
}

if ($sparkCommand) {
    $runner = $sparkCommand
} else {
    $runner = $pythonCommand
}

Invoke-Step "Running CSV to Parquet..." $runner @((Join-Path $projectRoot "jobs\\csv_to_parquet.py"))
Invoke-Step "Running cleansing..." $runner @((Join-Path $projectRoot "jobs\\cleanse_data.py"))
Invoke-Step "Running indicators..." $runner @((Join-Path $projectRoot "jobs\\calculate_indicators.py"))
Invoke-Step "Running portfolio selection..." $runner @((Join-Path $projectRoot "jobs\\select_portfolio.py"))
Invoke-Step "Running export..." $runner @((Join-Path $projectRoot "jobs\\export_portfolio.py"))
Invoke-Step "Building market dashboard data..." $pythonCommand @((Join-Path $projectRoot "jobs\\build_market_dashboard_data.py"), "--mark-airflow-run")
Invoke-Step "Building investment insights..." $pythonCommand @((Join-Path $projectRoot "jobs\\build_investment_insights.py"), "--mark-airflow-run")

Write-Host "Pipeline completed."
