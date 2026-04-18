param(
    [switch]$Detached
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $projectRoot
try {
    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        $compose = "docker-compose"
    } elseif (Get-Command docker -ErrorAction SilentlyContinue) {
        $compose = "docker compose"
    } else {
        throw "Docker Compose was not found. Install Docker Desktop or docker-compose first."
    }

    Write-Host "Starting dashboard with Docker..."
    if ($compose -eq "docker-compose") {
        if ($Detached) {
            & docker-compose up -d dashboard
        } else {
            & docker-compose up dashboard
        }
    } else {
        if ($Detached) {
            & docker compose up -d dashboard
        } else {
            & docker compose up dashboard
        }
    }
}
finally {
    Pop-Location
}
