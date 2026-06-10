$ErrorActionPreference = "Stop"

$ProjectRoot = "C:\shopify_updater_amazon_work"
$Python = "C:\Users\salah\AppData\Local\Programs\Python\Python311\python.exe"
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$OutDir = Join-Path $ProjectRoot "reports\fast_stock_update_scheduled\$Timestamp"
$LogPath = Join-Path $OutDir "run.log"
$LockPath = Join-Path $ProjectRoot "reports\fast_stock_update.lock"

if (Test-Path -LiteralPath $LockPath) {
    Write-Host "Stock sync already running. Skipping this run."
    exit 0
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $LockPath) | Out-Null

$exitCode = 1

try {
    New-Item -ItemType File -Path $LockPath -Force | Out-Null
    Push-Location $ProjectRoot

    $ArgsList = @(
        "scripts\run_supplier_stock_fast_update.py",
        "--live-stock-update",
        "--api-limit", "3000",
        "--buffer", "0",
        "--max-stock", "999999",
        "--targets", "data\storefeeder_supplier_stock_update_targets.csv",
        "--out-dir", $OutDir
    )

    Write-Host "Starting StoreFeeder fast stock sync: $Timestamp"
    Write-Host "Output folder: $OutDir"
    Write-Host "Log file: $LogPath"

    & $Python @ArgsList 2>&1 | Tee-Object -FilePath $LogPath
    $exitCode = $LASTEXITCODE
}
finally {
    try {
        Pop-Location
    }
    catch {
    }
    if (Test-Path -LiteralPath $LockPath) {
        Remove-Item -LiteralPath $LockPath -Force
    }
}

exit $exitCode
