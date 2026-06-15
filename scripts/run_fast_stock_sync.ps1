$ErrorActionPreference = "Stop"

Set-Location "C:\shopify_updater_amazon_work"

# Load .env for scheduled task context
if (Test-Path ".env") {
  Get-Content ".env" | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
      $k, $v = $line.Split("=", 2)
      $v = $v.Trim().Trim('"').Trim("'")
      [Environment]::SetEnvironmentVariable($k.Trim(), $v, "Process")
    }
  }
}

$RunId = Get-Date -Format yyyyMMdd_HHmmss
$OutDir = "reports\scheduled_fast_stock_sync\$RunId"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

python scripts\run_supplier_stock_fast_update.py `
  --targets data\storefeeder_supplier_stock_update_targets.csv `
  --out-dir $OutDir `
  --live-stock-update `
  --api-limit 2500

exit $LASTEXITCODE
