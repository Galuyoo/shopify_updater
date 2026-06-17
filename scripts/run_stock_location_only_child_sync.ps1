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
$OutDir = "reports\stock_location_only_child_sync\$RunId"
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

python scripts\refresh_supplier_stock_files.py `
  --ralawise-out data\RALAWISE_stock_lvl.csv `
  --uneek-out data\Uneek_stock_levels.csv

python scripts\run_stock_location_only_child_sync.py `
  --rules data\stock_location_only_parent_rules.csv `
  --out-dir $OutDir

exit $LASTEXITCODE
