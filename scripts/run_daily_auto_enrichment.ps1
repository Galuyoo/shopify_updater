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

$LogDir = "reports\auto_enrich_new_products_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$RunId = Get-Date -Format yyyyMMdd_HHmmss
$LogPath = "$LogDir\$RunId.log"

"== refresh supplier stock files ==" *> $LogPath
python scripts\refresh_supplier_stock_files.py `
  --ralawise-out data\RALAWISE_stock_lvl.csv `
  --uneek-out data\Uneek_stock_levels.csv *>> $LogPath

$ExitCode = $LASTEXITCODE
if ($ExitCode -ne 0) {
  Get-Content $LogPath
  exit $ExitCode
}

"== auto enrichment report ==" *>> $LogPath
python scripts\auto_enrich_new_storefeeder_products.py `
  --use-api `
  --out-root reports\auto_enrich_new_products *>> $LogPath

$ExitCode = $LASTEXITCODE
if ($ExitCode -ne 0) {
  Get-Content $LogPath
  exit $ExitCode
}

"== generic exact SKU supplier promoter ==" *>> $LogPath
python scripts\promote_exact_supplier_matches.py `
  --execute `
  --create-missing-product-suppliers `
  --out-root reports\exact_supplier_promoter *>> $LogPath

$ExitCode = $LASTEXITCODE

Get-Content $LogPath

exit $ExitCode
