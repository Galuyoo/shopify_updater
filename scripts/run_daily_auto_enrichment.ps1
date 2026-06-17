$ErrorActionPreference = "Stop"

Set-Location "C:\shopify_updater_amazon_work"

$Py = "C:\Users\salah\AppData\Local\Programs\Python\Python311\python.exe"

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
& $Py scripts\refresh_supplier_stock_files.py `
  --ralawise-out data\RALAWISE_stock_lvl.csv `
  --uneek-out data\Uneek_stock_levels.csv *>> $LogPath

$ExitCode = $LASTEXITCODE
if ($ExitCode -ne 0) {
  Get-Content $LogPath
  exit $ExitCode
}

"== enrichment change gate ==" *>> $LogPath
& $Py scripts\enrichment_change_gate.py *>> $LogPath

$GateExitCode = $LASTEXITCODE

if ($GateExitCode -eq 0) {
  "No enrichment-relevant change detected. Skipping expensive enrichment/promoter stage." *>> $LogPath
  Get-Content $LogPath
  exit 0
}

if ($GateExitCode -ne 2) {
  "Change gate failed with exit code $GateExitCode" *>> $LogPath
  Get-Content $LogPath
  exit $GateExitCode
}

"Change detected. Running full enrichment pipeline." *>> $LogPath

"== auto enrichment report ==" *>> $LogPath
& $Py scripts\auto_enrich_new_storefeeder_products.py `
  --use-api `
  --out-root reports\auto_enrich_new_products *>> $LogPath

$ExitCode = $LASTEXITCODE
if ($ExitCode -ne 0) {
  Get-Content $LogPath
  exit $ExitCode
}

"== generic exact SKU supplier promoter ==" *>> $LogPath
& $Py scripts\promote_exact_supplier_matches.py `
  --execute `
  --create-missing-product-suppliers `
  --out-root reports\exact_supplier_promoter *>> $LogPath

$ExitCode = $LASTEXITCODE
if ($ExitCode -ne 0) {
  Get-Content $LogPath
  exit $ExitCode
}

"== update enrichment change gate state ==" *>> $LogPath
& $Py scripts\enrichment_change_gate.py --update-state *>> $LogPath

$StateExitCode = $LASTEXITCODE
if (($StateExitCode -ne 0) -and ($StateExitCode -ne 2)) {
  Get-Content $LogPath
  exit $StateExitCode
}

Get-Content $LogPath
exit 0
