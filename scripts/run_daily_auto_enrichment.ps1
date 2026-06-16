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

python scripts\auto_enrich_new_storefeeder_products.py `
  --use-api `
  --out-root reports\auto_enrich_new_products *> $LogPath

$ExitCode = $LASTEXITCODE

Get-Content $LogPath

exit $ExitCode
