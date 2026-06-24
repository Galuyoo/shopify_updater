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
$FastSyncTriggerLog = "$LogDir\${RunId}_fast_sync_trigger.log"
$StrictFastSync = ($env:STRICT_FAST_SYNC_AFTER_ENRICHMENT -eq "true")
$RunFastSyncAfterEnrichment = if ($env:RUN_FAST_SYNC_AFTER_ENRICHMENT) { $env:RUN_FAST_SYNC_AFTER_ENRICHMENT -eq "true" } else { $true }

function Get-LatestOnboardingSummary {
  $summary = Get-ChildItem -Path "reports\new_product_onboarding_delta" -Recurse -Filter "onboarding_summary.csv" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
  return $summary
}

function Get-MetricValue {
  param([string]$Path, [string]$Metric)
  if (-not (Test-Path -LiteralPath $Path)) { return 0 }
  try {
    $row = Import-Csv -LiteralPath $Path | Where-Object { $_.metric -eq $Metric } | Select-Object -First 1
    if ($null -eq $row) { return 0 }
    return [int]([double]$row.value)
  } catch {
    return 0
  }
}

"== refresh supplier stock files ==" *> $LogPath
& $Py scripts\refresh_supplier_stock_files.py `
  --ralawise-out data\RALAWISE_stock_lvl.csv `
  --uneek-out data\Uneek_stock_levels.csv *>> $LogPath

$ExitCode = $LASTEXITCODE
if ($ExitCode -ne 0) {
  Get-Content $LogPath
  exit $ExitCode
}

"== new product onboarding delta ==" *>> $LogPath
& $Py scripts\run_new_product_onboarding_delta.py `
  --execute `
  --create-missing-product-suppliers *>> $LogPath

$OnboardingExitCode = $LASTEXITCODE
$FastSyncExitCode = 0
$FastSyncTriggered = $false
$FastSyncAfterEnrichmentFailed = $false

if ($OnboardingExitCode -eq 0 -and $RunFastSyncAfterEnrichment) {
  $summary = Get-LatestOnboardingSummary
  if ($summary) {
    $targetRowsAppended = Get-MetricValue -Path $summary.FullName -Metric "target_rows_appended"
    $supplierInfoOnlyRowsAppended = Get-MetricValue -Path $summary.FullName -Metric "supplier_info_only_targets_appended"
    if ($targetRowsAppended -gt 0 -or $supplierInfoOnlyRowsAppended -gt 0) {
      $FastSyncTriggered = $true
      "Triggering fast stock sync because onboarding appended target rows. summary=$($summary.FullName) target_rows_appended=$targetRowsAppended supplier_info_only_targets_appended=$supplierInfoOnlyRowsAppended" *> $FastSyncTriggerLog
      powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\run_fast_stock_sync.ps1 *>> $FastSyncTriggerLog
      $FastSyncExitCode = $LASTEXITCODE
      if ($FastSyncExitCode -ne 0) {
        $FastSyncAfterEnrichmentFailed = $true
        "fast_sync_after_enrichment_failed=yes exit_code=$FastSyncExitCode" *>> $FastSyncTriggerLog
      }
    }
  }
}

"enrichment_success=$($OnboardingExitCode -eq 0)" *>> $LogPath
"fast_sync_triggered=$FastSyncTriggered" *>> $LogPath
"fast_sync_after_enrichment_failed=$FastSyncAfterEnrichmentFailed" *>> $LogPath
"fast_sync_exit_code=$FastSyncExitCode" *>> $LogPath

Get-Content $LogPath
if (Test-Path $FastSyncTriggerLog) { Get-Content $FastSyncTriggerLog }

if ($OnboardingExitCode -ne 0) { exit $OnboardingExitCode }
if ($FastSyncAfterEnrichmentFailed -and $StrictFastSync) { exit $FastSyncExitCode }
exit 0
