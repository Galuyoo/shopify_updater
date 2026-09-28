param(
  [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$ProjectRoot = "C:\shopify_updater_amazon_work"
$Py = "C:\Users\salah\AppData\Local\Programs\Python\Python311\python.exe"

$CatalogueFile = "reports\cleaned_catalogue\cleaned_storefeeder_catalogue.csv"
$TargetFile = "data\storefeeder_supplier_stock_update_targets.csv"
$RalawiseStockFile = "data\RALAWISE_stock_lvl.csv"
$UneekStockFile = "data\Uneek_stock_levels.csv"

$RunId = Get-Date -Format yyyyMMdd_HHmmss
$OutDir = Join-Path $ProjectRoot "reports\scheduled_fast_stock_sync\$RunId"

$WrapperLog = Join-Path $OutDir "wrapper_run.log"
$PythonStdoutLog = Join-Path $OutDir "python_stdout.log"
$PythonStderrLog = Join-Path $OutDir "python_stderr.log"
$PythonCombinedLog = Join-Path $OutDir "python_combined.log"
$EnvironmentPath = Join-Path $OutDir "wrapper_environment.txt"
$ExitStatusPath = Join-Path $OutDir "wrapper_exit_status.csv"
$SummaryPath = Join-Path $OutDir "fast_stock_summary.csv"
$SharedLockPath = Join-Path $ProjectRoot "reports\storefeeder_stock_sync.lock"

$StartTime = Get-Date
$PythonExitCode = $null
$PowerShellExitCode = 0
$ExceptionText = ""
$FailureStage = ""
$LockAcquired = $false

function Get-TargetRowCount {
  param([string]$Path)

  if (-not (Test-Path -LiteralPath $Path)) {
    return 0
  }

  try {
    return @((Import-Csv -LiteralPath $Path)).Count
  }
  catch {
    return 0
  }
}

function Write-WrapperEnvironment {
  param([string]$Path)

  $branch = ""
  $commit = ""

  try {
    $branch = (& git branch --show-current 2>$null)
  }
  catch {
    $branch = ""
  }

  try {
    $commit = (& git rev-parse HEAD 2>$null)
  }
  catch {
    $commit = ""
  }

  @(
    "current_directory=$(Get-Location)"
    "project_root=$ProjectRoot"
    "python_path=$Py"
    "git_branch=$branch"
    "git_commit=$commit"
    "catalogue_file=$CatalogueFile"
    "target_file=$TargetFile"
    "ralawise_stock_file=$RalawiseStockFile"
    "uneek_stock_file=$UneekStockFile"
    "target_rows_before_run=$(Get-TargetRowCount -Path $TargetFile)"
    "start_timestamp=$($StartTime.ToString('o'))"
  ) | Set-Content -LiteralPath $Path -Encoding UTF8
}

function Write-ExitStatus {
  param(
    [string]$Path,
    [int]$PowerShellCode,
    [object]$PythonCode,
    [string]$Exception,
    [string]$Stage,
    [datetime]$EndTime,
    [string]$StdoutPath,
    [string]$StderrPath,
    [string]$CombinedPath
  )

  $pyCodeText = if ($null -eq $PythonCode) {
    ""
  }
  else {
    [string]$PythonCode
  }

  $lastTaskHex = if ($PowerShellCode -lt 0) {
    "0x{0:X8}" -f ([uint32]$PowerShellCode)
  }
  else {
    "0x{0:X8}" -f $PowerShellCode
  }

  [pscustomobject]@{
    start_timestamp          = $StartTime.ToString('o')
    end_timestamp            = $EndTime.ToString('o')
    powershell_exit_code     = $PowerShellCode
    powershell_exit_code_hex = $lastTaskHex
    python_process_exit_code = $pyCodeText
    failure_stage            = $Stage
    exception_text           = $Exception
    target_rows              = Get-TargetRowCount -Path $TargetFile
    stdout_log_path          = $StdoutPath
    stderr_log_path          = $StderrPath
    combined_log_path        = $CombinedPath
    lock_path                = $SharedLockPath
    lock_acquired            = $LockAcquired
  } | Export-Csv -LiteralPath $Path -NoTypeInformation
}

function Write-FailureSummary {
  param(
    [string]$Path,
    [string]$Stage
  )

  if (Test-Path -LiteralPath $Path) {
    return
  }

  @(
    [pscustomobject]@{
      metric = "dry_run"
      value  = "no"
    }
    [pscustomobject]@{
      metric = "wrapper_failed"
      value  = "yes"
    }
    [pscustomobject]@{
      metric = "failure_stage"
      value  = $Stage
    }
    [pscustomobject]@{
      metric = "target_rows"
      value  = Get-TargetRowCount -Path $TargetFile
    }
    [pscustomobject]@{
      metric = "live_stock_update"
      value  = "no"
    }
  ) | Export-Csv -LiteralPath $Path -NoTypeInformation
}

function Write-CombinedPythonLog {
  param(
    [string]$StdoutPath,
    [string]$StderrPath,
    [string]$CombinedPath
  )

  @(
    "===== STDOUT ====="
    $(if (Test-Path -LiteralPath $StdoutPath) {
        Get-Content -LiteralPath $StdoutPath -Raw
      }
      else {
        ""
      })
    "===== STDERR ====="
    $(if (Test-Path -LiteralPath $StderrPath) {
        Get-Content -LiteralPath $StderrPath -Raw
      }
      else {
        ""
      })
  ) | Set-Content -LiteralPath $CombinedPath -Encoding UTF8
}

try {
  Set-Location $ProjectRoot

  New-Item `
    -ItemType Directory `
    -Force `
    -Path $OutDir |
    Out-Null

  "" | Set-Content -LiteralPath $PythonStdoutLog -Encoding UTF8
  "" | Set-Content -LiteralPath $PythonStderrLog -Encoding UTF8
  "" | Set-Content -LiteralPath $PythonCombinedLog -Encoding UTF8

  Write-WrapperEnvironment -Path $EnvironmentPath

  "Fast stock sync wrapper started $($StartTime.ToString('o'))" |
    Tee-Object -FilePath $WrapperLog

  if ($WhatIf) {
    "WhatIf mode: wrapper syntax and environment check only. Python was not started." |
      Tee-Object -FilePath $WrapperLog -Append

    $PowerShellExitCode = 0
    return
  }

  if (-not (Test-Path -LiteralPath $Py)) {
    throw "Python executable not found: $Py"
  }

  if (-not (Test-Path -LiteralPath $CatalogueFile)) {
    throw "Catalogue file not found: $CatalogueFile"
  }

  if (-not (Test-Path -LiteralPath $TargetFile)) {
    throw "Target file not found: $TargetFile"
  }

  if (Test-Path -LiteralPath $SharedLockPath) {
    $lockAge = (Get-Date) - (Get-Item -LiteralPath $SharedLockPath).LastWriteTime
    if ($lockAge.TotalHours -lt 6) {
      "Another StoreFeeder stock sync appears to be running. This fast cycle was skipped." |
        Tee-Object -FilePath $WrapperLog -Append
      return
    }
    "Removing stale stock-sync lock older than 6 hours." |
      Tee-Object -FilePath $WrapperLog -Append
    Remove-Item -LiteralPath $SharedLockPath -Force
  }

  [pscustomobject]@{
    process_id = $PID
    started_at = $StartTime.ToString("o")
    job_type   = "fast_supplier_sync"
  } | Export-Csv -LiteralPath $SharedLockPath -NoTypeInformation
  $LockAcquired = $true

  if (Test-Path -LiteralPath ".env") {
    Get-Content -LiteralPath ".env" | ForEach-Object {
      $line = $_.Trim()

      if (
        $line -and
        -not $line.StartsWith("#") -and
        $line.Contains("=")
      ) {
        $k, $v = $line.Split("=", 2)
        $v = $v.Trim().Trim('"').Trim("'")

        [Environment]::SetEnvironmentVariable(
          $k.Trim(),
          $v,
          "Process"
        )
      }
    }
  }

  $command = @(
    "scripts\run_supplier_stock_fast_update.py"
    "--catalogue"
    $CatalogueFile
    "--targets"
    $TargetFile
    "--ralawise-stock"
    $RalawiseStockFile
    "--uneek-stock"
    $UneekStockFile
    "--out-dir"
    $OutDir
    "--missing-as-zero"
    "--api-limit"
    "100000"
    "--buffer"
    "0"
    "--max-stock"
    "999999"
    "--live-stock-update"
    "--scheduled-run"
  )

  "Running: $Py $($command -join ' ')" |
    Tee-Object -FilePath $WrapperLog -Append

  & $Py @command 1> $PythonStdoutLog 2> $PythonStderrLog

  $PythonExitCode = $LASTEXITCODE

  Write-CombinedPythonLog `
    -StdoutPath $PythonStdoutLog `
    -StderrPath $PythonStderrLog `
    -CombinedPath $PythonCombinedLog

  Get-Content -LiteralPath $PythonCombinedLog |
    Tee-Object -FilePath $WrapperLog -Append

  if ($PythonExitCode -ne 0) {
    $FailureStage = "python_start_or_runtime"
    $PowerShellExitCode = $PythonExitCode

    Write-FailureSummary `
      -Path $SummaryPath `
      -Stage $FailureStage
  }
}
catch {
  $ExceptionText = $_.Exception.ToString()

  $FailureStage = if ($FailureStage) {
    $FailureStage
  }
  else {
    "wrapper_exception"
  }

  $PowerShellExitCode = 1

  $ExceptionText |
    Tee-Object -FilePath $WrapperLog -Append

  Write-FailureSummary `
    -Path $SummaryPath `
    -Stage $FailureStage
}
finally {
  if ($LockAcquired -and (Test-Path -LiteralPath $SharedLockPath)) {
    $lock = Import-Csv -LiteralPath $SharedLockPath -ErrorAction SilentlyContinue
    if ($lock.process_id -eq [string]$PID) {
      Remove-Item -LiteralPath $SharedLockPath -Force
    }
  }

  $EndTime = Get-Date

  Write-ExitStatus `
    -Path $ExitStatusPath `
    -PowerShellCode $PowerShellExitCode `
    -PythonCode $PythonExitCode `
    -Exception $ExceptionText `
    -Stage $FailureStage `
    -EndTime $EndTime `
    -StdoutPath $PythonStdoutLog `
    -StderrPath $PythonStderrLog `
    -CombinedPath $PythonCombinedLog

  try {
    & $Py `
      "scripts\write_stock_sync_metric.py" `
      "--run-dir" $OutDir `
      "--job-type" "fast_supplier_sync" `
      2>> $WrapperLog

    if ($LASTEXITCODE -ne 0) {
      "Metrics writer failed with exit $LASTEXITCODE; stock-sync result is unchanged." |
        Tee-Object -FilePath $WrapperLog -Append
    }
  }
  catch {
    "Metrics writer failed: $($_.Exception.Message); stock-sync result is unchanged." |
      Tee-Object -FilePath $WrapperLog -Append
  }

  "Fast stock sync wrapper finished $($EndTime.ToString('o')) with exit $PowerShellExitCode" |
    Tee-Object -FilePath $WrapperLog -Append
}

exit $PowerShellExitCode
