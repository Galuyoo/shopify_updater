param(
  [switch]$WhatIf
)

$ErrorActionPreference = "Stop"
$ProjectRoot = "C:\shopify_updater_amazon_work"
$Py = "C:\Users\salah\AppData\Local\Programs\Python\Python311\python.exe"
$TargetFile = "data\storefeeder_supplier_stock_update_targets.csv"
$RunId = Get-Date -Format yyyyMMdd_HHmmss
$OutDir = Join-Path $ProjectRoot "reports\scheduled_fast_stock_sync\$RunId"
$WrapperLog = Join-Path $OutDir "wrapper_run.log"
$PythonStdoutLog = Join-Path $OutDir "python_stdout.log"
$PythonStderrLog = Join-Path $OutDir "python_stderr.log"
$PythonCombinedLog = Join-Path $OutDir "python_combined.log"
$EnvironmentPath = Join-Path $OutDir "wrapper_environment.txt"
$ExitStatusPath = Join-Path $OutDir "wrapper_exit_status.csv"
$SummaryPath = Join-Path $OutDir "fast_stock_summary.csv"
$StartTime = Get-Date
$PythonExitCode = $null
$PowerShellExitCode = 0
$ExceptionText = ""
$FailureStage = ""

function Get-TargetRowCount {
  param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) { return 0 }
  try {
    return @((Import-Csv -LiteralPath $Path)).Count
  } catch {
    return 0
  }
}

function Write-WrapperEnvironment {
  param([string]$Path)
  $branch = ""
  $commit = ""
  try { $branch = (& git branch --show-current 2>$null) } catch { $branch = "" }
  try { $commit = (& git rev-parse HEAD 2>$null) } catch { $commit = "" }
  @(
    "current_directory=$(Get-Location)",
    "project_root=$ProjectRoot",
    "python_path=$Py",
    "git_branch=$branch",
    "git_commit=$commit",
    "target_file=$TargetFile",
    "target_rows_before_run=$(Get-TargetRowCount -Path $TargetFile)",
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
  $pyCodeText = if ($null -eq $PythonCode) { "" } else { [string]$PythonCode }
  $lastTaskHex = if ($PowerShellCode -lt 0) { "0x{0:X8}" -f ([uint32]$PowerShellCode) } else { "0x{0:X8}" -f $PowerShellCode }
  [pscustomobject]@{
    start_timestamp = $StartTime.ToString('o')
    end_timestamp = $EndTime.ToString('o')
    powershell_exit_code = $PowerShellCode
    powershell_exit_code_hex = $lastTaskHex
    python_process_exit_code = $pyCodeText
    failure_stage = $Stage
    exception_text = $Exception
    target_rows = Get-TargetRowCount -Path $TargetFile
    stdout_log_path = $StdoutPath
    stderr_log_path = $StderrPath
    combined_log_path = $CombinedPath
  } | Export-Csv -LiteralPath $Path -NoTypeInformation
}

function Write-FailureSummary {
  param([string]$Path, [string]$Stage)
  if (Test-Path -LiteralPath $Path) { return }
  @(
    [pscustomobject]@{ metric = "dry_run"; value = "no" },
    [pscustomobject]@{ metric = "wrapper_failed"; value = "yes" },
    [pscustomobject]@{ metric = "failure_stage"; value = $Stage },
    [pscustomobject]@{ metric = "target_rows"; value = Get-TargetRowCount -Path $TargetFile },
    [pscustomobject]@{ metric = "live_stock_update"; value = "no" }
  ) | Export-Csv -LiteralPath $Path -NoTypeInformation
}

function Write-CombinedPythonLog {
  param(
    [string]$StdoutPath,
    [string]$StderrPath,
    [string]$CombinedPath
  )
  @(
    "===== STDOUT =====",
    $(if (Test-Path -LiteralPath $StdoutPath) { Get-Content -LiteralPath $StdoutPath -Raw } else { "" }),
    "===== STDERR =====",
    $(if (Test-Path -LiteralPath $StderrPath) { Get-Content -LiteralPath $StderrPath -Raw } else { "" })
  ) | Set-Content -LiteralPath $CombinedPath -Encoding UTF8
}

try {
  Set-Location $ProjectRoot
  New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
  "" | Set-Content -LiteralPath $PythonStdoutLog -Encoding UTF8
  "" | Set-Content -LiteralPath $PythonStderrLog -Encoding UTF8
  "" | Set-Content -LiteralPath $PythonCombinedLog -Encoding UTF8
  Write-WrapperEnvironment -Path $EnvironmentPath
  "Fast stock sync wrapper started $($StartTime.ToString('o'))" | Tee-Object -FilePath $WrapperLog

  if ($WhatIf) {
    "WhatIf mode: wrapper syntax/environment check only. Python not started." | Tee-Object -FilePath $WrapperLog -Append
    $PowerShellExitCode = 0
    return
  }

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

  $command = @(
    "scripts\run_supplier_stock_fast_update.py",
    "--targets", $TargetFile,
    "--out-dir", $OutDir,
    "--live-stock-update",
    "--api-limit", "2500",
    "--buffer", "0",
    "--max-stock", "999999",
    "--zero-other-locations-for-supplier-synced",
    "--scheduled-run"
  )
  "Running: $Py $($command -join ' ')" | Tee-Object -FilePath $WrapperLog -Append
  & $Py @command 1> $PythonStdoutLog 2> $PythonStderrLog
  $PythonExitCode = $LASTEXITCODE
  Write-CombinedPythonLog -StdoutPath $PythonStdoutLog -StderrPath $PythonStderrLog -CombinedPath $PythonCombinedLog
  Get-Content -LiteralPath $PythonCombinedLog | Tee-Object -FilePath $WrapperLog -Append
  if ($PythonExitCode -ne 0) {
    $FailureStage = "python_start_or_runtime"
    $PowerShellExitCode = $PythonExitCode
    Write-FailureSummary -Path $SummaryPath -Stage $FailureStage
  }
} catch {
  $ExceptionText = $_.Exception.ToString()
  $FailureStage = if ($FailureStage) { $FailureStage } else { "wrapper_exception" }
  $PowerShellExitCode = 1
  $ExceptionText | Tee-Object -FilePath $WrapperLog -Append
  Write-FailureSummary -Path $SummaryPath -Stage $FailureStage
} finally {
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
  "Fast stock sync wrapper finished $($EndTime.ToString('o')) with exit $PowerShellExitCode" | Tee-Object -FilePath $WrapperLog -Append
}

exit $PowerShellExitCode
