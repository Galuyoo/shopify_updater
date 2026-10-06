param(
    [switch]$WhatIf,
    [string]$ResumeRunDir = ""
)

$ErrorActionPreference = "Stop"

$ProjectRoot = "C:\shopify_updater_amazon_work"
$Python = "C:\Users\salah\AppData\Local\Programs\Python\Python311\python.exe"
$TargetFile = "data\storefeeder_supplier_stock_update_targets.csv"
$RunId = Get-Date -Format "yyyyMMdd_HHmmss"
$OutDir = if ($ResumeRunDir) {
    if ([System.IO.Path]::IsPathRooted($ResumeRunDir)) {
        $ResumeRunDir
    }
    else {
        Join-Path $ProjectRoot $ResumeRunDir
    }
}
else {
    Join-Path $ProjectRoot "reports\scheduled_daily_inventory_mirror\$RunId"
}
$WrapperLog = Join-Path $OutDir "wrapper_run.log"
$StdoutLog = Join-Path $OutDir "python_stdout.log"
$StderrLog = Join-Path $OutDir "python_stderr.log"
$ExitStatusPath = Join-Path $OutDir "wrapper_exit_status.csv"
$SharedLockPath = Join-Path $ProjectRoot "reports\storefeeder_stock_sync.lock"
$LockWaitSeconds = 120

$StartTime = Get-Date
$PythonExitCode = $null
$PowerShellExitCode = 0
$FailureStage = ""
$ExceptionText = ""
$LockAcquired = $false

function Write-ExitStatus {
    param([datetime]$EndTime)

    [pscustomobject]@{
        start_timestamp      = $StartTime.ToString("o")
        end_timestamp        = $EndTime.ToString("o")
        powershell_exit_code = $PowerShellExitCode
        python_exit_code     = if ($null -eq $PythonExitCode) { "" } else { $PythonExitCode }
        failure_stage        = $FailureStage
        exception_text       = $ExceptionText
        target_file          = $TargetFile
        mirror_policy        = "global_supplier_managed_products"
        output_directory     = $OutDir
        lock_path            = $SharedLockPath
        lock_acquired        = $LockAcquired
    } | Export-Csv -LiteralPath $ExitStatusPath -NoTypeInformation
}

function Wait-ForSyncLock {
    $deadline = (Get-Date).AddSeconds($LockWaitSeconds)
    while (Test-Path -LiteralPath $SharedLockPath) {
        $lockAge = (Get-Date) - (Get-Item -LiteralPath $SharedLockPath).LastWriteTime
        if ($lockAge.TotalHours -ge 6) {
            "Removing stale stock-sync lock older than 6 hours." |
                Tee-Object -FilePath $WrapperLog -Append |
                Out-Null
            Remove-Item -LiteralPath $SharedLockPath -Force
            return $true
        }
        if ((Get-Date) -ge $deadline) {
            return $false
        }
        "Waiting for the current StoreFeeder sync to finish." |
            Tee-Object -FilePath $WrapperLog -Append |
            Out-Null
        Start-Sleep -Seconds 30
    }
    return $true
}

try {
    Set-Location $ProjectRoot
    New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
    "Daily inventory mirror reconciliation started: $($StartTime.ToString('o'))" |
        Tee-Object -FilePath $WrapperLog

    if ($WhatIf) {
        "WhatIf mode: Python was not started." |
            Tee-Object -FilePath $WrapperLog -Append
        return
    }

    $lockAvailable = Wait-ForSyncLock
    if (-not $lockAvailable) {
        $FailureStage = "SKIPPED_LOCK_HELD"
        "Daily inventory mirror skipped cleanly because another StoreFeeder sync still owns the lock." |
            Tee-Object -FilePath $WrapperLog -Append
        return
    }
    [pscustomobject]@{
        process_id = $PID
        started_at = $StartTime.ToString("o")
        job_type   = "daily_inventory_mirror_reconciliation"
    } | Export-Csv -LiteralPath $SharedLockPath -NoTypeInformation
    $LockAcquired = $true

    $command = @(
        "scripts\run_global_supplier_inventory_mirror.py",
        "--targets", $TargetFile,
        "--live",
        "--confirm", "LIVE GLOBAL STOREFEEDER INVENTORY MIRROR",
        "--resume-run-dir", $OutDir
    )
    if ($ResumeRunDir) {
        $command += "--skip-feed-refresh"
    }

    "Running: $Python $($command -join ' ')" |
        Tee-Object -FilePath $WrapperLog -Append
    & $Python @command 1> $StdoutLog 2> $StderrLog
    $PythonExitCode = $LASTEXITCODE

    Get-Content -LiteralPath $StdoutLog -ErrorAction SilentlyContinue |
        Tee-Object -FilePath $WrapperLog -Append
    Get-Content -LiteralPath $StderrLog -ErrorAction SilentlyContinue |
        Tee-Object -FilePath $WrapperLog -Append

    if ($PythonExitCode -ne 0) {
        $FailureStage = "python_runtime"
        $PowerShellExitCode = $PythonExitCode
        throw "Daily inventory mirror failed with Python exit code $PythonExitCode."
    }
}
catch {
    $ExceptionText = $_.Exception.ToString()
    if (-not $FailureStage) {
        $FailureStage = "wrapper_exception"
    }
    if ($PowerShellExitCode -eq 0) {
        $PowerShellExitCode = 1
    }
    $ExceptionText | Tee-Object -FilePath $WrapperLog -Append
}
finally {
    if ($LockAcquired -and (Test-Path -LiteralPath $SharedLockPath)) {
        $lock = Import-Csv -LiteralPath $SharedLockPath -ErrorAction SilentlyContinue
        if ($lock.process_id -eq [string]$PID) {
            Remove-Item -LiteralPath $SharedLockPath -Force
        }
    }
    $EndTime = Get-Date
    Write-ExitStatus -EndTime $EndTime
    "Daily inventory mirror reconciliation finished: $($EndTime.ToString('o')); exit=$PowerShellExitCode" |
        Tee-Object -FilePath $WrapperLog -Append
}

exit $PowerShellExitCode
