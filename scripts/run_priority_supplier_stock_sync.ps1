param(
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

$ProjectRoot = "C:\shopify_updater_amazon_work"
$Python = "C:\Users\salah\AppData\Local\Programs\Python\Python311\python.exe"

$TargetFile = "data\storefeeder_priority_stock_update_targets.csv"
$RunId = Get-Date -Format "yyyyMMdd_HHmmss"
$OutDir = Join-Path $ProjectRoot "reports\scheduled_priority_supplier_sync\$RunId"

$WrapperLog = Join-Path $OutDir "wrapper_run.log"
$StdoutLog = Join-Path $OutDir "python_stdout.log"
$StderrLog = Join-Path $OutDir "python_stderr.log"
$ExitStatusPath = Join-Path $OutDir "wrapper_exit_status.csv"

$SharedLockPath = Join-Path $ProjectRoot "reports\storefeeder_stock_sync.lock"

$StartTime = Get-Date
$PythonExitCode = $null
$PowerShellExitCode = 0
$FailureStage = ""
$ExceptionText = ""
$LockAcquired = $false

function Write-ExitStatus {
    param(
        [datetime]$EndTime
    )

    [pscustomobject]@{
        start_timestamp         = $StartTime.ToString("o")
        end_timestamp           = $EndTime.ToString("o")
        powershell_exit_code    = $PowerShellExitCode
        python_exit_code        = if ($null -eq $PythonExitCode) { "" } else { $PythonExitCode }
        failure_stage           = $FailureStage
        exception_text          = $ExceptionText
        target_file             = $TargetFile
        output_directory        = $OutDir
        lock_path               = $SharedLockPath
        lock_acquired           = $LockAcquired
    } | Export-Csv -LiteralPath $ExitStatusPath -NoTypeInformation
}

try {
    Set-Location $ProjectRoot
    New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

    "Priority supplier sync started: $($StartTime.ToString('o'))" |
        Tee-Object -FilePath $WrapperLog

    if ($WhatIf) {
        "WhatIf mode: Python was not started." |
            Tee-Object -FilePath $WrapperLog -Append

        return
    }

    if (Test-Path -LiteralPath $SharedLockPath) {
        $lockAge = (Get-Date) - (Get-Item -LiteralPath $SharedLockPath).LastWriteTime

        if ($lockAge.TotalHours -lt 6) {
            "Another StoreFeeder stock sync appears to be running. This priority cycle was skipped." |
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
        job_type   = "priority_supplier_sync"
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
                $key, $value = $line.Split("=", 2)
                $value = $value.Trim().Trim('"').Trim("'")

                [Environment]::SetEnvironmentVariable(
                    $key.Trim(),
                    $value,
                    "Process"
                )
            }
        }
    }

    $command = @(
        "scripts\run_supplier_stock_fast_update.py",
        "--catalogue", "reports\cleaned_catalogue\cleaned_storefeeder_catalogue.csv",
        "--targets", $TargetFile,
        "--missing-as-zero",
        "--api-limit", "100000",
        "--buffer", "0",
        "--max-stock", "999999",
        "--live-stock-update",
        "--inventory-mirror-families", "data\storefeeder_inventory_mirror_families.csv",
        "--inventory-mirror-live-verify",
        "--scheduled-run",
        "--out-dir", $OutDir
    )

    "Running: $Python $($command -join ' ')" |
        Tee-Object -FilePath $WrapperLog -Append

    & $Python @command 1> $StdoutLog 2> $StderrLog
    $PythonExitCode = $LASTEXITCODE

    if (Test-Path -LiteralPath $StdoutLog) {
        Get-Content -LiteralPath $StdoutLog |
            Tee-Object -FilePath $WrapperLog -Append
    }

    if (Test-Path -LiteralPath $StderrLog) {
        Get-Content -LiteralPath $StderrLog |
            Tee-Object -FilePath $WrapperLog -Append
    }

    if ($PythonExitCode -ne 0) {
        $FailureStage = "python_runtime"
        $PowerShellExitCode = $PythonExitCode

        throw "Priority supplier sync failed with Python exit code $PythonExitCode."
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

    $ExceptionText |
        Tee-Object -FilePath $WrapperLog -Append
}
finally {
    if ($LockAcquired -and (Test-Path -LiteralPath $SharedLockPath)) {
        Remove-Item -LiteralPath $SharedLockPath -Force
    }

    $EndTime = Get-Date
    Write-ExitStatus -EndTime $EndTime

    try {
        & $Python `
            "scripts\write_stock_sync_metric.py" `
            "--run-dir" $OutDir `
            "--job-type" "priority_supplier_sync" `
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

    "Priority supplier sync finished: $($EndTime.ToString('o')); exit=$PowerShellExitCode" |
        Tee-Object -FilePath $WrapperLog -Append
}

exit $PowerShellExitCode
