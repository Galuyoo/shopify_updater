$ErrorActionPreference = "Stop"

Set-Location "C:\shopify_updater_amazon_work"

$Py = "C:\Users\salah\AppData\Local\Programs\Python\Python311\python.exe"

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

$LogDir = "reports\new_product_onboarding_delta_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$RunId = Get-Date -Format yyyyMMdd_HHmmss
$LogPath = "$LogDir\$RunId.log"

"== new product onboarding delta ==" *> $LogPath
& $Py scripts\run_new_product_onboarding_delta.py @args *>> $LogPath
$ExitCode = $LASTEXITCODE

Get-Content $LogPath
exit $ExitCode