$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LogDir = Join-Path $Root "logs"
$LogFile = Join-Path $LogDir "daily_update.log"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Add-Content -Path $LogFile -Value ""
Add-Content -Path $LogFile -Value "=================================================="
Add-Content -Path $LogFile -Value "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Sequoia-X 每日数据更新开始"

$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    $uv = Get-Command uv -ErrorAction SilentlyContinue
    if (-not $uv) {
        Add-Content -Path $LogFile -Value "ERROR: 未找到 uv，且 .venv 不存在"
        Write-Host "未找到 Python 环境，请先运行 start_webui.bat。"
        exit 1
    }

    Push-Location $Root
    try {
        uv sync 2>&1 | Tee-Object -FilePath $LogFile -Append
    }
    finally {
        Pop-Location
    }
}

Write-Host "正在更新本地行情数据，请稍等..."
Push-Location $Root
try {
    & $Python -m sequoia_x.tools.update_data --mode daily 2>&1 | Tee-Object -FilePath $LogFile -Append
    $exitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}

if ($exitCode -eq 0) {
    Add-Content -Path $LogFile -Value "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Sequoia-X 每日数据更新完成"
    Write-Host "更新完成。最近日志:"
}
else {
    Add-Content -Path $LogFile -Value "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Sequoia-X 每日数据更新失败，退出码 $exitCode"
    Write-Host "更新失败。最近日志:"
}

Get-Content -Path $LogFile -Tail 30
exit $exitCode

