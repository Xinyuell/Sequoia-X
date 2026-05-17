param(
    [string]$Action = ""
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Port = 8001
$Url = "http://127.0.0.1:$Port/"
$ApiUrl = "${Url}api/data/summary"
$DailyTime = "18:30"
$TaskName = "Sequoia-X-Daily-Data-Update"
$RuntimeDir = Join-Path $Root "runtime"
$LogDir = Join-Path $Root "logs"
$PidFile = Join-Path $RuntimeDir "webui.pid"
$WebLog = Join-Path $LogDir "webui.log"
$WebErr = Join-Path $LogDir "webui.err.log"

New-Item -ItemType Directory -Force -Path $RuntimeDir, $LogDir | Out-Null

function Get-PythonPath {
    $envFile = Join-Path $Root ".env"
    $envExample = Join-Path $Root ".env.example"
    if (-not (Test-Path $envFile) -and (Test-Path $envExample)) {
        Copy-Item $envExample $envFile
    }

    $python = Join-Path $Root ".venv\Scripts\python.exe"
    if (Test-Path $python) {
        return $python
    }

    $uv = Get-Command uv -ErrorAction SilentlyContinue
    if (-not $uv) {
        Write-Host "未找到 Python 虚拟环境，也未找到 uv。"
        Write-Host "请先安装 uv，或让开发人员执行一次 uv sync。"
        throw "缺少运行环境"
    }

    Write-Host "首次运行需要准备 Python 环境，请稍等..."
    Push-Location $Root
    try {
        uv sync
    }
    finally {
        Pop-Location
    }

    if (-not (Test-Path $python)) {
        throw "Python 环境准备失败"
    }
    return $python
}

function Stop-WebUI {
    if (Test-Path $PidFile) {
        $oldPid = (Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
        if ($oldPid) {
            Stop-Process -Id $oldPid -Force -ErrorAction SilentlyContinue
        }
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    }

    Get-CimInstance Win32_Process -Filter "name = 'python.exe'" |
        Where-Object { $_.CommandLine -like "*sequoia_x.web*" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

    Write-Host "WebUI 已停止。"
}

function Wait-WebUI {
    for ($i = 0; $i -lt 30; $i++) {
        try {
            Invoke-WebRequest -Uri $ApiUrl -UseBasicParsing -TimeoutSec 1 | Out-Null
            return $true
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    }
    return $false
}

function Start-WebUI {
    $python = Get-PythonPath
    $env:PYTHONUTF8 = "1"
    $process = Start-Process `
        -FilePath $python `
        -ArgumentList @("-m", "sequoia_x.web", "--host", "127.0.0.1", "--port", "$Port") `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $WebLog `
        -RedirectStandardError $WebErr `
        -WindowStyle Hidden `
        -PassThru
    Set-Content -Path $PidFile -Value $process.Id -Encoding ascii

    if (Wait-WebUI) {
        Write-Host "WebUI 已启动: $Url"
    }
    else {
        Write-Host "WebUI 可能启动失败，请查看日志: $WebErr"
    }
}

function Restart-WebUI {
    Stop-WebUI
    Start-WebUI
}

function Open-Browser {
    $freshUrl = "${Url}?v=$(Get-Date -Format 'yyyyMMddHHmmss')"
    Start-Process $freshUrl
}

function Install-DailyTask {
    $dailyBat = Join-Path $Root "scripts\daily_update.bat"
    schtasks /Create /TN $TaskName /TR "`"$dailyBat`"" /SC DAILY /ST $DailyTime /F
    if ($LASTEXITCODE -eq 0) {
        Write-Host "已安装每日自动更新任务，时间为 $DailyTime。"
    }
    else {
        Write-Host "自动更新任务安装失败。"
        Write-Host "可右键 install_daily_update.bat，选择以管理员身份运行后重试。"
    }
}

function Uninstall-DailyTask {
    schtasks /Delete /TN $TaskName /F 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "已卸载每日自动更新任务。"
    }
    else {
        Write-Host "未找到每日自动更新任务，或删除失败。"
    }
}

function Show-Status {
    Write-Host ""
    Write-Host "WebUI 地址: $Url"
    if (Test-Path $PidFile) {
        $webPid = (Get-Content $PidFile | Select-Object -First 1)
        Write-Host "WebUI PID: $webPid"
    }
    else {
        Write-Host "WebUI PID: 未记录"
    }

    try {
        $summary = Invoke-RestMethod -Uri $ApiUrl -TimeoutSec 2
        Write-Host ("本地数据: 股票 {0}, 行情 {1}, 最新日期 {2}" -f $summary.symbol_count, $summary.row_count, $summary.latest_date)
    }
    catch {
        Write-Host "WebUI 当前不可访问"
    }

    schtasks /Query /TN $TaskName 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "每日自动更新: 已安装，时间为 $DailyTime"
    }
    else {
        Write-Host "每日自动更新: 未安装"
    }
}

function Show-Menu {
    while ($true) {
        Write-Host ""
        Write-Host "==============================================="
        Write-Host "Sequoia-X 本地 WebUI 控制台"
        Write-Host "==============================================="
        Write-Host "1. 重启 WebUI 并打开浏览器"
        Write-Host "2. 只打开浏览器"
        Write-Host "3. 立即更新每日行情数据"
        Write-Host "4. 安装每日自动更新任务 - 每天 $DailyTime"
        Write-Host "5. 卸载每日自动更新任务"
        Write-Host "6. 查看 WebUI 和自动更新状态"
        Write-Host "7. 停止 WebUI"
        Write-Host "0. 退出"
        Write-Host ""

        $choice = Read-Host "请输入数字后按回车"
        switch ($choice) {
            "1" { Restart-WebUI; Open-Browser }
            "2" { Open-Browser }
            "3" { & (Join-Path $Root "scripts\daily_update.bat") }
            "4" { Install-DailyTask }
            "5" { Uninstall-DailyTask }
            "6" { Show-Status }
            "7" { Stop-WebUI }
            "0" { return }
            default { Write-Host "输入无效，请重新选择。" }
        }
    }
}

try {
    switch ($Action.ToLowerInvariant()) {
        "install-schedule" { Get-PythonPath | Out-Null; Install-DailyTask }
        "uninstall-schedule" { Get-PythonPath | Out-Null; Uninstall-DailyTask }
        "stop" { Get-PythonPath | Out-Null; Stop-WebUI }
        "status" { Get-PythonPath | Out-Null; Show-Status }
        "restart" { Restart-WebUI; Open-Browser }
        "open" { Open-Browser }
        default {
            Write-Host ""
            Write-Host "正在启动 Sequoia-X WebUI..."
            Restart-WebUI
            Open-Browser
            Show-Menu
        }
    }
}
catch {
    Write-Host ""
    Write-Host "操作失败: $($_.Exception.Message)"
    exit 1
}

Write-Host ""
Write-Host "已完成。可以关闭这个窗口。"
