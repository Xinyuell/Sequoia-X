@echo off
chcp 65001 >nul
setlocal EnableExtensions

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"
cd /d "%ROOT%"

set "PORT=8001"
set "URL=http://127.0.0.1:%PORT%/"
set "API_URL=%URL%api/data/summary"
set "DAILY_TIME=18:30"
set "TASK_NAME=Sequoia-X-Daily-Data-Update"
set "RUNTIME_DIR=%ROOT%\runtime"
set "LOG_DIR=%ROOT%\logs"
set "PID_FILE=%RUNTIME_DIR%\webui.pid"
set "WEB_LOG=%LOG_DIR%\webui.log"
set "WEB_ERR=%LOG_DIR%\webui.err.log"

if not exist "%RUNTIME_DIR%" mkdir "%RUNTIME_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

call :ensure_python
if errorlevel 1 goto end

if /i "%~1"=="install-schedule" (
    call :install_schedule
    goto end
)
if /i "%~1"=="uninstall-schedule" (
    call :uninstall_schedule
    goto end
)
if /i "%~1"=="stop" (
    call :stop_web
    goto end
)
if /i "%~1"=="status" (
    call :status
    goto end
)
if /i "%~1"=="restart" (
    call :restart_web
    call :open_browser
    goto end
)
if /i "%~1"=="open" (
    call :open_browser
    goto end
)

echo.
echo 正在启动 Sequoia-X WebUI...
call :restart_web
call :open_browser

:menu
echo.
echo ===============================================
echo Sequoia-X 本地 WebUI 控制台
echo ===============================================
echo 1. 重启 WebUI 并打开浏览器
echo 2. 只打开浏览器
echo 3. 立即更新每日行情数据
echo 4. 安装每日自动更新任务 - 每天 %DAILY_TIME%
echo 5. 卸载每日自动更新任务
echo 6. 查看 WebUI 和自动更新状态
echo 7. 停止 WebUI
echo 0. 退出
echo.
set /p "ACTION=请输入数字后按回车: "

if "%ACTION%"=="1" (
    call :restart_web
    call :open_browser
    goto menu
)
if "%ACTION%"=="2" (
    call :open_browser
    goto menu
)
if "%ACTION%"=="3" (
    call "%ROOT%\scripts\daily_update.bat"
    goto menu
)
if "%ACTION%"=="4" (
    call :install_schedule
    goto menu
)
if "%ACTION%"=="5" (
    call :uninstall_schedule
    goto menu
)
if "%ACTION%"=="6" (
    call :status
    goto menu
)
if "%ACTION%"=="7" (
    call :stop_web
    goto menu
)
if "%ACTION%"=="0" goto end

echo 输入无效，请重新选择。
goto menu

:ensure_python
if not exist "%ROOT%\.env" (
    if exist "%ROOT%\.env.example" copy "%ROOT%\.env.example" "%ROOT%\.env" >nul
)

if exist "%ROOT%\.venv\Scripts\python.exe" (
    set "PYTHON=%ROOT%\.venv\Scripts\python.exe"
    exit /b 0
)

where uv >nul 2>nul
if errorlevel 1 (
    echo 未找到 Python 虚拟环境，也未找到 uv。
    echo 请先安装 uv，或让开发人员执行一次 uv sync。
    exit /b 1
)

echo 首次运行需要准备 Python 环境，请稍等...
uv sync
if errorlevel 1 (
    echo Python 环境准备失败。
    exit /b 1
)

set "PYTHON=%ROOT%\.venv\Scripts\python.exe"
exit /b 0

:restart_web
call :stop_web
call :start_web
exit /b 0

:stop_web
if exist "%PID_FILE%" (
    for /f %%P in (%PID_FILE%) do taskkill /PID %%P /T /F >nul 2>nul
    del "%PID_FILE%" >nul 2>nul
)
powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-CimInstance Win32_Process -Filter \"name = 'python.exe'\" | Where-Object { $_.CommandLine -like '*sequoia_x.web*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>nul
echo WebUI 已停止。
exit /b 0

:start_web
powershell -NoProfile -ExecutionPolicy Bypass -Command "$env:PYTHONUTF8='1'; $p=Start-Process -FilePath '%PYTHON%' -ArgumentList @('-m','sequoia_x.web','--host','127.0.0.1','--port','%PORT%') -WorkingDirectory '%ROOT%' -RedirectStandardOutput '%WEB_LOG%' -RedirectStandardError '%WEB_ERR%' -WindowStyle Hidden -PassThru; $p.Id | Set-Content -Encoding ascii '%PID_FILE%'"
call :wait_web
if errorlevel 1 (
    echo WebUI 可能启动失败，请查看日志: %WEB_ERR%
) else (
    echo WebUI 已启动: %URL%
)
exit /b 0

:wait_web
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ok=$false; for($i=0; $i -lt 30; $i++){ try { Invoke-WebRequest -Uri '%API_URL%' -UseBasicParsing -TimeoutSec 1 | Out-Null; $ok=$true; break } catch { Start-Sleep -Milliseconds 500 } }; if($ok){ exit 0 } else { exit 1 }"
exit /b %ERRORLEVEL%

:open_browser
start "" "%URL%"
exit /b 0

:install_schedule
schtasks /Create /TN "%TASK_NAME%" /TR "\"%ROOT%\scripts\daily_update.bat\"" /SC DAILY /ST %DAILY_TIME% /F
if errorlevel 1 (
    echo 自动更新任务安装失败。
    echo 可右键 install_daily_update.bat，选择以管理员身份运行后重试。
) else (
    echo 已安装每日自动更新任务，时间为 %DAILY_TIME%。
)
exit /b 0

:uninstall_schedule
schtasks /Delete /TN "%TASK_NAME%" /F >nul 2>nul
if errorlevel 1 (
    echo 未找到每日自动更新任务，或删除失败。
) else (
    echo 已卸载每日自动更新任务。
)
exit /b 0

:status
echo.
echo WebUI 地址: %URL%
if exist "%PID_FILE%" (
    for /f %%P in (%PID_FILE%) do echo WebUI PID: %%P
) else (
    echo WebUI PID: 未记录
)
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r=Invoke-RestMethod -Uri '%API_URL%' -TimeoutSec 2; Write-Host ('本地数据: 股票 {0}, 行情 {1}, 最新日期 {2}' -f $r.symbol_count,$r.row_count,$r.latest_date) } catch { Write-Host 'WebUI 当前不可访问' }"
schtasks /Query /TN "%TASK_NAME%" >nul 2>nul
if errorlevel 1 (
    echo 每日自动更新: 未安装
) else (
    echo 每日自动更新: 已安装，时间为 %DAILY_TIME%
)
exit /b 0

:end
echo.
echo 已完成。可以关闭这个窗口。
endlocal
