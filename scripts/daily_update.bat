@echo off
chcp 65001 >nul
setlocal EnableExtensions

set "ROOT=%~dp0.."
cd /d "%ROOT%"

set "LOG_DIR=%ROOT%\logs"
set "LOG_FILE=%LOG_DIR%\daily_update.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo.>> "%LOG_FILE%"
echo ==================================================>> "%LOG_FILE%"
echo [%date% %time%] Sequoia-X 每日数据更新开始>> "%LOG_FILE%"

if exist "%ROOT%\.venv\Scripts\python.exe" (
    set "PYTHON=%ROOT%\.venv\Scripts\python.exe"
) else (
    where uv >nul 2>nul
    if errorlevel 1 (
        echo [%date% %time%] ERROR: uv not found and .venv is missing>> "%LOG_FILE%"
        echo 未找到 Python 环境，请先运行 start_webui.bat。
        exit /b 1
    )
    uv sync >> "%LOG_FILE%" 2>&1
    if errorlevel 1 (
        echo [%date% %time%] ERROR: uv sync failed>> "%LOG_FILE%"
        echo Python 环境准备失败，请查看 %LOG_FILE%
        exit /b 1
    )
    set "PYTHON=%ROOT%\.venv\Scripts\python.exe"
)

echo 正在更新本地行情数据，请稍等...
"%PYTHON%" -m sequoia_x.tools.update_data --mode daily >> "%LOG_FILE%" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

if "%EXIT_CODE%"=="0" (
    echo [%date% %time%] Sequoia-X 每日数据更新完成>> "%LOG_FILE%"
    echo 更新完成。最近日志:
) else (
    echo [%date% %time%] Sequoia-X 每日数据更新失败，退出码 %EXIT_CODE%>> "%LOG_FILE%"
    echo 更新失败。最近日志:
)

powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-Content -Path '%LOG_FILE%' -Tail 30"
exit /b %EXIT_CODE%
