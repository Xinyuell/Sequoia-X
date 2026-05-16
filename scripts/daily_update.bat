@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0daily_update.ps1"
exit /b %ERRORLEVEL%

