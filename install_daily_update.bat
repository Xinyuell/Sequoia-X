@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\webui_menu.ps1" install-schedule
pause

