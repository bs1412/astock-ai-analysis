@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not defined ANNOUNCEMENT_PYTHON set "ANNOUNCEMENT_PYTHON=%~dp0.venv-announcements\Scripts\python.exe"
if not exist "%ANNOUNCEMENT_PYTHON%" (
  echo 请先按 announcement_lab\README.md 安装独立环境，或设置 ANNOUNCEMENT_PYTHON。
  exit /b 1
)
"%ANNOUNCEMENT_PYTHON%" -I announcement_lab\serve.py
