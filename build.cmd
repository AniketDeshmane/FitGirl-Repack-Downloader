@echo off
REM Install dependencies and run Download Manager (Windows)
setlocal
cd /d "%~dp0"

where py >nul 2>&1
if %errorlevel% equ 0 (
  set PY=py -3
) else (
  where python >nul 2>&1
  if %errorlevel% neq 0 (
    echo Python 3 not found. Install Python 3 and try again.
    exit /b 1
  )
  set PY=python
)

if not exist "venv\Scripts\activate.bat" (
  echo Creating venv...
  %PY% -m venv venv
)
call venv\Scripts\activate.bat

echo Installing dependencies...
pip install -q -r requirements.txt
if %errorlevel% neq 0 exit /b %errorlevel%

echo Starting Download Manager...
%PY% download_manager.py
exit /b %errorlevel%
