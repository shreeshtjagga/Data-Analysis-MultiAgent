@echo off
setlocal EnableDelayedExpansion

echo.
echo  ================================================
echo       DataPulse ^| Frontend Server
echo  ================================================
echo.

REM Always work from the frontend folder
cd /d "%~dp0frontend"

REM ── 1. Check Node / npm ──────────────────────────────────────────────────────
where npm >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Node.js / npm not found on PATH.
    echo.
    echo          Install Node.js 18 or later from: https://nodejs.org/
    echo          Then restart this script.
    echo.
    pause
    exit /b 1
)

for /f "tokens=1" %%V in ('node --version 2^>^&1') do set NODE_VER=%%V
echo  [OK]  Node.js %NODE_VER% found.

REM ── 2. Check frontend .env file ──────────────────────────────────────────────
if not exist ".env" (
    echo.
    echo  [WARN] frontend\.env file is missing.
    echo         Google login will NOT work without it.
    echo.
    echo         Ask the project owner to send you the frontend\.env file
    echo         and place it here:
    echo.
    echo         %~dp0frontend\.env
    echo.
    echo  Continuing anyway — email/password login will still work.
    echo.
)

REM ── 3. Install dependencies ──────────────────────────────────────────────────
echo  [1/2] Installing Node dependencies (first run may take ~1 min)...

REM Clear Vite's internal cache to prevent stale module issues
if exist "node_modules\.vite" (
    rd /s /q "node_modules\.vite" >nul 2>&1
)

call npm install --silent
if errorlevel 1 (
    echo  [ERROR] npm install failed. Check your internet connection.
    pause
    exit /b 1
)
echo  [OK]  Dependencies ready.

REM ── 4. Start Vite dev server ─────────────────────────────────────────────────
echo  [2/2] Starting frontend...
echo.
echo  ================================================
echo   App running at:  http://localhost:5173
echo   Make sure start-backend.bat is also running!
echo   Press Ctrl+C to stop
echo  ================================================
echo.

call npx vite --port 5173 --force --clearScreen false

if errorlevel 1 (
    echo.
    echo  [ERROR] Vite exited with an error. Check the logs above.
    pause
)
