@echo off
setlocal EnableDelayedExpansion

echo.
echo  ==============================================
echo      DataPulse - Frontend Dev Server
echo  ==============================================
echo.

:: Change to the frontend directory
cd /d "%~dp0frontend"

:: ── Check node / npm is available ──────────────────────────────────
where npm >nul 2>&1
if errorlevel 1 goto npm_missing
goto npm_ok

:npm_missing
echo  [ERROR] npm not found on PATH.
echo          Install Node.js from https://nodejs.org/ (v18 or later recommended)
pause
exit /b 1

:npm_ok

:: ── Check .env file ────────────────────────────────────────────────
if not exist ".env" (
    echo  [WARN]  No frontend\.env file found.
    echo          VITE_GOOGLE_CLIENT_ID will be undefined and Google login will not work.
    echo          Create frontend\.env with:  VITE_GOOGLE_CLIENT_ID=your_client_id
    echo.
)

:: ── Install dependencies (skips if node_modules is up to date) ─────
echo  [1/2] Checking Node dependencies ...

:: FORCE CLEAR VITE CACHE
if exist "node_modules\.vite" (
    echo  [SYNC] Clearing Vite internal cache ...
    rd /s /q "node_modules\.vite"
)

call npm install
if errorlevel 1 (
    echo  [ERROR] npm install failed. Check your internet connection.
    pause
    exit /b 1
)

echo  [2/2] Starting Vite on http://localhost:5174 (FORCE SYNC)
echo.
echo        Make sure the backend is also running (start-backend.bat).
echo        Press Ctrl+C to stop the dev server.
echo.

call npx vite --port 5174 --force --clearScreen false

if errorlevel 1 (
    echo.
    echo  [ERROR] Vite exited with an error. Check the logs above.
    pause
)
