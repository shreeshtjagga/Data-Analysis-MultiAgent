@echo off
setlocal EnableDelayedExpansion

echo.
echo  ==============================================
echo      DataPulse - Backend Dev Server (Live Logs)
echo  ==============================================
echo.

:: Change to the project root (where this .bat lives)
cd /d "%~dp0"

:: ── Check .env file ─────────────────────────────────────────────────────
if not exist ".env" (
    echo  [ERROR] .env file not found!
    echo          Copy .env.example to .env and configure your Google OAuth keys.
    pause
    exit /b 1
)

:: ── Virtual environment ─────────────────────────────────────────────────
if not exist ".venv\Scripts\python.exe" (
    echo  [1/3] Creating virtual environment .venv ...
    python -m venv .venv
    if errorlevel 1 (
        echo  [ERROR] Failed to create virtual environment.
        pause
        exit /b 1
    )
    echo        .venv created.
) else (
    echo  [1/3] Virtual environment found.
)

:: ── Activate venv ───────────────────────────────────────────────────────
call ".venv\Scripts\activate.bat"

:: ── Dependencies ────────────────────────────────────────────────────────
echo  [2/3] Updating dependencies...
python -m pip install --upgrade pip --quiet
python -m pip install greenlet==3.1.1 --prefer-binary --quiet
python -m pip install -r requirements.txt --prefer-binary
echo        Dependencies ready.

:: ── Start FastAPI with LIVE console logs ────────────────────────────────
echo  [3/3] Starting FastAPI server...
echo.
echo  ==============================================
echo  Server running at:  http://localhost:8000
echo  Press Ctrl+C to stop
echo  ==============================================
echo.

python -m uvicorn backend.api:app --reload --host 0.0.0.0 --port 8000

:: ── Only reaches here if server crashes immediately ─────────────────────
if errorlevel 1 (
    echo.
    echo  [ERROR] Server failed to start!
    echo  Check the error message above.
    echo  Common causes: syntax error in visualizer.py or missing dependency.
    pause
    exit /b 1
)