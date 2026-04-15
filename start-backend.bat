@echo off
setlocal EnableDelayedExpansion

echo.
echo  ==============================================
echo      DataPulse - Backend Dev Server
echo  ==============================================
echo.

:: Change to the project root (same folder as this .bat)
cd /d "%~dp0"

:: ── Check that .env file exists ────────────────────────────────────
if not exist ".env" (
    echo  [ERROR] .env file not found in "%CD%"
    echo          Copy .env.example to .env and fill in your credentials.
    pause
    exit /b 1
)

:: ── Locate or create virtual environment ──────────────────────────
set "VENV_PY=.venv\Scripts\python.exe"
set "VENV_PIP=.venv\Scripts\pip.exe"

if not exist "%VENV_PY%" (
    echo  [1/3] Virtual environment not found. Creating .venv ...
    python -m venv .venv
    if errorlevel 1 (
        echo  [ERROR] Failed to create virtual environment.
        echo          Make sure Python 3.10+ is installed and on PATH.
        pause

    )
    echo        .venv created successfully.
) else (
    echo  [1/3] Virtual environment found.
)

:: ── Install / sync dependencies ────────────────────────────────────
echo  [2/3] Installing/updating Python dependencies from requirements.txt ...
"%VENV_PIP%" install -r requirements.txt
if errorlevel 1 (
    echo  [ERROR] pip install failed. Check requirements.txt and your internet connection.
    pause
    exit /b 1
)
echo        Dependencies OK.

:: ── Start FastAPI ──────────────────────────────────────────────────
echo  [3/3] Starting FastAPI on http://localhost:8000
echo.
echo        Backend logs will appear below.
echo        Press Ctrl+C to stop the server.
echo.

"%VENV_PY%" -m uvicorn backend.api:app --reload --host 0.0.0.0 --port 8000

if errorlevel 1 (
    echo.
    echo  [ERROR] uvicorn exited with an error. Check the logs above.
    pause
)
