@echo off
echo.
echo  ╔══════════════════════════════════════════╗
echo  ║    DataPulse — Local Development Start   ║
echo  ╚══════════════════════════════════════════╝
echo.

cd /d "%~dp0"

echo [1/3] Checking Python virtual environment...
if not exist ".venv" (
    echo  Creating venv...
    python -m venv .venv
)
call .venv\Scripts\activate.bat

echo [2/3] Installing/checking Python dependencies...
pip install -r requirements.txt --quiet

echo [3/3] Starting FastAPI backend on http://localhost:8000
echo       (Press Ctrl+C to stop, then run: cd frontend ^&^& npm run dev)
echo.
echo  IMPORTANT: Make sure .env file exists with your DB credentials!
echo.

python -m uvicorn backend.api:app --reload --host 0.0.0.0 --port 8000
