@echo off
echo.
echo  Starting DataPulse Frontend on http://localhost:5173
echo.
cd /d "%~dp0frontend"
call npm install
call npm run dev
pause
