@echo off
echo.
echo  Starting DataPulse Frontend on http://localhost:5173
echo.
cd /d "%~dp0frontend"
npm install
npm run dev
