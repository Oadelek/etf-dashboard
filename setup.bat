@echo off
REM Spectra — Quick Setup Script (Windows)
REM Run this once after cloning to set up both backend and frontend

echo.
echo Setting up Spectra...
echo.

REM Check for Python
where python >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Python not found. Please install Python 3.11+
    exit /b 1
)

REM Check for Node
where node >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Node.js not found. Please install Node.js 18+
    exit /b 1
)

REM Check for npm
where npm >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: npm not found. Please install npm
    exit /b 1
)

echo All prerequisites found
echo.

REM Setup Backend
echo Setting up backend...
cd backend

if not exist "venv" (
    echo   Creating Python virtual environment...
    python -m venv venv
)

echo   Installing Python dependencies...
call venv\Scripts\activate.bat
pip install -r requirements.txt --quiet

echo Backend ready
cd ..

REM Setup Frontend
echo.
echo Setting up frontend...
cd frontend

echo   Installing npm dependencies...
call npm install --silent

echo Frontend ready
cd ..

REM Done
echo.
echo ==========================================
echo Setup complete!
echo ==========================================
echo.
echo To start the application, run these in separate terminals:
echo.
echo Terminal 1 (Backend):
echo   cd backend
echo   venv\Scripts\activate
echo   uvicorn app.main:app --reload --port 8000
echo.
echo Terminal 2 (Frontend):
echo   cd frontend
echo   npm run dev
echo.
echo Then open http://localhost:5173 in your browser
echo.
