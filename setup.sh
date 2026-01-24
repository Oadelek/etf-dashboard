#!/bin/bash

# ETF Dashboard - Quick Setup Script
# Run this once after cloning to set up both backend and frontend

set -e  # Exit on any error

echo "Setting up ETF Dashboard..."
echo ""

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v python &> /dev/null && ! command -v python3 &> /dev/null; then
    echo "ERROR: Python not found. Please install Python 3.11+"
    exit 1
fi

if ! command -v node &> /dev/null; then
    echo "ERROR: Node.js not found. Please install Node.js 18+"
    exit 1
fi

if ! command -v npm &> /dev/null; then
    echo "ERROR: npm not found. Please install npm"
    exit 1
fi

echo "All prerequisites found"
echo ""

# Setup Backend
echo "Setting up backend..."
cd backend

if [ ! -d "venv" ]; then
    echo "  Creating Python virtual environment..."
    python -m venv venv 2>/dev/null || python3 -m venv venv
fi

echo "  Installing Python dependencies..."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    source venv/Scripts/activate
else
    source venv/bin/activate
fi
pip install -r requirements.txt --quiet

echo "Backend ready"
cd ..

# Setup Frontend
echo ""
echo "Setting up frontend..."
cd frontend

echo "  Installing npm dependencies..."
npm install --silent

echo "Frontend ready"
cd ..

# Done
echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "To start the application, run these in separate terminals:"
echo ""
echo "Terminal 1 (Backend):"
echo "  cd backend"
echo "  source venv/Scripts/activate  # Windows"
echo "  # or: source venv/bin/activate  # Mac/Linux"
echo "  uvicorn app.main:app --reload --port 8000"
echo ""
echo "Terminal 2 (Frontend):"
echo "  cd frontend"
echo "  npm run dev"
echo ""
echo "Then open http://localhost:5173 in your browser"
echo ""
