@echo off
cd /d "%~dp0"
echo Installing dependencies...
pip install -r requirements.txt --quiet
echo.
echo Starting Close Checklist...
echo Open your browser at:  http://127.0.0.1:5050
echo.
python app.py
pause
