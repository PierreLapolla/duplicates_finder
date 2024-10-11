@echo off
REM Check if the project folder exists
IF NOT EXIST "duplicates_finder" (
    REM Clone the repository if folder doesn't exist
    git clone https://github.com/PierreLapolla/duplicates_finder.git duplicates_finder
) ELSE (
    REM Pull the latest updates if the folder exists
    cd duplicates_finder
    git pull
)

REM Navigate to the project directory
cd duplicates_finder

REM Check if the virtual environment exists
IF NOT EXIST "venv" (
    REM Create a virtual environment
    python -m venv venv
)

REM Activate the virtual environment
call venv\Scripts\activate

REM Install dependencies
pip install -r requirements.txt

REM Run the Python app
python main.py

pause
