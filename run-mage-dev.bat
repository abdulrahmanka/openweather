@echo off
set "VENV_DIR=venv"

REM Check if the virtual environment exists
if not exist "%VENV_DIR%" (
    REM Create the virtual environment if it doesn't exist
    python -m venv %VENV_DIR%
    echo Created virtual environment in %VENV_DIR%.
)

REM Activate the virtual environment
CALL %VENV_DIR%\Scripts\activate.bat

python -m pip install -r requirements.txt
cd ../
mage start openweather_etl
