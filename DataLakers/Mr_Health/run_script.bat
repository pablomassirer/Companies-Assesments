@echo off

REM Set the path to your Python executable (replace 'python' with 'python3' if needed)
set PYTHON_PATH="C:\Users\Pablo\AppData\Local\Programs\Python\Python310\python.exe"

REM Set the path to your Python script
set SCRIPT_PATH="C:\Users\Pablo\DevStuff\Companies_Assesments\DataLakers\Mr_Health\listener.py"

REM Change the current directory to the location of the Python script
cd /d "C:\Users\Pablo\DevStuff\Companies_Assesments\DataLakers\Mr_Health"

REM Install dependencies
pip install -r requirements.txt

REM Run the Python script
%PYTHON_PATH% %SCRIPT_PATH%

REM Pause to keep the command prompt open (optional)
pause

