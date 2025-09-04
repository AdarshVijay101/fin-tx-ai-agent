@echo off
REM Activate conda env and run the monitor. Append output to a log file.
call conda activate financetx
cd /d "D:\DATA_ENGINEER\PROJECTS\NOTEBOOKS\finance-tx\app"
python monitor_errors.py >> monitor.log 2>&1
