@echo off
call conda activate financetx
cd /d "D:\DATA_ENGINEER\PROJECTS\NOTEBOOKS\finance-tx\app"
python agent.py >> agent.log 2>&1
