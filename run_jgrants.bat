@echo off
cd /d C:\Users\noppy\Subsidy
set PYTHONIOENCODING=utf-8
C:\Users\noppy\AppData\Local\Python\pythoncore-3.14-64\python.exe scraper\run_jgrants.py >> data\run_logs\jgrants_stdout.log 2>&1
