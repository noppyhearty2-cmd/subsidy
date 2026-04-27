@echo off
schtasks /create /tn "SubsidyScraper" /tr "C:\Users\noppy\Subsidy\scripts\auto_run.bat" /sc daily /st 23:00 /ru "%USERNAME%" /f
if %errorlevel% == 0 (
    echo SUCCESS: Task registered. Runs daily at 23:00.
    echo Check: schtasks /query /tn "SubsidyScraper"
) else (
    echo FAILED: Please run as administrator.
)
pause
