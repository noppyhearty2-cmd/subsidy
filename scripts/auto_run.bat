@echo off
cd /d C:\Users\noppy\Subsidy

if not exist logs mkdir logs

for /f "tokens=1,2 delims==" %%a in (.env) do (
    if "%%a"=="ANTHROPIC_API_KEY" set ANTHROPIC_API_KEY=%%b
)

python scripts\scrape_only.py --municipality osaka > %TEMP%\subsidy_count.txt 2>> logs\pipeline.log
set /p NEW_COUNT=< %TEMP%\subsidy_count.txt

echo %date% %time% new=%NEW_COUNT% >> logs\pipeline.log

if "%NEW_COUNT%"=="0" goto :end

git add data\raw\ data\state\
git commit -m "chore: scrape %NEW_COUNT% new urls %date%"
git push >> logs\pipeline.log 2>&1

:end
