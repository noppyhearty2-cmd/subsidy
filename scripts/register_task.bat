@echo off
REM Windowsタスクスケジューラに毎日23:00実行を登録する
REM 管理者権限で実行してください

schtasks /create /tn "SubsidyScraper" /tr "C:\Users\noppy\Subsidy\scripts\auto_run.bat" /sc daily /st 23:00 /ru "%USERNAME%" /f

if %errorlevel% == 0 (
    echo.
    echo タスク登録完了。毎日23:00に自動スクレイプが実行されます。
    echo 確認: schtasks /query /tn "SubsidyScraper"
) else (
    echo.
    echo 登録に失敗しました。管理者として実行してください。
)
pause
