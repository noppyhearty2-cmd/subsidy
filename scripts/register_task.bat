@echo off
REM Windowsタスクスケジューラに毎日03:00実行を登録する
REM 管理者権限で実行してください

schtasks /create ^
  /tn "SubsidyScraper" ^
  /tr "C:\Users\noppy\Subsidy\scripts\auto_run.bat" ^
  /sc daily ^
  /st 03:00 ^
  /ru "%USERNAME%" ^
  /f

echo.
echo タスク登録完了。毎日03:00に自動スクレイプが実行されます。
echo 確認: schtasks /query /tn "SubsidyScraper"
pause
