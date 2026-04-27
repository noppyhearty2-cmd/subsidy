@echo off
cd /d C:\Users\noppy\Subsidy

REM ログディレクトリ作成
if not exist logs mkdir logs

echo %date% %time% - スクレイプ開始 >> logs\pipeline.log

REM スクレイプのみ実行（Anthropic API 不要）
python scripts\scrape_only.py --municipality osaka > %TEMP%\subsidy_count.txt 2>> logs\pipeline.log
set /p NEW_COUNT=< %TEMP%\subsidy_count.txt

echo %date% %time% - 新規取得: %NEW_COUNT% 件 >> logs\pipeline.log

if "%NEW_COUNT%"=="0" (
    echo %date% %time% - 新規なし、終了 >> logs\pipeline.log
    goto :end
)

REM 新規rawデータをGitにコミット（記事生成はClaude Codeで手動）
git add data\raw\ data\state\
git commit -m "chore: scrape %NEW_COUNT% new urls %date%" >> logs\pipeline.log 2>&1
git push >> logs\pipeline.log 2>&1

echo %date% %time% - raw データをプッシュしました。Claude Code で記事生成を実行してください >> logs\pipeline.log

:end
