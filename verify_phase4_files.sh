#!/bin/bash

# Phase 4-A対象の37市
declare -a PHASE4_CITIES=(
    "sapporo" "hakodate" "asahikawa" "aomori" "morioka" "akita" "yamagata" "koriyama"
    "toyama" "kanazawa" "fukui" "nagano" "matsumoto" "gifu" "nagoya" "tsu"
    "otsu" "osaka" "kobe" "nara" "wakayama" "toyooka" "tottori" "matsue"
    "kurashiki" "kure" "tokushima" "takamatsu" "matsuyama" "kochi" "saga"
    "nagasaki" "kumamoto" "oita" "miyazaki" "kagoshima" "naha"
)

echo "=== Phase 4-A マークダウン生成確認レポート ==="
echo ""

TOTAL=0
for city in "${PHASE4_CITIES[@]}"; do
    count=$(find site/src/content/subsidies/$city -type f -name "2026-06-09-*.md" 2>/dev/null | wc -l)
    if [ $count -eq 10 ]; then
        echo "✓ $city: $count/10 ファイル"
        ((TOTAL += count))
    elif [ $count -gt 0 ]; then
        echo "✗ $city: $count/10 ファイル（不完全）"
        ((TOTAL += count))
    else
        echo "✗ $city: 0/10 ファイル（未作成）"
    fi
done

echo ""
echo "=== 統計 ==="
echo "総生成ファイル数: $TOTAL"
echo "目標ファイル数: $((${#PHASE4_CITIES[@]} * 10))"

if [ $TOTAL -eq $((${#PHASE4_CITIES[@]} * 10)) ]; then
    echo "状態: ✓ 完了"
else
    echo "状態: ✗ 不完全"
fi

echo ""
echo "サンプルファイル内容:"
SAMPLE=$(find site/src/content/subsidies/sapporo -type f -name "2026-06-09-*.md" | head -1)
if [ -f "$SAMPLE" ]; then
    echo "ファイル: $SAMPLE"
    echo "---"
    head -25 "$SAMPLE"
fi
