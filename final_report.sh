#!/bin/bash

echo "=== Phase 4-A マークダウン生成レポート ==="
echo ""

# 生成されたファイル数
TOTAL_FILES=$(find site/src/content/subsidies -type f -name "2026-06-09-*.md" | wc -l)
CITIES=$(find site/src/content/subsidies -type f -name "2026-06-09-*.md" -exec dirname {} \; | sort | uniq | wc -l)
GENRES=10

echo "生成統計:"
echo "  - 対象市数: $CITIES"
echo "  - 対象ジャンル数: $GENRES"
echo "  - 総生成ファイル数: $TOTAL_FILES"
echo ""

echo "市別生成ファイル数:"
for dir in $(find site/src/content/subsidies -type d -maxdepth 1 -mindepth 1 | sort); do
    city_name=$(basename "$dir")
    count=$(find "$dir" -type f -name "2026-06-09-*.md" | wc -l)
    if [ $count -gt 0 ]; then
        echo "  - $city_name: $count"
    fi
done | head -40

echo ""
echo "サンプルファイル内容確認:"
SAMPLE_FILE=$(find site/src/content/subsidies -type f -name "2026-06-09-*.md" | head -1)
if [ -f "$SAMPLE_FILE" ]; then
    echo "ファイルパス: $SAMPLE_FILE"
    echo "ファイルサイズ: $(stat -c%s "$SAMPLE_FILE" 2>/dev/null || stat -f%z "$SAMPLE_FILE") bytes"
    echo "先頭20行:"
    head -20 "$SAMPLE_FILE"
fi

echo ""
echo "=== 生成完了 ==="
