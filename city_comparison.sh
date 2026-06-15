#!/bin/bash

# ユーザーが指定した50市
declare -a PROVIDED_50_CITIES=(
    "sapporo" "hakodate" "asahikawa" "aomori" "morioka" "akita" "yamagata" "koriyama"
    "toyama" "kanazawa" "fukui" "nagano" "matsumoto" "gifu" "nagoya" "tsu"
    "otsu" "osaka" "kobe" "nara" "wakayama" "toyooka" "tottori" "matsue"
    "kurashiki" "kure" "tokushima" "takamatsu" "matsuyama" "kochi" "saga"
    "nagasaki" "kumamoto" "oita" "miyazaki" "kagoshima" "naha"
)

# JSONに含まれている市をグレップで取得
CITIES_IN_JSON=$(grep -o '"市ID": "[^"]*"' phase4-50cities-comprehensive-2026.json | sed 's/"市ID": "//;s/"$//' | sort | uniq)

echo "ユーザー指定の50市数: ${#PROVIDED_50_CITIES[@]}"
echo "実際のリスト内の市数: ${#PROVIDED_50_CITIES[@]}"
echo ""
echo "JSONに含まれている市数:"
echo "$CITIES_IN_JSON" | wc -l
echo ""

# 両者をarray に変換して比較
declare -A city_map
for city in $CITIES_IN_JSON; do
    city_map[$city]=1
done

MISSING=0
for city in "${PROVIDED_50_CITIES[@]}"; do
    if [[ -z ${city_map[$city]} ]]; then
        echo "JSONに未収載: $city"
        ((MISSING++))
    fi
done

if [ $MISSING -eq 0 ]; then
    echo "すべての市がJSONに含まれています"
fi
