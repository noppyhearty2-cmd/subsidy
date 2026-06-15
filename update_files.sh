#!/bin/bash

# JSONファイルのパス
JSON_FILE="phase4-50cities-comprehensive-2026.json"
OUTPUT_BASE="site/src/content/subsidies"

# 指定された50市のcity_id配列
declare -a CITY_IDS=(
    "sapporo" "hakodate" "asahikawa" "aomori" "morioka" "akita" "yamagata" "koriyama"
    "toyama" "kanazawa" "fukui" "nagano" "matsumoto" "gifu" "nagoya" "tsu"
    "otsu" "osaka" "kobe" "nara" "wakayama" "toyooka" "tottori" "matsue"
    "kurashiki" "kure" "tokushima" "takamatsu" "matsuyama" "kochi" "saga"
    "nagasaki" "kumamoto" "oita" "miyazaki" "kagoshima" "naha"
)

echo "現在のcity_idの数: ${#CITY_IDS[@]}"
for cid in "${CITY_IDS[@]}"; do
    echo "  - $cid"
done
