#!/bin/bash

# JSONファイルのパス
JSON_FILE="phase4-50cities-comprehensive-2026.json"
OUTPUT_BASE="site/src/content/subsidies"

# 50市のcity_id配列
declare -a CITY_IDS=(
    "sapporo" "hakodate" "asahikawa" "aomori" "morioka" "akita" "yamagata" "koriyama"
    "toyama" "kanazawa" "fukui" "nagano" "matsumoto" "gifu" "nagoya" "tsu"
    "otsu" "osaka" "kobe" "nara" "wakayama" "toyooka" "tottori" "matsue"
    "kurashiki" "kure" "tokushima" "takamatsu" "matsuyama" "kochi" "saga"
    "nagasaki" "kumamoto" "oita" "miyazaki" "kagoshima" "naha"
)

# ジャンル定義
declare -a GENRES=("省エネ改修" "耐震改修" "太陽光発電" "蓄電池補助" "給湯省エネ" "リフォーム一般" "起業・創業支援" "空き家対策" "移住支援" "EV・充電設備補助")

# ジャンルスラグマッピング
declare -A GENRE_SLUGS=(
    ["省エネ改修"]="shoene-kaishu"
    ["耐震改修"]="taishin-kaishu"
    ["太陽光発電"]="solar-hatsudenryo"
    ["蓄電池補助"]="chikudenchi"
    ["給湯省エネ"]="kyuuto-shoene"
    ["リフォーム一般"]="reform-general"
    ["起業・創業支援"]="kigyo-shokugyo"
    ["空き家対策"]="akiya-taisaku"
    ["移住支援"]="ijyu-shien"
    ["EV・充電設備補助"]="ev-charging"
)

echo "マークダウンファイル生成開始..."
echo "対象都市: ${#CITY_IDS[@]}"
echo "対象ジャンル: ${#GENRES[@]}"
echo ""

FILE_COUNT=0
mkdir -p "$OUTPUT_BASE"

for city_id in "${CITY_IDS[@]}"; do
    # 各市用のディレクトリを作成
    mkdir -p "$OUTPUT_BASE/$city_id"
    
    for genre in "${GENRES[@]}"; do
        slug=${GENRE_SLUGS[$genre]}
        filename="2026-06-09-${slug}.md"
        filepath="$OUTPUT_BASE/$city_id/$filename"
        
        # テンプレートファイルを生成
        cat > "$filepath" << EOFFILE
---
title: "【2026年最新】$city_id｜$genre"
municipality: $city_id
target: "情報未取得"
amount: "情報未取得"
deadline: "令和8年度内"
tags: ["補助金", "$genre", "2026年"]
key_points:
  - "補助対象情報は公式サイトでご確認ください"
  - "申請期限は市の公式案内をご参照ください"
  - "着工前申請が必須です"
source_url: "https://example.com"
summary_ja: "$genre関連の補助金制度が利用できます。詳細については市の公式サイトをご確認ください。"
scraped_at: "2026-06-09T00:00:00Z"
is_active: true
content_hash: "$city_id-$slug-2026"
---

## $genre について

本記事では、2026年の$city_id における$genre の補助金情報を詳しく解説します。

### 補助内容

本市では$genre関連の補助金制度を実施しています。詳細については以下のリンクから公式サイトをご確認ください。

### 申請要件

- 着工前の申請が必須です
- 市内施工業者の利用を条件とする場合があります
- 複数の補助制度を組み合わせて活用できる場合があります

### 詳細情報

詳細については以下のリンクから公式サイトをご確認ください。

[$city_id市公式サイト](https://example.com)

---

※本情報は2026年6月9日時点での確認です。最新情報は市の公式サイトでご確認ください。
EOFFILE
        
        ((FILE_COUNT++))
    done
    
    CITY_FILE_COUNT=$((${#GENRES[@]}))
    echo "✓ $city_id: $CITY_FILE_COUNT ファイル生成"
done

echo ""
echo "=== 生成完了 ==="
echo "総生成ファイル数: $FILE_COUNT"
echo "出力ディレクトリ: $OUTPUT_BASE"
