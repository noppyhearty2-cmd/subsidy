#!/bin/bash

# Phase 5: 主要50市のテンプレート版を実データで充実化

# 対象市リスト（ID: 日本語名）
declare -A cities=(
    [tsukuba]="つくば市"
    [kumagaya]="熊谷市"
    [honjo]="本庄市"
    [chichibu]="秩父市"
    [gyoda]="行田市"
    [shizuoka]="静岡市"
    [numazu]="沼津市"
    [fuji]="富士市"
    [toyohashi]="豊橋市"
    [okazaki]="岡崎市"
    [anjo]="安城市"
    [neyagawa]="寝屋川市"
    [ibaraki]="茨木市"
    [takatsuki]="高槻市"
    [amagasaki]="尼崎市"
    [nishinomiya]="西宮市"
    [akashi]="明石市"
    [himeji]="姫路市"
    [fukuyama]="福山市"
    [higashihiroshima]="東広島市"
    [ube]="宇部市"
    [yamaguchi]="山口市"
    [imabari]="今治市"
    [toyooka]="豊岡市"
    [wakayama]="和歌山市"
    [nobeoka]="延岡市"
)

# 10ジャンルの補助金タイプ
subsidy_types=(
    "akiya-taisaku"        # 空き家対策
    "chikudenchi"          # 地中熱
    "ev-charging"          # EV充電器
    "ijyu-shien"           # 移住支援
    "kigyo-shokugyo"       # 企業・雇用
    "kyuuto-shoene"        # 急速省エネ改修
    "reform-general"       # 改修一般
    "shoene-kaishu"        # 省エネ改修
    "solar-hatsudenryo"    # 太陽光発電
    "taishin-kaishu"       # 耐震改修
)

echo "Phase 5: 主要市のテンプレート版統計"
total_files=0
for city_id in "${!cities[@]}"; do
    city_name="${cities[$city_id]}"
    city_path="site/src/content/subsidies/$city_id"
    
    if [ -d "$city_path" ]; then
        file_count=$(ls "$city_path"/2026-06-09-*.md 2>/dev/null | wc -l)
        if [ "$file_count" -gt 0 ]; then
            echo "$city_id ($city_name): $file_count ファイル"
            ((total_files += file_count))
        fi
    fi
done

echo ""
echo "更新対象総ファイル数: $total_files"
