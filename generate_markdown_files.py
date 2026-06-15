#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
from datetime import datetime
from pathlib import Path

def extract_data_value(data, key):
    """JSONデータから値を安全に抽出"""
    if data is None:
        return None
    if isinstance(data, dict):
        return data.get(key)
    return None

def generate_summary(target, amount):
    """補助対象と補助額からサマリーを生成"""
    if target and amount:
        return f"{target}に対して{amount}の補助金が利用できます。"
    elif target:
        return f"{target}が対象の補助金制度があります。"
    elif amount:
        return f"{amount}の補助金が利用できます。"
    else:
        return "補助金制度が利用できます。"

def get_genre_slug(genre_name):
    """ジャンル名からスラグを取得"""
    genre_map = {
        "省エネ改修": "shoene-kaishu",
        "耐震改修": "taishin-kaishu",
        "太陽光発電": "solar-hatsudenryo",
        "蓄電池補助": "chikudenchi",
        "給湯省エネ": "kyuuto-shoene",
        "リフォーム一般": "reform-general",
        "起業・創業支援": "kigyo-shokugyo",
        "空き家対策": "akiya-taisaku",
        "移住支援": "ijyu-shien",
        "EV・充電設備補助": "ev-charging"
    }
    return genre_map.get(genre_name, "unknown")

def main():
    # JSONファイルを読み込む
    json_path = "phase4-50cities-comprehensive-2026.json"

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 出力ディレクトリ
    output_base = Path("site/src/content/subsidies")

    # 都道府県キーを抽出（メタデータ以外）
    metadata_keys = {
        '調査概要', '対象ジャンル', '調査結果サマリー',
        '補助金額最大市', '申請注意事項', 'データ完全性',
        'next_actions', '補助金分布統計'
    }
    pref_keys = [k for k in data.keys() if k not in metadata_keys]

    # ジャンル定義
    genres = [
        "省エネ改修",
        "耐震改修",
        "太陽光発電",
        "蓄電池補助",
        "給湯省エネ",
        "リフォーム一般",
        "起業・創業支援",
        "空き家対策",
        "移住支援",
        "EV・充電設備補助"
    ]

    # 市情報を収集
    all_cities = []
    for pref in pref_keys:
        cities = data[pref]
        for city_name, city_data in cities.items():
            if isinstance(city_data, dict) and '市ID' in city_data:
                city_id = city_data['市ID']
                all_cities.append({
                    'city_id': city_id,
                    'city_name': city_name,
                    'pref': pref,
                    'data': city_data
                })

    print(f"総市数: {len(all_cities)}")

    # 50市ループ
    created_files = []
    failed_files = []

    for city_info in all_cities:
        city_id = city_info['city_id']
        city_name = city_info['city_name']
        city_data = city_info['data']

        # 市用のディレクトリを作成
        city_dir = output_base / city_id
        city_dir.mkdir(parents=True, exist_ok=True)

        # 各ジャンルのファイルを作成
        for genre_name in genres:
            genre_slug = get_genre_slug(genre_name)
            filename = f"2026-06-09-{genre_slug}.md"
            filepath = city_dir / filename

            # JSONから補助金情報を取得
            # 主要補助金から該当ジャンルを探す
            target = None
            amount = None
            deadline = None
            source_url = None
            key_points = []

            if '主要補助金' in city_data and isinstance(city_data['主要補助金'], dict):
                # 主要補助金から該当データを探す
                for subsidy_name, subsidy_data in city_data['主要補助金'].items():
                    if isinstance(subsidy_data, dict):
                        # ジャンル名がサブシディ名に含まれているか確認
                        if genre_name in subsidy_name or genre_slug.split('-')[0] in subsidy_name.lower():
                            target = extract_data_value(subsidy_data, '対象')
                            amount = extract_data_value(subsidy_data, '補助額')
                            deadline = extract_data_value(subsidy_data, '申請期限') or extract_data_value(subsidy_data, '申請')
                            source_url = extract_data_value(subsidy_data, 'URL')

                            # key_pointsを構築
                            for key in ['条件', 'note', 'URL']:
                                value = extract_data_value(subsidy_data, key)
                                if value:
                                    key_points.append(value)
                            break

            # フロントマターを生成
            if target is None:
                target = "情報未取得"
            if amount is None:
                amount = "情報未取得"
            if deadline is None:
                deadline = "令和8年度内"
            if source_url is None:
                source_url = "https://example.com"

            summary = generate_summary(target if target != "情報未取得" else None,
                                     amount if amount != "情報未取得" else None)

            # ファイル内容を生成
            frontmatter = f"""---
title: "【2026年最新】{city_name}｜{genre_name}"
municipality: {city_id}
target: "{target}"
amount: "{amount}"
deadline: "{deadline}"
tags: ["補助金", "{genre_name}", "2026年"]
key_points:
  - "補助対象：{target}"
  - "補助額：{amount}"
  - "申請期限：{deadline}"
source_url: "{source_url}"
summary_ja: "{summary}"
scraped_at: "2026-06-09T00:00:00Z"
is_active: true
content_hash: "{city_id}-{genre_slug}-2026"
---

## {genre_name}について

本記事では、2026年の{city_name}における{genre_name}の補助金情報を詳しく解説します。

### 補助内容

**補助対象：** {target}

**補助額：** {amount}

**申請期限：** {deadline}

### ポイント

- 着工前の申請が必須です
- 市内施工業者の利用を条件とする場合があります
- 複数の補助制度を組み合わせて活用できる場合があります

### 詳細情報

詳細については以下のリンクから公式サイトをご確認ください。

[{city_name}公式サイト]({source_url})
"""

            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(frontmatter)
                created_files.append(str(filepath))
            except Exception as e:
                failed_files.append((str(filepath), str(e)))

        # 進捗を表示
        created_count = len([f for f in created_files if city_id in f])
        print(f"✓ {city_name} ({city_id}): {created_count}ファイル生成")

    # 結果を表示
    print(f"\n=== 生成完了 ===")
    print(f"総生成ファイル数: {len(created_files)}")
    print(f"失敗: {len(failed_files)}")

    if failed_files:
        print("\n失敗したファイル:")
        for filepath, error in failed_files:
            print(f"  {filepath}: {error}")

    # 結果をサマリーファイルに出力
    with open("markdown_generation_report.txt", 'w', encoding='utf-8') as f:
        f.write("Markdown File Generation Report\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Total files: {len(created_files)}\n")
        f.write(f"Failed: {len(failed_files)}\n\n")

        f.write("Generated files:\n")
        for filepath in sorted(created_files):
            f.write(f"  {filepath}\n")

        if failed_files:
            f.write("\nFailed files:\n")
            for filepath, error in failed_files:
                f.write(f"  {filepath}: {error}\n")

    print("\n生成レポートを保存しました: markdown_generation_report.txt")

if __name__ == "__main__":
    main()
