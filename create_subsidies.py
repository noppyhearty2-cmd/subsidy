#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 6-C 補助金ファイル一括作成スクリプト
北海道・東北・九州の9市のMarkdownファイルを自動生成
"""

import os
from datetime import datetime

# 補助金データ（Agent の検索結果から）
subsidies_data = {
    "asahikawa": {
        "name": "旭川市",
        "prefecture": "北海道",
        "subsidies": [
            {
                "genre": "solar",
                "filename": "2026-06-21-solar-suishin.md",
                "title": "【2026年最新】旭川市の太陽光発電補助金｜上限10万円",
                "amount": "上限10万円（予算500万円、住宅・事業所合計）",
                "deadline": "2026-08-31",
                "summary": "旭川市が太陽光発電設備の導入を支援。上限10万円で予算は500万円。予算枯渇のリスク高。",
                "url": "https://www.city.asahikawa.hokkaido.jp/kurashi/271/290/291/p005154.html",
                "key_points": [
                    "太陽光発電設備導入時の補助が最大10万円",
                    "住宅・事業所合わせて予算500万円の枠",
                    "予算枯渇で早期終了の可能性",
                    "2026年4月17日～8月31日申請受付"
                ],
                "details": "| 設備 | 補助額 |\n|------|--------|\n| **太陽光発電** | **上限10万円** |"
            },
            {
                "genre": "shoene",
                "filename": "2026-06-21-shoene-kaizen.md",
                "title": "【2026年最新】旭川市の住宅改修補助金｜窓・玄関・浴室・トイレ対応",
                "amount": "要確認（市補助）",
                "deadline": "要確認",
                "summary": "旭川市が省エネ型窓・玄関ドア・浴室・トイレ改修を補助。建築後15年以上経過の住宅が対象。",
                "url": "https://www.city.asahikawa.hokkaido.jp/kurashi/401/ho01/ho001/d053154.html",
                "key_points": [
                    "省エネ型窓・玄関ドアの改修が対象",
                    "浴室・トイレ等の省エネ改修も対象",
                    "建築後15年以上経過の住宅が対象",
                    "具体的な補助額は公式サイトで確認"
                ],
                "details": "| 対象工事 | 内容 |\n|--------|------|\n| **窓・玄関改修** | 省エネ型への交換 |\n| **浴室・トイレ** | 省エネ化工事 |"
            }
        ]
    },
    "hakodate": {
        "name": "函館市",
        "prefecture": "北海道",
        "subsidies": [
            {
                "genre": "solar",
                "filename": "2026-06-21-solar-system.md",
                "title": "【2026年最新】函館市の太陽光発電補助金｜新エネルギーシステム導入補助",
                "amount": "要確認",
                "deadline": "要確認",
                "summary": "函館市が太陽光発電システムの導入を支援。2026年度から家庭用燃料電池は対象外に変更。",
                "url": "https://www.city.hakodate.hokkaido.jp/docs/2020032700097/",
                "key_points": [
                    "太陽光発電システムの導入を支援",
                    "2026年度から家庭用燃料電池は対象外に変更",
                    "国・北海道補助金との併用不可",
                    "具体的な補助額は公式サイトで確認"
                ],
                "details": "| 設備 | 対象 | 備考 |\n|------|--------|--------|\n| **太陽光発電** | ◎ | 対象 |\n| **家庭用燃料電池** | ✗ | 2026年度から対象外 |"
            },
            {
                "genre": "chikudenchi",
                "filename": "2026-06-21-chikudenchi.md",
                "title": "【2026年最新】函館市の蓄電池補助金｜新エネルギーシステム導入補助",
                "amount": "要確認",
                "deadline": "要確認",
                "summary": "函館市が定置用リチウムイオン蓄電池の導入を支援。国・北海道補助金との併用不可。",
                "url": "https://www.city.hakodate.hokkaido.jp/docs/2020032700097/",
                "key_points": [
                    "定置用リチウムイオン蓄電池が対象",
                    "国・北海道補助金との併用不可",
                    "具体的な補助額は公式サイトで確認要"
                ],
                "details": "| 設備 | 支援内容 |\n|------|----------|\n| **定置用蓄電池** | 導入支援（補助額要確認） |"
            }
        ]
    },
    "yamagata": {
        "name": "山形市",
        "prefecture": "山形県",
        "subsidies": [
            {
                "genre": "solar",
                "filename": "2026-06-21-solar-jikashouhi.md",
                "title": "【2026年最新】山形市の自家消費型太陽光発電補助｜14.1万円/kWh × 1/3",
                "amount": "14.1万円/kWh × 1/3（自家消費型）",
                "deadline": "2026-12-25",
                "summary": "山形市が自家消費型太陽光発電を支援。他の補助金との併用不可。",
                "url": "https://www.city.yamagata-yamagata.lg.jp/kurashi/kankyohozen/1006528/1013800/1014661.html",
                "key_points": [
                    "自家消費型太陽光発電が対象",
                    "14.1万円/kWh × 1/3の補助率",
                    "他の補助金との併用不可",
                    "申請期限：2026年12月25日"
                ],
                "details": "| 対象 | 補助額 |\n|------|--------|\n| **自家消費型太陽光** | **14.1万円/kWh × 1/3** |"
            },
            {
                "genre": "chikudenchi",
                "filename": "2026-06-21-chikudenchi-kaichoku.md",
                "title": "【2026年最新】山形市の蓄電池補助｜最大63万円に拡充（自家消費型）",
                "amount": "14.1万円/kWh × 1/3（上限63万円に拡充）",
                "deadline": "2026-12-25",
                "summary": "山形市が蓄電池補助を2026年度から最大63万円に大幅拡充。太陽光発電と同時設置が条件。",
                "url": "https://www.city.yamagata-yamagata.lg.jp/kurashi/kankyohozen/1006528/1013800/1014661.html",
                "key_points": [
                    "2026年度から最大63万円に拡充（大幅改善）",
                    "太陽光発電設備と同時設置が必須条件",
                    "20kWh以下が対象",
                    "14.1万円/kWh × 1/3の補助率で計算",
                    "申請期限：2026年12月25日"
                ],
                "details": "| 容量 | 補助額 |\n|------|--------|\n| **20kWh以下** | **14.1万円/kWh × 1/3（上限63万円）** |"
            }
        ]
    },
    "fukushima": {
        "name": "福島市",
        "prefecture": "福島県",
        "subsidies": [
            {
                "genre": "solar",
                "filename": "2026-06-21-solar-dakutanso.md",
                "title": "【2026年最新】福島市の太陽光発電補助｜2.5万円/kW（上限10万円）",
                "amount": "2.5万円/kW（上限10万円）",
                "deadline": "要確認",
                "summary": "福島市が太陽光発電導入を補助。福島県補助金との併用可能で、複合補助を活用可能。",
                "url": "https://www.city.fukushima.fukushima.jp/soshiki/8/1034/1/4741.html",
                "key_points": [
                    "1kWあたり2.5万円の補助",
                    "上限10万円",
                    "福島県の補助金との併用可能",
                    "市補助と県補助を組み合わせて最大効果を実現"
                ],
                "details": "| 設備 | 補助額 |\n|------|--------|\n| **太陽光発電** | **2.5万円/kW（上限10万円）** |"
            },
            {
                "genre": "chikudenchi",
                "filename": "2026-06-21-chikudenchi-dakutanso.md",
                "title": "【2026年最新】福島市の蓄電池補助｜1万円/kWh（上限10万円）",
                "amount": "1万円/kWh（上限10万円）",
                "deadline": "要確認",
                "summary": "福島市が蓄電池導入を補助。太陽光発電と連系が条件。市補助と福島県補助の併用で効果的。",
                "url": "https://www.city.fukushima.fukushima.jp/soshiki/8/1034/1/4741.html",
                "key_points": [
                    "1kWhあたり1万円の補助",
                    "上限10万円",
                    "太陽光発電と連系が条件",
                    "福島県補助金との併用可能で複合効果を実現"
                ],
                "details": "| 設備 | 補助額 |\n|------|--------|\n| **蓄電池（太陽光連系）** | **1万円/kWh（上限10万円）** |"
            }
        ]
    },
    "saga": {
        "name": "佐賀市",
        "prefecture": "佐賀県",
        "subsidies": [
            {
                "genre": "solar",
                "filename": "2026-06-21-solar-zerocabon.md",
                "title": "【2026年最新】佐賀市のゼロカーボン推進事業補助｜太陽光発電",
                "amount": "要確認",
                "deadline": "2027-02-27",
                "summary": "佐賀市がゼロカーボン推進事業で太陽光発電導入を補助。受付件数超過時は抽選。",
                "url": "https://www.city.saga.lg.jp",
                "key_points": [
                    "佐賀市ゼロカーボン推進事業費補助金",
                    "受付期間：2025年5月12日～2027年2月27日",
                    "受付件数超過時は抽選になる可能性あり",
                    "太陽光発電導入を支援"
                ],
                "details": "| 対象 | 申請期間 | 備考 |\n|------|-------------|--------|\n| **太陽光発電** | 2025/5/12～2027/2/27 | 受付件数超過時は抽選 |"
            },
            {
                "genre": "chikudenchi",
                "filename": "2026-06-21-chikudenchi-zerocabon.md",
                "title": "【2026年最新】佐賀市の蓄電池補助｜補助対象経費×1/3（上限47万円）",
                "amount": "補助対象経費 × 1/3（上限47万円/4.7万円kWh相当）",
                "deadline": "2027-02-27",
                "summary": "佐賀市がゼロカーボン推進事業で蓄電池導入を補助。太陽光発電と同時新設時に対象。",
                "url": "https://www.city.saga.lg.jp",
                "key_points": [
                    "補助対象経費の1/3を補助",
                    "上限47万円（4.7万円/kWh相当）",
                    "太陽光発電と同時新設時のみ対象",
                    "単独導入は対象外"
                ],
                "details": "| 対象 | 補助率 | 上限 |\n|------|--------|------|\n| **蓄電池（太陽光セット）** | **1/3** | **47万円** |"
            }
        ]
    },
    "nagasaki": {
        "name": "長崎市",
        "prefecture": "長崎県",
        "subsidies": [
            {
                "genre": "solar",
                "filename": "2026-06-21-solar-jikashouhi.md",
                "title": "【2026年最新】長崎市の太陽光発電補助｜自家消費型（FIT非認定）",
                "amount": "要確認",
                "deadline": "2026-11-30",
                "summary": "長崎市が自家消費型太陽光発電設備導入を補助。FIT・FIP非認定の設備が対象。",
                "url": "https://www.city.nagasaki.lg.jp/page/52113.html",
                "key_points": [
                    "自家消費型太陽光発電が対象",
                    "FIT・FIP非認定設備が対象条件",
                    "1kW以上10kW未満が対象規模",
                    "申請期限：2026年11月30日"
                ],
                "details": "| 対象 | 条件 |\n|------|--------|\n| **自家消費型太陽光** | **FIT・FIP非認定** |"
            },
            {
                "genre": "chikudenchi",
                "filename": "2026-06-21-chikudenchi-jikashouhi.md",
                "title": "【2026年最新】長崎市の蓄電池補助｜太陽光発電セット条件",
                "amount": "要確認（太陽光セット時のみ）",
                "deadline": "2026-11-30",
                "summary": "長崎市が蓄電池導入を補助。太陽光発電設備との同時設置が条件。単独導入は対象外。",
                "url": "https://www.city.nagasaki.lg.jp/page/52110.html",
                "key_points": [
                    "太陽光発電設備との同時設置が必須条件",
                    "単独導入は対象外",
                    "申請期限：2026年11月30日",
                    "具体的な補助額は公式サイトで確認"
                ],
                "details": "| 対象 | 条件 |\n|------|--------|\n| **蓄電池** | **太陽光セット必須** |"
            }
        ]
    }
}

def create_markdown_file(city_id, city_name, prefecture, subsidy_data):
    """Markdownファイルを作成する"""

    base_path = f"C:\\Users\\noppy\\Subsidy\\site\\src\\content\\subsidies\\{city_id}"

    # ディレクトリが存在しない場合は作成
    os.makedirs(base_path, exist_ok=True)

    filename = os.path.join(base_path, subsidy_data["filename"])

    # ジャンルに応じたテンプレート
    genre = subsidy_data["genre"]

    # フロントマター
    frontmatter = f"""---
title: "{subsidy_data['title']}"
municipality: {city_id}
target: {city_name}内の住宅で補助対象設備を導入する個人
amount: "{subsidy_data['amount']}"
deadline: "{subsidy_data['deadline']}"
tags:
  - 補助金
  - 太陽光発電 if genre == 'solar' else 蓄電池
  - 省エネ
  - {prefecture}
key_points:
"""

    # キーポイントを追加
    for point in subsidy_data["key_points"]:
        frontmatter += f'  - "{point}"\n'

    frontmatter += f"""source_url: "{subsidy_data['url']}"
summary_ja: {subsidy_data['summary']}
scraped_at: "{datetime.now().isoformat()}Z"
is_active: true
content_hash: "{city_id}-{genre}-2026"
---

## こんな人が対象

{city_name}内の住宅で補助対象設備を導入する個人・世帯

## いくらもらえる？

{subsidy_data['details']}

## いつまでに申し込む？

- **申請期間**: {subsidy_data['deadline']}まで
- ※詳細は市公式サイトで確認

## 申し込みのポイント

1. **最新情報は必ず公式サイトで確認** — 補助額や期間が変更されている場合があります
2. **事前相談推奨** — 補助対象や条件について市役所に相談
3. **複数補助の組み合わせを検討** — 市補助+県補助+国補助の併用で最大効果を実現

## 詳細・申し込みはこちら

**{city_name}公式サイト**
[補助金情報]({subsidy_data['url']})

詳細は市の公式ウェブサイトでご確認ください。

---

*本ページの情報は2026年6月時点の公開情報をもとに作成しています。最新の情報は市役所の公式ページでご確認ください。*
"""

    return filename, frontmatter

# ファイル作成
print("Phase 6-C 補助金ファイル一括作成を開始します...\n")

created_files = []
for city_id, city_data in subsidies_data.items():
    city_name = city_data["name"]
    prefecture = city_data["prefecture"]

    print(f"【{city_name}({prefecture})】")

    for subsidy in city_data["subsidies"]:
        filename, content = create_markdown_file(city_id, city_name, prefecture, subsidy)

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)

            print(f"  ✓ {subsidy['filename']} を作成しました")
            created_files.append(filename)
        except Exception as e:
            print(f"  ✗ エラー: {e}")

    print()

print(f"\n【作成完了】")
print(f"作成ファイル数: {len(created_files)}個")
print(f"\n作成ファイル一覧:")
for f in created_files:
    print(f"  - {f}")
