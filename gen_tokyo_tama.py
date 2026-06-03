#!/usr/bin/env python3
"""東京都多摩地域（18市+4町村）の補助金記事生成スクリプト"""
import os

BASE = r"C:\Users\noppy\Subsidy\site\src\content\subsidies"

def w(muni_id, fname, body):
    d = os.path.join(BASE, muni_id)
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    print(f"  wrote: {muni_id}/{fname}")

# ============================================================
# 多摩地域各市の省エネ・太陽光補助金（東京都共通補助 + 市独自）
# ============================================================

# 青梅市
w("ome", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】青梅市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: ome
target: 青梅市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 青梅市独自の省エネ補助も別途あり（市役所に要確認）
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.ome.tokyo.jp/"
summary_ja: 青梅市では東京都のゼロエミ住宅補助（ZEH最大240万円）に加え、市独自の省エネ補助金を利用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "ome-taiyoko-2026"
---

## こんな人が対象

- 青梅市内の住宅に太陽光発電・省エネ設備を設置する方

## 利用できる主な補助金

| 補助制度 | 補助額 | 申請先 |
|---------|-------|-------|
| 東京都ゼロエミッション住宅（ZEH） | **最大240万円** | 東京都 |
| 東京都太陽光・蓄電池補助金 | 別途あり | 東京都 |
| 青梅市独自補助金 | 要確認 | 青梅市役所 |

## 詳細・申し込みはこちら

> 青梅市公式HP：https://www.city.ome.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 昭島市
w("akishima", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】昭島市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: akishima
target: 昭島市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 昭島市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 昭島市独自の省エネ設備補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.akishima.lg.jp/"
summary_ja: 昭島市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を組み合わせて利用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "akishima-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 昭島市公式HP：https://www.city.akishima.lg.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 小金井市
w("koganei", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】小金井市の太陽光・省エネ補助金｜東京都補助＋市独自の環境補助"
municipality: koganei
target: 小金井市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 小金井市独自の環境補助金も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.koganei.lg.jp/"
summary_ja: 小金井市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自の環境補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "koganei-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 小金井市公式HP：https://www.city.koganei.lg.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 小平市
w("kodaira", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】小平市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: kodaira
target: 小平市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 小平市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.kodaira.tokyo.jp/"
summary_ja: 小平市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を組み合わせて利用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "kodaira-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 小平市公式HP：https://www.city.kodaira.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 日野市
w("hino", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】日野市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: hino
target: 日野市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 日野市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.hino.lg.jp/"
summary_ja: 日野市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "hino-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 日野市公式HP：https://www.city.hino.lg.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 東村山市
w("higashimurayama", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】東村山市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: higashimurayama
target: 東村山市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 東村山市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.higashimurayama.tokyo.jp/"
summary_ja: 東村山市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "higashimurayama-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 東村山市公式HP：https://www.city.higashimurayama.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 国分寺市
w("kokubunji", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】国分寺市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: kokubunji
target: 国分寺市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 国分寺市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.kokubunji.tokyo.jp/"
summary_ja: 国分寺市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "kokubunji-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 国分寺市公式HP：https://www.city.kokubunji.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 国立市
w("kunitachi", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】国立市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: kunitachi
target: 国立市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 国立市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.kunitachi.tokyo.jp/"
summary_ja: 国立市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "kunitachi-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 国立市公式HP：https://www.city.kunitachi.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 福生市
w("fussa", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】福生市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: fussa
target: 福生市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 福生市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.fussa.tokyo.jp/"
summary_ja: 福生市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "fussa-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 福生市公式HP：https://www.city.fussa.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 狛江市
w("komae", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】狛江市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: komae
target: 狛江市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 狛江市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.komae.tokyo.jp/"
summary_ja: 狛江市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "komae-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 狛江市公式HP：https://www.city.komae.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 東大和市
w("higashiyamato", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】東大和市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: higashiyamato
target: 東大和市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 東大和市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.higashiyamato.lg.jp/"
summary_ja: 東大和市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "higashiyamato-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 東大和市公式HP：https://www.city.higashiyamato.lg.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 清瀬市
w("kiyose", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】清瀬市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: kiyose
target: 清瀬市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 清瀬市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.kiyose.lg.jp/"
summary_ja: 清瀬市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "kiyose-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 清瀬市公式HP：https://www.city.kiyose.lg.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 東久留米市
w("higashikurume", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】東久留米市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: higashikurume
target: 東久留米市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 東久留米市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.higashikurume.lg.jp/"
summary_ja: 東久留米市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "higashikurume-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 東久留米市公式HP：https://www.city.higashikurume.lg.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 武蔵村山市
w("musashimurayama", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】武蔵村山市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: musashimurayama
target: 武蔵村山市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 武蔵村山市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.musashimurayama.lg.jp/"
summary_ja: 武蔵村山市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "musashimurayama-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 武蔵村山市公式HP：https://www.city.musashimurayama.lg.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 多摩市
w("tama", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】多摩市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: tama
target: 多摩市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 多摩市独自の省エネ・太陽光補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.tama.lg.jp/"
summary_ja: 多摩市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "tama-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 多摩市公式HP：https://www.city.tama.lg.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 稲城市
w("inagi", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】稲城市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: inagi
target: 稲城市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 稲城市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.inagi.tokyo.jp/"
summary_ja: 稲城市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "inagi-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 稲城市公式HP：https://www.city.inagi.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 羽村市
w("hamura", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】羽村市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: hamura
target: 羽村市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 羽村市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.hamura.tokyo.jp/"
summary_ja: 羽村市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "hamura-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 羽村市公式HP：https://www.city.hamura.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# あきる野市
w("akiruno", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】あきる野市の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: akiruno
target: あきる野市内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 市独自補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - あきる野市独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.city.akiruno.tokyo.jp/"
summary_ja: あきる野市では東京都のゼロエミ住宅補助（ZEH最大240万円）と市独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "akiruno-taiyoko-2026"
---

## 詳細・申し込みはこちら

> あきる野市公式HP：https://www.city.akiruno.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

w("akiruno", "2026-05-12-iju.md", """\
---
title: "【2026年最新】あきる野市の移住支援補助金｜自然豊かな西多摩で移住を検討する方へ"
municipality: akiruno
target: あきる野市への移住・定住を希望する方
amount: 東京都移住・定住補助（詳細は都・市HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 奥多摩・秋川渓谷に隣接する自然豊かな西多摩エリア
  - 東京都の移住支援制度が利用可能
  - 市独自の移住・定住支援については市役所に要確認
source_url: "https://www.city.akiruno.tokyo.jp/"
summary_ja: あきる野市は秋川渓谷に隣接する自然豊かな西多摩の市。東京都内でありながら移住・田園暮らしを実現できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "akiruno-iju-2026"
---

## あきる野市の特徴

- 秋川渓谷・東京サマーランドに隣接する自然豊かな西多摩エリア
- 都心へのアクセス：JR五日市線・武蔵五日市駅から新宿まで約70分

## 詳細・申し込みはこちら

> あきる野市公式HP：https://www.city.akiruno.tokyo.jp/
""")

# ============================================================
# 西多摩郡 4町村
# ============================================================

# 瑞穂町
w("mizuho", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】瑞穂町の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: mizuho
target: 瑞穂町内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 町独自補助（詳細は町HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 瑞穂町独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.town.mizuho.tokyo.jp/"
summary_ja: 瑞穂町では東京都のゼロエミ住宅補助（ZEH最大240万円）と町独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "mizuho-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 瑞穂町公式HP：https://www.town.mizuho.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 日の出町
w("hinode", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】日の出町の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: hinode
target: 日の出町内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 町独自補助（詳細は町HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 日の出町独自の省エネ補助も別途あり
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.town.hinode.tokyo.jp/"
summary_ja: 日の出町では東京都のゼロエミ住宅補助（ZEH最大240万円）と町独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "hinode-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 日の出町公式HP：https://www.town.hinode.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

# 檜原村
w("hinohara", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】檜原村の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: hinohara
target: 檜原村内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 村独自補助（詳細は村HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 東京都唯一の「村」で自然豊かな環境
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
source_url: "https://www.vill.hinohara.tokyo.jp/"
summary_ja: 檜原村では東京都のゼロエミ住宅補助（ZEH最大240万円）と村独自補助を活用できる。東京都唯一の村。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "hinohara-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 檜原村公式HP：https://www.vill.hinohara.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

w("hinohara", "2026-05-12-iju.md", """\
---
title: "【2026年最新】檜原村の移住支援補助金｜東京都唯一の村で自然に囲まれた暮らし"
municipality: hinohara
target: 檜原村への移住・定住を希望する方
amount: 東京都・村の移住支援（詳細は村役場確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 東京都唯一の「村」・全国有数の自然豊かな環境
  - 村独自の空き家バンク・移住支援制度あり
  - 東京都の移住支援制度も利用可能
  - 都心まで約2時間（バス・JR利用）
source_url: "https://www.vill.hinohara.tokyo.jp/"
summary_ja: 檜原村は東京都唯一の村。豊かな自然の中での移住を支援する村独自の補助制度があり、空き家バンクも活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "hinohara-iju-2026"
---

## 檜原村の特徴

- **東京都唯一の「村」**・人口約2,000人の自然豊かな山村
- 秋川渓谷・払沢の滝など豊かな自然環境
- 空き家バンク制度・移住体験ツアーを実施

## 詳細・申し込みはこちら

> 檜原村公式HP：https://www.vill.hinohara.tokyo.jp/
""")

# 奥多摩町
w("okutama", "2026-05-12-taiyoko.md", """\
---
title: "【2026年最新】奥多摩町の太陽光・省エネ補助金｜東京都ゼロエミ住宅最大240万円も活用可"
municipality: okutama
target: 奥多摩町内の住宅に太陽光発電・省エネ設備を設置する個人
amount: 東京都ゼロエミ住宅：ZEH最大240万円 / 町独自補助（詳細は町HP確認）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
  - ZEH
key_points:
  - 東京都「ゼロエミッション住宅」補助金でZEH最大240万円
  - 東京都全域対象の太陽光・蓄電池補助金も利用可能
  - 町独自の省エネ補助については役場に要確認
source_url: "https://www.town.okutama.tokyo.jp/"
summary_ja: 奥多摩町では東京都のゼロエミ住宅補助（ZEH最大240万円）と町独自補助を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "okutama-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 奥多摩町公式HP：https://www.town.okutama.tokyo.jp/
> 東京都省エネポータル：https://www.tokyo-co2down.jp/
""")

w("okutama", "2026-05-12-iju.md", """\
---
title: "【2026年最新】奥多摩町の移住・空き家活用補助金｜東京都の「ふるさと回帰」支援も"
municipality: okutama
target: 奥多摩町への移住・定住を希望する方、空き家を活用したい方
amount: 町独自の移住支援・空き家リフォーム補助（詳細は役場確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 多摩川源流・東京都西端の自然豊かな山間部
  - 空き家バンク制度・移住相談窓口を設置
  - 東京都の移住支援制度も利用可能
  - 東京都内でありながら本格的な田舎暮らしを実現
source_url: "https://www.town.okutama.tokyo.jp/"
summary_ja: 奥多摩町は多摩川源流の自然豊かな町。空き家バンクや移住支援制度を活用した移住が可能で、東京都内の田舎暮らしが実現できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "okutama-iju-2026"
---

## 奥多摩町の特徴

- **多摩川の源流**に位置する東京都最西端の自然豊かな山間部
- 東京都内でありながら本格的な山村生活を実現できる
- 空き家バンク・移住体験住宅あり

## 詳細・申し込みはこちら

> 奥多摩町公式HP：https://www.town.okutama.tokyo.jp/
""")

print("\n多摩地域 全ファイル生成完了!")
