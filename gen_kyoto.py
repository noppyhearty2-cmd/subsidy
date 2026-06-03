#!/usr/bin/env python3
"""京都府全市町村の補助金記事を生成するスクリプト"""
import os

BASE = r"C:\Users\noppy\Subsidy\site\src\content\subsidies"

def w(muni_id: str, fname: str, body: str):
    d = os.path.join(BASE, muni_id)
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    print(f"  wrote: {muni_id}/{fname}")

# ============================================================
# 京都府レベル
# ============================================================
w("kyoto_pref", "2026-05-11-taiyoko-chikudenchi.md", """\
---
title: "【2026年最新】京都府の住宅用太陽光・蓄電池補助金｜非FIT型は2倍の補助額"
municipality: kyoto_pref
target: 京都府内の住宅に太陽光発電・蓄電池を設置する個人
amount: 太陽光1万円/kW（上限4万円）、蓄電池1.5万円/kWh（上限9万円）、非FIT型は約2倍
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
  - 再エネ
key_points:
  - 太陽光+蓄電池の同時導入が必要条件
  - 非FIT型（自家消費型）はFIT型の約2倍の補助額
  - 太陽光FIT: 1万円/kW（上限4万円）、非FIT: 2万円/kW（上限8万円）
  - 蓄電池FIT: 1.5万円/kWh（上限9万円）、非FIT: 3万円/kWh（上限18万円）
  - 2025年4月から府内新築戸建ての太陽光設置が義務化
source_url: "https://www.pref.kyoto.jp/energy/kateimukehojo.html"
summary_ja: 京都府の家庭向け太陽光・蓄電池補助金。FIT型と非FIT型（自家消費型）で補助額が異なり、非FIT型は2倍の補助を受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kyoto_pref-taiyoko-2026"
---

## こんな人が対象

- 京都府内の住宅（戸建・共同住宅）に**太陽光+蓄電池を同時設置**する方
- FIT型・非FIT型（自家消費型）どちらも対象

## いくらもらえる？

| 設備 | FIT型 | 非FIT型（自家消費） |
|------|-------|-----------------|
| 太陽光発電 | **1万円/kW（上限4万円）** | **2万円/kW（上限8万円）** |
| 蓄電池 | **1.5万円/kWh（上限9万円）** | **3万円/kWh（上限18万円）** |

## ポイント

- **非FIT型（自家消費型）**は補助額が約2倍に！
- 太陽光+蓄電池の**同時導入が必須条件**
- 各市町村の補助金との**上乗せ併用**が可能な場合あり

## 詳細・申し込みはこちら

> 公式ページ：https://www.pref.kyoto.jp/energy/kateimukehojo.html
> ※最新情報は必ず公式ページでご確認ください。
""")

w("kyoto_pref", "2026-05-11-iju-shien.md", """\
---
title: "【2026年最新】京都府の移住支援金｜最大100万円＋子ども加算100万円/人"
municipality: kyoto_pref
target: 東京圏から京都府内の対象市町村（農村部）へ移住する方
amount: 世帯100万円、単身60万円、18歳未満の子1人あたり最大100万円加算
deadline: "2027-03-31"
tags:
  - 移住
  - 子育て
key_points:
  - 東京圏からの移住者に最大100万円（世帯）＋子ども加算
  - 18歳未満の子ども1人につき最大100万円を加算
  - 対象市町村：京丹後市・宮津市・舞鶴市・綾部市・京丹波町・南丹市・亀岡市・木津川市など
  - 就業または起業が条件
source_url: "https://www.pref.kyoto.jp/jobpark/ijusienkin.html"
summary_ja: 京都府の移住支援金。東京圏から府内農村部へ移住する世帯に最大100万円、さらに子ども1人あたり最大100万円を加算。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kyoto_pref-iju-2026"
---

## こんな人が対象

- **東京圏**（東京都・埼玉県・千葉県・神奈川県）から京都府内の対象市町村に転入した方
- 就業または起業が条件

## いくらもらえる？

| 区分 | 金額 |
|------|------|
| 世帯での移住 | **100万円** |
| 単身での移住 | **60万円** |
| 18歳未満の子ども（1人あたり） | **最大100万円加算** |

## 対象市町村

京丹後市・宮津市・舞鶴市・綾部市・京丹波町・南丹市・亀岡市・宇治田原町・井手町・和束町・木津川市など

## 詳細・申し込みはこちら

> 公式ページ：https://www.pref.kyoto.jp/jobpark/ijusienkin.html
""")

w("kyoto_pref", "2026-05-11-sogyo.md", """\
---
title: "【2026年最新】京都府の起業・創業支援補助金｜補助率1/2・上限200万円"
municipality: kyoto_pref
target: 京都府内で起業・創業する個人・中小企業
amount: 補助率1/2・上限200万円
deadline: "2027-03-31"
tags:
  - 創業
  - 中小企業
key_points:
  - 起業支援事業費補助金：補助率1/2・上限200万円
  - 経営基盤強化推進事業費補助金（賃上げ原資となる経費削減事業向け）
source_url: "https://www.pref.kyoto.jp/sangyo-sinko/startup/index.html"
summary_ja: 京都府の起業・創業支援補助金。補助率1/2・上限200万円で、新規創業から経営基盤強化まで幅広く支援。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kyoto_pref-sogyo-2026"
---

## こんな人が対象

- 京都府内で起業・創業する個人・法人
- 経営基盤強化を目指す中小企業

## いくらもらえる？

| 補助金名 | 補助率 | 上限 |
|---------|-------|------|
| 起業支援事業費補助金 | **1/2** | **200万円** |
| 経営基盤強化推進事業費補助金 | ― | 要確認 |

## 詳細・申し込みはこちら

> 公式ページ：https://www.pref.kyoto.jp/sangyo-sinko/startup/index.html
""")

# ============================================================
# 京都市
# ============================================================
w("kyoto", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】京都市の太陽光・蓄電池補助金｜さんさんポイント20万ポイント"
municipality: kyoto
target: 京都市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 太陽光+蓄電池同時設置でさんさんポイント20万ポイント、蓄電池のみ10万ポイント
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 「京都再エネクラブ」経由で設置するとポイント付与
  - 非FIT型（自家消費型）太陽光+蓄電池同時設置で20万ポイント
  - 蓄電池のみ設置でも10万ポイント付与
  - さんさんポイントは地域商店等で利用可能
  - 建築物（戸建除く）向け上乗せ促進補助金も別途あり
source_url: "https://www.city.kyoto.lg.jp/kankyo/page/0000281917.html"
summary_ja: 京都市の「京都再エネクラブ」経由での太陽光+蓄電池設置で最大20万さんさんポイント付与。地域で使える独自ポイント制度。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kyoto-taiyoko-2026"
---

## こんな人が対象

- 京都市内の住宅に太陽光発電・蓄電池を新規設置する方
- 「京都再エネクラブ」経由での設置が条件

## いくらもらえる？

| 設置内容 | 付与ポイント |
|---------|------------|
| 太陽光+蓄電池（非FIT型）同時設置 | **さんさんポイント20万ポイント** |
| 蓄電池のみ設置 | **さんさんポイント10万ポイント** |

さんさんポイントは京都市内の対象店舗・施設で利用可能。

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.kyoto.lg.jp/kankyo/page/0000281917.html
> 京都再エネクラブ：https://www.city.kyoto.lg.jp/kankyo/page/0000324700.html
""")

w("kyoto", "2026-05-11-kosodatesumai.md", """\
---
title: "【2026年最新】京都市の子育て世帯住宅取得補助金｜最大200万円"
municipality: kyoto
target: 京都市内で既存住宅を取得する子育て世帯
amount: 基本100万円＋加算で最大200万円
deadline: "2027-12-31"
tags:
  - 子育て
  - 住宅
  - 移住
key_points:
  - 制度名：京都安心すまい応援金（子育て世帯既存住宅取得応援金）
  - 基本補助100万円
  - 子ども2人以上・市外からの転入等の条件で最大50万円加算（上限200万円）
  - 申請期間：令和8年4月1日〜令和9年12月31日
source_url: "https://miyakoanshinsumai.com/kosodatesumai/"
summary_ja: 京都市の子育て世帯向け住宅取得補助。既存住宅取得で基本100万円、条件により最大200万円まで加算される。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kyoto-kosodatesumai-2026"
---

## こんな人が対象

- 京都市内で**既存住宅を取得**する子育て世帯
- 市外からの転入者は加算対象

## いくらもらえる？

| 区分 | 金額 |
|------|------|
| 基本補助 | **100万円** |
| 子ども2人以上・市外転入等による加算 | **最大+100万円（上限200万円）** |

## 申請期間

令和8年4月1日〜令和9年12月31日

## 詳細・申し込みはこちら

> 公式ページ：https://miyakoanshinsumai.com/kosodatesumai/
""")

# ============================================================
# 福知山市
# ============================================================
w("fukuchiyama", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】福知山市の太陽光・蓄電池補助金｜非FIT型は2万円/kW・上限8万円"
municipality: fukuchiyama
target: 福知山市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 非FIT型：太陽光2万円/kW（上限8万円）、蓄電池3万円/kWh（上限18万円）+2万円
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 非FIT型（自家消費型）：太陽光2万円/kW（上限8万円）
  - 非FIT型蓄電池：3万円/kWh（上限18万円）+定額2万円
  - FIT型：太陽光1万円/kW（上限4万円）、蓄電池1.5万円/kWh（上限9万円）
  - 京都府補助金との上乗せ併用で更なる補助も可能
source_url: "https://www.city.fukuchiyama.lg.jp/soshiki/72/22945.html"
summary_ja: 福知山市の太陽光・蓄電池補助金。非FIT型（自家消費型）なら太陽光2万円/kW・蓄電池3万円/kWhと手厚い補助を受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "fukuchiyama-taiyoko-2026"
---

## いくらもらえる？

| 設備 | FIT型 | 非FIT型（自家消費） |
|------|-------|-----------------|
| 太陽光発電 | 1万円/kW（上限4万円） | **2万円/kW（上限8万円）** |
| 蓄電池 | 1.5万円/kWh（上限9万円） | **3万円/kWh（上限18万円）+2万円** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.fukuchiyama.lg.jp/soshiki/72/22945.html
""")

w("fukuchiyama", "2026-05-11-iju.md", """\
---
title: "【2026年最新】福知山市の移住支援金｜京都府支援金100万円＋市独自補助"
municipality: fukuchiyama
target: 東京圏から福知山市へ移住する方
amount: 世帯100万円・単身60万円（京都府移住支援金）＋市独自補助あり
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 京都府移住支援金の対象市
  - 世帯移住100万円・単身移住60万円
  - 市独自の移住促進事業補助金も別途あり
source_url: "https://www.city.fukuchiyama.lg.jp/"
summary_ja: 福知山市は京都府移住支援金（世帯100万円・単身60万円）の対象市。市独自補助も利用可能。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "fukuchiyama-iju-2026"
---

## いくらもらえる？

| 区分 | 金額 |
|------|------|
| 世帯での移住 | **100万円**（京都府支援金） |
| 単身での移住 | **60万円**（京都府支援金） |
| 市独自補助 | 別途あり（市役所にご確認ください） |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.fukuchiyama.lg.jp/
> 京都府移住支援金：https://www.pref.kyoto.jp/jobpark/ijusienkin.html
""")

# ============================================================
# 舞鶴市
# ============================================================
w("maizuru", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】舞鶴市の太陽光・蓄電池補助金｜非FIT型2万円/kW・上限8万円"
municipality: maizuru
target: 舞鶴市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 非FIT型：太陽光2万円/kW（上限8万円）、蓄電池3万円/kWh（上限18万円）+1万円
deadline: "2026-01-16"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 令和7年度申請期間：令和7年5月12日〜令和8年1月16日
  - 非FIT型：太陽光2万円/kW（上限8万円）、蓄電池3万円/kWh（上限18万円）+1万円
  - FIT型：太陽光1万円/kW（上限4万円）、蓄電池1.5万円/kWh（上限9万円）
source_url: "https://www.city.maizuru.kyoto.jp/kurashi/0000013719.html"
summary_ja: 舞鶴市の太陽光・蓄電池補助金。非FIT型なら太陽光2万円/kW・蓄電池最大19万円の補助を受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "maizuru-taiyoko-2026"
---

## いくらもらえる？

| 設備 | FIT型 | 非FIT型（自家消費） |
|------|-------|-----------------|
| 太陽光発電 | 1万円/kW（上限4万円） | **2万円/kW（上限8万円）** |
| 蓄電池 | 1.5万円/kWh（上限9万円） | **3万円/kWh（上限18万円）+1万円** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.maizuru.kyoto.jp/kurashi/0000013719.html
""")

w("maizuru", "2026-05-11-iju.md", """\
---
title: "【2026年最新】舞鶴市の移住支援金｜世帯100万円・子ども加算あり"
municipality: maizuru
target: 東京圏から舞鶴市へ移住する方
amount: 世帯100万円・単身60万円＋子ども加算
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 京都府移住支援金の対象市
  - 世帯移住100万円・単身60万円
  - 18歳未満の子ども1人あたり最大100万円加算
source_url: "https://www.city.maizuru.kyoto.jp/"
summary_ja: 舞鶴市は京都府移住支援金の対象市。東京圏からの移住で世帯100万円・子ども加算も受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "maizuru-iju-2026"
---

## いくらもらえる？

| 区分 | 金額 |
|------|------|
| 世帯での移住 | **100万円** |
| 単身での移住 | **60万円** |
| 18歳未満の子ども（1人あたり） | **最大100万円加算** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.maizuru.kyoto.jp/
> 京都府移住支援金：https://www.pref.kyoto.jp/jobpark/ijusienkin.html
""")

# ============================================================
# 綾部市
# ============================================================
w("ayabe", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】綾部市の太陽光・蓄電池補助金｜太陽光1.5万円/kW・蓄電池1万円/kWh"
municipality: ayabe
target: 綾部市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 太陽光1.5万円/kW（上限6万円）、蓄電池1万円/kWh（上限6万円）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 太陽光：1.5万円/kW（上限6万円）
  - 蓄電池：1万円/kWh（上限6万円）
  - 申請：令和7年5月22日〜予算満了まで（先着順）
source_url: "https://www.city.ayabe.lg.jp/"
summary_ja: 綾部市の太陽光・蓄電池補助金。太陽光1.5万円/kW（上限6万円）、蓄電池1万円/kWh（上限6万円）。先着順。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "ayabe-taiyoko-2026"
---

## いくらもらえる？

| 設備 | 補助額 | 上限 |
|------|-------|------|
| 太陽光発電 | **1.5万円/kW** | **6万円** |
| 蓄電池 | **1万円/kWh** | **6万円** |

## 注意点

- **先着順・予算満了で受付終了**（令和7年5月22日〜）

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.ayabe.lg.jp/
""")

w("ayabe", "2026-05-11-iju.md", """\
---
title: "【2026年最新】綾部市の移住支援金｜世帯100万円・京都府支援金対象"
municipality: ayabe
target: 東京圏から綾部市へ移住する方
amount: 世帯100万円・単身60万円＋子ども加算
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 京都府移住支援金の対象市
  - 世帯移住100万円・単身60万円
source_url: "https://www.city.ayabe.lg.jp/"
summary_ja: 綾部市は京都府移住支援金の対象市。東京圏からの移住で世帯100万円・子ども加算も受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "ayabe-iju-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.ayabe.lg.jp/
> 京都府移住支援金：https://www.pref.kyoto.jp/jobpark/ijusienkin.html
""")

# ============================================================
# 宇治市
# ============================================================
w("uji", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】宇治市の太陽光・蓄電池補助金｜非FIT型2万円/kW・上限8万円"
municipality: uji
target: 宇治市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 非FIT型：太陽光2万円/kW（上限8万円）、蓄電池3.5万円/kWh（上限21万円）
deadline: "2026-03-13"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 制度名：宇治市ゼロカーボン設備導入事業費補助金
  - 非FIT型：太陽光2万円/kW（上限8万円）、蓄電池3.5万円/kWh（上限21万円）
  - FIT型：太陽光1万円/kW（上限4万円）、蓄電池2万円/kWh（上限12万円）
  - 申請期間：令和7年4月3日〜令和8年3月13日
source_url: "https://www.city.uji.kyoto.jp/soshiki/21/81824.html"
summary_ja: 宇治市の太陽光・蓄電池補助金。非FIT型なら蓄電池3.5万円/kWhと府内トップクラスの補助額。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "uji-taiyoko-2026"
---

## いくらもらえる？

| 設備 | FIT型 | 非FIT型（自家消費） |
|------|-------|-----------------|
| 太陽光発電 | 1万円/kW（上限4万円） | **2万円/kW（上限8万円）** |
| 蓄電池 | 2万円/kWh（上限12万円） | **3.5万円/kWh（上限21万円）** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.uji.kyoto.jp/soshiki/21/81824.html
> 問合せ：環境企画課 TEL: 0774-20-8726
""")

# ============================================================
# 宮津市
# ============================================================
w("miyazu", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】宮津市の太陽光・蓄電池補助金｜非FIT型2万円/kW・先着順"
municipality: miyazu
target: 宮津市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 非FIT型：太陽光2万円/kW（上限8万円）、蓄電池3万円/kWh（上限18万円）+2万円
deadline: "2026-01-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 非FIT型：太陽光2万円/kW（上限8万円）、蓄電池3万円/kWh（上限18万円）+2万円
  - 申請：先着順・令和8年1月31日まで
  - 問合せ：市民環境課 TEL: 0772-45-1617
source_url: "https://www.city.miyazu.kyoto.jp/soshiki/5/25623.html"
summary_ja: 宮津市の太陽光・蓄電池補助金。非FIT型なら太陽光2万円/kW・蓄電池最大20万円の補助を先着順で受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "miyazu-taiyoko-2026"
---

## いくらもらえる？

| 設備 | 補助額 |
|------|-------|
| 太陽光発電（非FIT型） | **2万円/kW（上限8万円）** |
| 蓄電池（非FIT型） | **3万円/kWh（上限18万円）+2万円** |

## 注意点

- **先着順**・令和8年1月31日まで

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.miyazu.kyoto.jp/soshiki/5/25623.html
""")

w("miyazu", "2026-05-11-iju.md", """\
---
title: "【2026年最新】宮津市の移住・子育て支援補助金｜子育て世帯リフォーム上限100万円"
municipality: miyazu
target: 宮津市へ移住する方、子育て世帯
amount: 京都府移住支援金（世帯100万円）、子育て世帯リフォーム補助上限100万円
deadline: "2027-03-31"
tags:
  - 移住
  - 子育て
  - 住宅
key_points:
  - 京都府移住支援金の対象市（世帯100万円・単身60万円）
  - 市外からの移住で子育て世帯住宅リフォーム補助金：上限100万円
  - 結婚新生活支援補助金も利用可能
source_url: "https://www.city.miyazu.kyoto.jp/"
summary_ja: 宮津市の移住・子育て支援。京都府移住支援金に加え、市独自の子育て世帯リフォーム上限100万円の補助も受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "miyazu-iju-2026"
---

## いくらもらえる？

| 補助制度 | 金額 |
|---------|------|
| 京都府移住支援金（世帯） | **100万円** |
| 京都府移住支援金（単身） | **60万円** |
| 子育て世帯住宅リフォーム補助 | **上限100万円** |
| 結婚新生活支援補助金 | 別途あり |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.miyazu.kyoto.jp/
""")

# ============================================================
# 亀岡市
# ============================================================
w("kameoka", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】亀岡市の太陽光・蓄電池補助金｜太陽光1万円/kW・蓄電池1.7万円/kWh"
municipality: kameoka
target: 亀岡市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 太陽光1万円/kW（上限4万円）、蓄電池1.7万円/kWh（上限10.2万円）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 太陽光2kW以上10kW未満＋蓄電池の同時設置が条件
  - 太陽光：1万円/kW（上限4万円）
  - 蓄電池：1.7万円/kWh（上限10.2万円）
  - 問合せ：環境政策課 TEL: 0771-25-5023
source_url: "https://www.city.kameoka.kyoto.jp/soshiki/21/68819.html"
summary_ja: 亀岡市の太陽光・蓄電池補助金。2kW以上の太陽光と蓄電池の同時設置で太陽光1万円/kW・蓄電池1.7万円/kWhの補助を受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kameoka-taiyoko-2026"
---

## こんな人が対象

- 亀岡市内の住宅に**太陽光（2kW以上10kW未満）+蓄電池を同時設置**する方

## いくらもらえる？

| 設備 | 補助額 | 上限 |
|------|-------|------|
| 太陽光発電 | **1万円/kW** | **4万円** |
| 蓄電池 | **1.7万円/kWh** | **10.2万円** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.kameoka.kyoto.jp/soshiki/21/68819.html
> 問合せ：環境政策課 TEL: 0771-25-5023
""")

w("kameoka", "2026-05-11-iju.md", """\
---
title: "【2026年最新】亀岡市の移住・定住支援補助金｜多子世帯・三世代同居支援も"
municipality: kameoka
target: 亀岡市への移住・定住を希望する方（市外からの転入者）
amount: 京都府移住支援金（世帯100万円）＋多子世帯・三世代同居・近居支援補助金
deadline: "2027-03-31"
tags:
  - 移住
  - 子育て
  - 住宅
key_points:
  - 京都府移住支援金の対象市（世帯100万円・単身60万円）
  - 多子世帯・三世代同居・近居支援補助金（住宅取得・改築・賃貸入居費用補助）
  - 子育て世帯Uターン支援補助金も利用可能
source_url: "https://www.city.kameoka.kyoto.jp/"
summary_ja: 亀岡市の移住・定住支援。京都府移住支援金のほか、多子世帯・三世代同居支援など充実した補助制度がある。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kameoka-iju-2026"
---

## 利用できる支援制度

| 補助制度 | 内容 |
|---------|------|
| 京都府移住支援金 | 世帯**100万円**・単身**60万円** |
| 多子世帯・三世代同居・近居支援 | 住宅取得・改築・賃貸入居費用補助 |
| 子育て世帯Uターン支援 | 別途詳細あり |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.kameoka.kyoto.jp/
""")

# ============================================================
# 城陽市
# ============================================================
w("joyo", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】城陽市の太陽光・蓄電池補助金｜カーボンニュートラル補助金"
municipality: joyo
target: 城陽市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 城陽市カーボンニュートラル補助金（詳細は市HP確認）
deadline: "2026-01-30"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 城陽市カーボンニュートラル補助金で太陽光・蓄電池が対象
  - 申請期間：令和8年1月30日まで
  - 詳細金額は城陽市役所に問い合わせ
source_url: "https://www.city.joyo.kyoto.jp/"
summary_ja: 城陽市のカーボンニュートラル補助金で太陽光・蓄電池の設置費用を補助。詳細は市役所に問い合わせ。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "joyo-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.joyo.kyoto.jp/
> ※詳細金額は城陽市役所にお問い合わせください。
""")

# ============================================================
# 向日市
# ============================================================
w("muko", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】向日市の太陽光補助金｜非FIT型7万円/kW・府内トップクラスの高額補助"
municipality: muko
target: 向日市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 非FIT型：太陽光7万円/kW（上限なし）、蓄電池最大80万円
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 制度名：向日市ゼロカーボン推進補助金
  - 非FIT型（自家消費型）：太陽光7万円/kW（上限なし！）
  - FIT型：太陽光1万円/kW（上限4万円）+1万円
  - 蓄電池：設置方法により25万円〜80万円の補助
  - 上限なしは府内トップクラス
source_url: "https://www.city.muko.kyoto.jp/site/zerocarbon/3454.html"
summary_ja: 向日市のゼロカーボン推進補助金。非FIT型太陽光は7万円/kWで上限なし！府内トップクラスの高額補助。蓄電池も最大80万円。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "muko-taiyoko-2026"
---

## こんな人が対象

- 向日市内の住宅に太陽光発電・蓄電池を設置する方
- 非FIT型（自家消費型）なら特に高額補助！

## いくらもらえる？

| 設備 | FIT型 | 非FIT型（自家消費） |
|------|-------|-----------------|
| 太陽光発電 | 1万円/kW（上限4万円）+1万円 | **7万円/kW（上限なし！）** |
| 蓄電池 | **25万円〜80万円**（設置方法による） | 同左 |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.muko.kyoto.jp/site/zerocarbon/3454.html
""")

# ============================================================
# 長岡京市
# ============================================================
w("nagaokakyo", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】長岡京市の太陽光・蓄電池補助金｜COOL CHOICE実践補助金"
municipality: nagaokakyo
target: 長岡京市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 非FIT型：太陽光2万円/kW（上限8万円）、蓄電池3万円/kWh（上限18万円）+1万円
deadline: "2026-02-02"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 制度名：長岡京市COOL CHOICE実践補助金
  - 非FIT型：太陽光2万円/kW（上限8万円）、蓄電池3万円/kWh（上限18万円）+1万円
  - FIT型：太陽光1万円/kW（上限4万円）、蓄電池1.5万円/kWh（上限9万円）
  - 申請期限：令和8年2月2日必着
  - 問合せ：環境政策室 TEL: 075-955-9542
source_url: "https://www.city.nagaokakyo.lg.jp/0000010968.html"
summary_ja: 長岡京市のCOOL CHOICE実践補助金。非FIT型なら太陽光2万円/kW・蓄電池最大19万円の補助を受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "nagaokakyo-taiyoko-2026"
---

## いくらもらえる？

| 設備 | FIT型 | 非FIT型（自家消費） |
|------|-------|-----------------|
| 太陽光発電 | 1万円/kW（上限4万円） | **2万円/kW（上限8万円）** |
| 蓄電池 | 1.5万円/kWh（上限9万円） | **3万円/kWh（上限18万円）+1万円** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.nagaokakyo.lg.jp/0000010968.html
> 問合せ：環境政策室 TEL: 075-955-9542
""")

# ============================================================
# 八幡市
# ============================================================
w("yawata", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】八幡市の太陽光・蓄電池補助金｜非FIT型2万円/kW・定額5万円加算"
municipality: yawata
target: 八幡市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 非FIT型：太陽光2万円/kW（上限8万円）、蓄電池3万円/kWh（上限18万円）+定額5万円
deadline: "2026-01-30"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 非FIT型：太陽光2万円/kW（上限8万円）
  - 非FIT型蓄電池：3万円/kWh（上限18万円）+定額5万円（合計最大23万円）
  - 申請期間：令和7年4月17日〜令和8年1月30日
source_url: "https://www.city.yawata.kyoto.jp/0000009900.html"
summary_ja: 八幡市の太陽光・蓄電池補助金。非FIT型なら太陽光2万円/kW・蓄電池最大23万円（定額5万円加算あり）。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "yawata-taiyoko-2026"
---

## いくらもらえる？

| 設備 | FIT型 | 非FIT型（自家消費） |
|------|-------|-----------------|
| 太陽光発電 | ― | **2万円/kW（上限8万円）** |
| 蓄電池 | ― | **3万円/kWh（上限18万円）+定額5万円** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.yawata.kyoto.jp/0000009900.html
""")

# ============================================================
# 京田辺市
# ============================================================
w("kyotanabe", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】京田辺市の太陽光・蓄電池補助金｜自家消費型・蓄電池システム補助"
municipality: kyotanabe
target: 京田辺市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 住宅用蓄電池システム等設置補助金・自立型再エネ補助金（詳細は市HP確認）
deadline: "2026-02-20"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 住宅用蓄電池システム等設置補助金（FIT型向け・予算192.5万円）
  - 家庭向け自立型再エネ補助金（非FIT型向け・予算1,265万円）
  - 申請期間：令和7年4月1日〜令和8年2月20日
source_url: "https://www.city.kyotanabe.lg.jp/0000022235.html"
summary_ja: 京田辺市の太陽光・蓄電池補助金。FIT型・非FIT型それぞれに補助制度あり。予算が大きい自立型再エネ補助金も注目。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kyotanabe-taiyoko-2026"
---

## こんな人が対象

- 京田辺市内の住宅に太陽光・蓄電池を設置する方

## 補助金の種類

| 補助制度 | 対象 | 予算 |
|---------|------|------|
| 住宅用蓄電池システム等設置補助金 | FIT型 | 192.5万円 |
| 家庭向け自立型再エネ補助金 | 非FIT型（自家消費） | **1,265万円** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.kyotanabe.lg.jp/0000022235.html
""")

# ============================================================
# 京丹後市
# ============================================================
w("kyotango", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】京丹後市の太陽光補助金｜非FIT型7万円/kW・上限70万円"
municipality: kyotango
target: 京丹後市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 非FIT型：太陽光7万円/kW（上限70万円）、蓄電池：補助対象経費の1/3
deadline: "2026-01-09"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 制度名：脱炭素重点対策加速化事業補助金
  - 非FIT型（個人）：太陽光7万円/kW（上限70万円）
  - 蓄電池：補助対象経費の1/3（上限あり）
  - 申請期間：令和7年5月1日〜令和8年1月9日
  - 問合せ：生活環境課 TEL: 0772-69-0240
source_url: "https://www.city.kyotango.lg.jp/top/soshiki/shiminkankyo/seikatsukankyo/3/4/2/21429.html"
summary_ja: 京丹後市の脱炭素加速化補助金。非FIT型太陽光は7万円/kWで上限70万円と非常に手厚い補助。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kyotango-taiyoko-2026"
---

## いくらもらえる？

| 設備 | 補助額 | 上限 |
|------|-------|------|
| 太陽光発電（非FIT型個人） | **7万円/kW** | **70万円** |
| 蓄電池 | 補助対象経費の1/3 | 要確認 |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.kyotango.lg.jp/top/soshiki/shiminkankyo/seikatsukankyo/3/4/2/21429.html
> 問合せ：生活環境課 TEL: 0772-69-0240
""")

w("kyotango", "2026-05-11-iju.md", """\
---
title: "【2026年最新】京丹後市の移住支援金｜世帯100万円＋子ども加算"
municipality: kyotango
target: 東京圏から京丹後市へ移住する方
amount: 世帯100万円・単身60万円＋子ども加算
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 京都府移住支援金の対象市
  - 世帯100万円・単身60万円
  - 18歳未満の子ども1人あたり最大100万円加算
source_url: "https://www.city.kyotango.lg.jp/"
summary_ja: 京丹後市は京都府移住支援金の対象市。東京圏からの移住で世帯100万円・子ども加算も受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kyotango-iju-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.kyotango.lg.jp/
> 京都府移住支援金：https://www.pref.kyoto.jp/jobpark/ijusienkin.html
""")

# ============================================================
# 南丹市
# ============================================================
w("nantan", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】南丹市の太陽光補助金｜非FIT型7万円/kW・上限35万円（住宅）"
municipality: nantan
target: 南丹市内の住宅・事業所に太陽光発電を設置する個人・法人
amount: 住宅：非FIT型7万円/kW（上限5kW・35万円）、事業所：5万円/kW（上限50kW・250万円）
deadline: "2026-01-09"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 脱炭素重点対策加速化事業で手厚い補助
  - 住宅用非FIT型：7万円/kW（上限5kW・35万円）
  - 事業所用：5万円/kW（上限50kW・250万円）
  - 申請期間：令和7年5月14日〜令和8年1月9日
source_url: "https://www.city.nantan.kyoto.jp/www/life/114/004/000/index_1011800.html"
summary_ja: 南丹市の太陽光補助金。脱炭素重点対策加速化事業として住宅7万円/kW（最大35万円）、事業所5万円/kW（最大250万円）の補助。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "nantan-taiyoko-2026"
---

## いくらもらえる？

| 対象 | 補助額 | 上限 |
|------|-------|------|
| 住宅用（非FIT型） | **7万円/kW** | **35万円（5kW上限）** |
| 事業所用 | **5万円/kW** | **250万円（50kW上限）** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.nantan.kyoto.jp/www/life/114/004/000/index_1011800.html
""")

w("nantan", "2026-05-11-iju.md", """\
---
title: "【2026年最新】南丹市の移住支援金｜世帯100万円＋子ども加算"
municipality: nantan
target: 東京圏から南丹市へ移住する方
amount: 世帯100万円・単身60万円＋子ども加算
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 京都府移住支援金の対象市
  - 世帯100万円・単身60万円
  - 18歳未満の子ども1人あたり最大100万円加算
source_url: "https://www.city.nantan.kyoto.jp/"
summary_ja: 南丹市は京都府移住支援金の対象市。東京圏からの移住で世帯100万円・子ども加算も受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "nantan-iju-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.nantan.kyoto.jp/
> 京都府移住支援金：https://www.pref.kyoto.jp/jobpark/ijusienkin.html
""")

# ============================================================
# 木津川市
# ============================================================
w("kizugawa", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】木津川市の蓄電池補助金｜導入費用の1/2・上限20万円"
municipality: kizugawa
target: 木津川市内の住宅に太陽光発電・蓄電池を設置する個人
amount: 蓄電池：導入費用の1/2・上限20万円、太陽光：別途補助あり
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 住宅用蓄電設備設置費補助金：導入費用の1/2・上限20万円
  - 太陽光発電設備の補助金も別途あり（詳細は市HP確認）
  - 問合せ：市民環境部環境課 TEL: 0774-75-1215
source_url: "https://kyoto-saiene.net/subsidy/subsidy-87/"
summary_ja: 木津川市の蓄電池補助金。導入費用の1/2・上限20万円の補助に加え、太陽光発電への補助金も別途用意。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kizugawa-taiyoko-2026"
---

## いくらもらえる？

| 設備 | 補助額 |
|------|-------|
| 蓄電設備 | **導入費用の1/2（上限20万円）** |
| 太陽光発電 | 別途補助あり（要確認） |

## 詳細・申し込みはこちら

> 問合せ：市民環境部環境課 TEL: 0774-75-1215
> 公式HP：https://www.city.kizugawa.lg.jp/
""")

w("kizugawa", "2026-05-11-iju.md", """\
---
title: "【2026年最新】木津川市の移住支援金｜世帯100万円＋子ども加算"
municipality: kizugawa
target: 東京圏から木津川市へ移住する方
amount: 世帯100万円・単身60万円＋子ども加算
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 京都府移住支援金の対象市
  - 世帯100万円・単身60万円
  - 18歳未満の子ども1人あたり最大100万円加算
source_url: "https://www.city.kizugawa.lg.jp/"
summary_ja: 木津川市は京都府移住支援金の対象市。東京圏からの移住で世帯100万円・子ども加算も受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kizugawa-iju-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.kizugawa.lg.jp/
> 京都府移住支援金：https://www.pref.kyoto.jp/jobpark/ijusienkin.html
""")

# ============================================================
# 精華町
# ============================================================
w("seika", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】精華町の太陽光・蓄電池補助金｜非FIT型2.1万円/kW"
municipality: seika
target: 精華町内の住宅に太陽光発電・蓄電池を設置する個人
amount: 非FIT型：太陽光2.1万円/kW（上限8.4万円）、蓄電池3万円/kWh（上限18万円）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 非FIT型：太陽光2.1万円/kW（上限8.4万円）
  - 蓄電池：3万円/kWh（上限18万円）
source_url: "https://kyoto-saiene.net/subsidy/subsidy-100/"
summary_ja: 精華町の太陽光・蓄電池補助金。非FIT型なら太陽光2.1万円/kW（上限8.4万円）・蓄電池3万円/kWhの補助を受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "seika-taiyoko-2026"
---

## いくらもらえる？

| 設備 | 補助額 | 上限 |
|------|-------|------|
| 太陽光発電（非FIT型） | **2.1万円/kW** | **8.4万円** |
| 蓄電池 | **3万円/kWh** | **18万円** |

## 詳細・申し込みはこちら

> 参考：https://kyoto-saiene.net/subsidy/subsidy-100/
> 公式HP：https://www.town.seika.kyoto.jp/
""")

# ============================================================
# 宇治田原町
# ============================================================
w("ujitawara", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】宇治田原町の太陽光・蓄電池補助金｜同時設置で太陽光1万円/kW"
municipality: ujitawara
target: 宇治田原町内の住宅に太陽光発電・蓄電池を設置する個人
amount: 太陽光1万円/kW（上限4万円）、蓄電池2万円/kWh（上限12万円）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 太陽光（2kW以上）+蓄電池+高効率給湯機またはコージェネの同時設置が条件
  - 太陽光：1万円/kW（上限4万円）
  - 蓄電池：2万円/kWh（上限12万円）
source_url: "https://www.town.ujitawara.kyoto.jp/soshiki/kensetsukankyoka/seikatsu_kankyo/4/779.html"
summary_ja: 宇治田原町の太陽光・蓄電池補助金。太陽光+蓄電池+高効率給湯機の同時設置で太陽光1万円/kW・蓄電池2万円/kWhの補助。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "ujitawara-taiyoko-2026"
---

## こんな人が対象

- **太陽光（2kW以上）+蓄電池+高効率給湯機**（またはコージェネ）を同時設置する方

## いくらもらえる？

| 設備 | 補助額 | 上限 |
|------|-------|------|
| 太陽光発電 | **1万円/kW** | **4万円** |
| 蓄電池 | **2万円/kWh** | **12万円** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.town.ujitawara.kyoto.jp/soshiki/kensetsukankyoka/seikatsu_kankyo/4/779.html
""")

w("ujitawara", "2026-05-11-iju.md", """\
---
title: "【2026年最新】宇治田原町の移住支援金｜世帯100万円＋子ども加算"
municipality: ujitawara
target: 東京圏から宇治田原町へ移住する方
amount: 世帯100万円・単身60万円＋子ども加算
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 京都府移住支援金の対象町
  - 世帯100万円・単身60万円
source_url: "https://www.town.ujitawara.kyoto.jp/"
summary_ja: 宇治田原町は京都府移住支援金の対象。東京圏からの移住で世帯100万円の支援を受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "ujitawara-iju-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：https://www.town.ujitawara.kyoto.jp/
> 京都府移住支援金：https://www.pref.kyoto.jp/jobpark/ijusienkin.html
""")

# ============================================================
# 京丹波町
# ============================================================
w("kyotamba", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】京丹波町の太陽光・蓄電池補助金｜太陽光1万円/kW・蓄電池1.5万円/kWh"
municipality: kyotamba
target: 京丹波町内の住宅に太陽光発電・蓄電池を設置する個人
amount: 太陽光1万円/kW（上限4万円）、蓄電池1.5万円/kWh（上限9万円）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 太陽光：1万円/kW（上限4万円）
  - 蓄電池：1.5万円/kWh（上限9万円）
  - 最新情報は町役場に要確認
source_url: "https://www.town.kyotamba.kyoto.jp/"
summary_ja: 京丹波町の太陽光・蓄電池補助金。太陽光1万円/kW（上限4万円）・蓄電池1.5万円/kWh（上限9万円）の補助を受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kyotamba-taiyoko-2026"
---

## いくらもらえる？

| 設備 | 補助額 | 上限 |
|------|-------|------|
| 太陽光発電 | **1万円/kW** | **4万円** |
| 蓄電池 | **1.5万円/kWh** | **9万円** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.town.kyotamba.kyoto.jp/
""")

w("kyotamba", "2026-05-11-iju.md", """\
---
title: "【2026年最新】京丹波町の移住支援金｜世帯100万円＋子ども加算"
municipality: kyotamba
target: 東京圏から京丹波町へ移住する方
amount: 世帯100万円・単身60万円＋子ども加算
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 京都府移住支援金の対象町
  - 世帯100万円・単身60万円
  - 18歳未満の子ども1人あたり最大100万円加算
source_url: "https://www.town.kyotamba.kyoto.jp/"
summary_ja: 京丹波町は京都府移住支援金の対象。東京圏からの移住で世帯100万円・子ども加算も受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kyotamba-iju-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：https://www.town.kyotamba.kyoto.jp/
> 京都府移住支援金：https://www.pref.kyoto.jp/jobpark/ijusienkin.html
""")

# ============================================================
# 伊根町
# ============================================================
w("ine", "2026-05-11-iju.md", """\
---
title: "【2026年最新】伊根町の移住・空き家リフォーム補助金｜上限180万円"
municipality: ine
target: 伊根町へ移住する方、空き家を活用する方
amount: 空き家住宅リフォーム補助金：上限180万円
deadline: "2027-03-31"
tags:
  - 移住
  - 空き家
  - 住宅
key_points:
  - 空き家住宅リフォーム補助金：上限180万円
  - 太陽光・蓄電池は京都府補助制度を利用可能
source_url: "https://www.town.ine.kyoto.jp/"
summary_ja: 伊根町の移住・空き家活用補助。空き家リフォーム補助金は上限180万円と充実した補助を受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "ine-iju-2026"
---

## いくらもらえる？

| 補助制度 | 金額 |
|---------|------|
| 空き家住宅リフォーム補助金 | **上限180万円** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.town.ine.kyoto.jp/
""")

w("ine", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】伊根町の太陽光・蓄電池補助金｜京都府補助制度が利用可能"
municipality: ine
target: 伊根町内の住宅に太陽光発電・蓄電池を設置する個人
amount: 京都府家庭向け補助金（太陽光1万円/kW・蓄電池1.5万円/kWh等）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 京都府の家庭向け太陽光・蓄電池補助金が利用可能
  - FIT型：太陽光1万円/kW（上限4万円）、蓄電池1.5万円/kWh（上限9万円）
  - 非FIT型は約2倍の補助額
  - 詳細は伊根町役場または京都府へ問い合わせ
source_url: "https://www.pref.kyoto.jp/energy/kateimukehojo.html"
summary_ja: 伊根町では京都府の家庭向け太陽光・蓄電池補助金が利用可能。非FIT型なら2倍の補助額になる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "ine-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：https://www.pref.kyoto.jp/energy/kateimukehojo.html
> 伊根町役場：https://www.town.ine.kyoto.jp/
""")

# ============================================================
# 与謝野町
# ============================================================
w("yosano", "2026-05-11-taiyoko.md", """\
---
title: "【2026年最新】与謝野町の太陽光・蓄電池補助金｜京都府補助制度が利用可能"
municipality: yosano
target: 与謝野町内の住宅に太陽光発電・蓄電池を設置する個人
amount: 京都府家庭向け補助金（太陽光1万円/kW・蓄電池1.5万円/kWh等）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 京都府の家庭向け太陽光・蓄電池補助金が利用可能
  - FIT型：太陽光1万円/kW（上限4万円）、蓄電池1.5万円/kWh（上限9万円）
  - 非FIT型は約2倍の補助額
source_url: "https://www.pref.kyoto.jp/energy/kateimukehojo.html"
summary_ja: 与謝野町では京都府の家庭向け太陽光・蓄電池補助金が利用可能。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "yosano-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：https://www.pref.kyoto.jp/energy/kateimukehojo.html
> 与謝野町役場：https://www.town.yosano.kyoto.jp/
""")

# ============================================================
# 大山崎町・久御山町・笠置町・和束町・南山城村（京都府補助のみ）
# ============================================================
for muni_id, name, url in [
    ("oyamazaki", "大山崎町", "https://www.town.oyamazaki.kyoto.jp/"),
    ("kumiyama", "久御山町", "https://www.town.kumiyama.lg.jp/"),
    ("kasagi", "笠置町", "https://www.town.kasagi.kyoto.jp/"),
    ("wazuka", "和束町", "https://www.town.wazuka.kyoto.jp/"),
    ("minamiyamashiro", "南山城村", "https://www.vill.minamiyamashiro.lg.jp/"),
]:
    slug = muni_id.replace("_", "")
    w(muni_id, "2026-05-11-taiyoko.md", f"""\
---
title: "【2026年最新】{name}の太陽光・蓄電池補助金｜京都府補助制度が利用可能"
municipality: {muni_id}
target: {name}内の住宅に太陽光発電・蓄電池を設置する個人
amount: 京都府家庭向け補助金（太陽光1万円/kW・蓄電池1.5万円/kWh等）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 京都府の家庭向け太陽光・蓄電池補助金が利用可能
  - FIT型：太陽光1万円/kW（上限4万円）、蓄電池1.5万円/kWh（上限9万円）
  - 非FIT型（自家消費型）は約2倍の補助額
  - 詳細は{name}役場または京都府へ問い合わせ
source_url: "https://www.pref.kyoto.jp/energy/kateimukehojo.html"
summary_ja: {name}では京都府の家庭向け太陽光・蓄電池補助金が利用可能。非FIT型なら2倍の補助額。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "{muni_id}-taiyoko-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：https://www.pref.kyoto.jp/energy/kateimukehojo.html
> {name}役場：{url}
""")

# 移住支援（笠置・和束・南山城）
for muni_id, name, url in [
    ("kasagi", "笠置町", "https://www.town.kasagi.kyoto.jp/"),
    ("wazuka", "和束町", "https://www.town.wazuka.kyoto.jp/"),
    ("minamiyamashiro", "南山城村", "https://www.vill.minamiyamashiro.lg.jp/"),
    ("ide", "井手町", "https://www.town.ide.kyoto.jp/"),
]:
    w(muni_id, "2026-05-11-iju.md", f"""\
---
title: "【2026年最新】{name}の移住支援金｜世帯100万円＋子ども加算"
municipality: {muni_id}
target: 東京圏から{name}へ移住する方
amount: 世帯100万円・単身60万円＋子ども加算
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 京都府移住支援金の対象
  - 世帯100万円・単身60万円
  - 18歳未満の子ども1人あたり最大100万円加算
source_url: "{url}"
summary_ja: {name}は京都府移住支援金の対象。東京圏からの移住で世帯100万円・子ども加算も受けられる。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "{muni_id}-iju-2026"
---

## 詳細・申し込みはこちら

> 公式ページ：{url}
> 京都府移住支援金：https://www.pref.kyoto.jp/jobpark/ijusienkin.html
""")

print("\n全ファイル生成完了!")
