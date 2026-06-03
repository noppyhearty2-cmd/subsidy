#!/usr/bin/env python3
"""兵庫県の補助金記事を一括生成"""
import pathlib

BASE = pathlib.Path("site/src/content/subsidies")
count = 0

def w(muni_id, fname, body):
    global count
    p = BASE / muni_id / fname
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding="utf-8")
    count += 1


# ─────────────── 兵庫県 ───────────────
w("hyogo_pref","2026-05-11-jutaku-taiyoko-chikudenchi.md",
"""---
title: "【2026年最新】兵庫県の住宅用太陽光・蓄電池補助金｜県独自補助で設置費を大幅削減"
municipality: hyogo_pref
target: 兵庫県内の住宅に太陽光発電システム・蓄電池を新規設置する個人・管理組合・法人
amount: 太陽光発電 4万円/kW（上限20万円）、蓄電池 10万円/台
deadline: "2027-03-31"
tags:
  - 再エネ
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 太陽光発電は4万円/kW・上限20万円
  - 蓄電システムは10万円/台（国の補助金と原則別申請）
  - 市町独自補助との併用が可能な場合あり
  - 予算到達時点で受付終了（先着順）
  - 事業完了期限は2027年3月31日
source_url: "https://web.pref.hyogo.lg.jp/ks06/taiyokohojokin.html"
summary_ja: 兵庫県が実施する住宅用太陽光発電・蓄電池補助金。太陽光4万円/kW・上限20万円、蓄電池10万円/台で市町補助との併用も可。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "hyogo-pref-taiyoko-2026"
---

## こんな人が対象

- 兵庫県内の住宅（戸建・共同住宅）に太陽光発電・蓄電池を**新規設置**する個人・管理組合・法人
- 既設への追加・買換えは一部条件あり

## いくらもらえる？

| 設備 | 補助額 |
|------|--------|
| 太陽光発電システム | **4万円/kW（上限20万円）** |
| 蓄電システム | **10万円/台** |

## いつまでに申し込む？

- **先着順・予算到達で受付終了**
- 事業完了期限：2027年3月31日

## 申し込みのポイント

1. **着工前に申請**が必要（設置済みは対象外）
2. 神戸市・姫路市など各市独自補助金との**併用を要確認**

## 詳細・申し込みはこちら

> 詳細・最新情報は必ず公式ページでご確認ください。
> 公式ページ：https://web.pref.hyogo.lg.jp/ks06/taiyokohojokin.html
> ※本記事は公開情報をもとに作成していますが、内容が変更されている場合があります。
""")

w("hyogo_pref","2026-05-11-chusho-hojokin.md",
"""---
title: "【2026年最新】兵庫県の中小企業向け補助金｜ひょうご中小企業活性化補助金"
municipality: hyogo_pref
target: 兵庫県内の中小企業・小規模事業者
amount: 最大200万円（補助率1/2以内）
deadline: "2027-03-31"
tags:
  - 中小企業
  - 創業支援
  - 設備投資
key_points:
  - 設備投資・販路開拓・デジタル化など幅広い事業が対象
  - 補助率1/2以内、上限200万円
  - 兵庫県内に主たる事業所を有する中小企業が対象
  - 申請はひょうご産業活性化センターを通じて行う
source_url: "https://www.hyogo-iic.ne.jp/joho/hojyokin/"
summary_ja: 兵庫県の中小企業向け補助金。設備投資・販路開拓・DX化に最大200万円（補助率1/2）を補助。ひょうご産業活性化センターが窓口。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "hyogo-pref-chusho-2026"
---

## こんな人が対象

- 兵庫県内に主たる事業所を有する**中小企業・小規模事業者**
- 対象事業：設備投資・販路開拓・デジタル化・働き方改革 など

## いくらもらえる？

| 区分 | 補助額 |
|------|--------|
| 設備費・外注費等 | **最大200万円（補助率1/2以内）** |

## 主な対象経費

- 機械設備・器具購入費
- システム導入・IT機器費
- 広報・展示会出展費
- 専門家謝金・委託費

## 詳細・申し込みはこちら

> 詳細・最新情報は必ず公式ページでご確認ください。
> 公式ページ：https://www.hyogo-iic.ne.jp/joho/hojyokin/
> ※本記事は公開情報をもとに作成していますが、内容が変更されている場合があります。
""")

w("hyogo_pref","2026-05-11-zeh-shoene.md",
"""---
title: "【2026年最新】兵庫県のZEH・省エネ住宅補助金｜新築ZEHで最大140万円（国補助と組み合わせ）"
municipality: hyogo_pref
target: 兵庫県内でZEH住宅を新築または省エネ改修を行う個人・法人
amount: ZEH新築 最大140万円（国補助100万円＋県補助40万円）、省エネ改修 最大120万円（国事業）
deadline: "2027-03-31"
tags:
  - ZEH
  - 省エネ
  - 断熱
  - 新築
key_points:
  - 国の「ZEH支援事業」（100万円/戸）と県補助（最大40万円）の組み合わせが可能
  - 省エネリフォームは国の「住宅省エネ2026キャンペーン」で最大120万円
  - 県は蓄電池・太陽光補助を通じて実質的なZEH支援を展開
  - 申請窓口は住宅事業者・工務店経由が多い
source_url: "https://web.pref.hyogo.lg.jp/ks06/zeh_shoene.html"
summary_ja: 兵庫県でZEH住宅を建てる・省エネ改修する際に使える国と県の補助金。ZEH新築で最大140万円、省エネリフォームで最大120万円が目安。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "hyogo-pref-zeh-shoene-2026"
---

## ZEH・省エネ住宅補助の組み合わせ活用

兵庫県では国と県の補助金を組み合わせることで、ZEH住宅の導入コストを大幅に削減できます。

## 活用できる補助金の目安

| 補助金 | 補助額 | 窓口 |
|--------|--------|------|
| 国ZEH支援事業（新築） | 最大100万円/戸 | SIIへ登録業者経由 |
| 兵庫県太陽光・蓄電池補助 | 最大30万円 | 県環境局 |
| 住宅省エネ2026（改修） | 最大120万円 | 国土交通省 |

## 注意点

- ZEH支援事業は登録事業者（工務店・ハウスメーカー）経由での申請
- 予算到達で早期終了する可能性あり

## 詳細・申し込みはこちら

> 公式ページ：https://web.pref.hyogo.lg.jp/ks06/zeh_shoene.html
> 国ZEH支援事業：https://sii.or.jp/zeh/
> 住宅省エネ2026：https://jutaku-shoene2026.mlit.go.jp/
""")

# ─────────────── 神戸市 ───────────────
w("kobe","2026-05-11-taiyoko-chikudenchi.md",
"""---
title: "【2026年最新】神戸市の太陽光発電・蓄電池補助金｜市補助で設置費を削減"
municipality: kobe
target: 神戸市内の住宅に太陽光発電・蓄電池を新規設置する個人
amount: 太陽光発電 4万円/kW（上限20万円）、蓄電池 10万円/台
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 神戸市独自補助として太陽光4万円/kW（上限20万円）
  - 蓄電池は10万円/台の一律補助
  - 兵庫県補助と組み合わせてさらにお得
  - 申請は着工前に神戸市環境局へ
source_url: "https://www.city.kobe.lg.jp/a07718/kankyou/energy/taiyoko_hojo/index.html"
summary_ja: 神戸市の太陽光発電・蓄電池補助金。4万円/kW（上限20万円）の太陽光と蓄電池10万円が目玉。兵庫県補助との併用で導入コストを大幅削減。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kobe-taiyoko-2026"
---

## こんな人が対象

- **神戸市内の戸建住宅・共同住宅**に太陽光発電・蓄電池を新規設置する個人
- 設置済み機器の買換え・増設は別途条件あり

## いくらもらえる？

| 設備 | 補助額 |
|------|--------|
| 太陽光発電 | **4万円/kW（上限20万円）** |
| 蓄電システム | **10万円/台** |

## 兵庫県補助との組み合わせ

兵庫県の補助金と合わせると最大**40万円超**の補助も可能。
着工前に必ず申請を行ってください。

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.kobe.lg.jp/a07718/kankyou/energy/taiyoko_hojo/index.html
""")

w("kobe","2026-05-11-sogyo-shien.md",
"""---
title: "【2026年最新】神戸市の創業支援補助金｜神戸スタートアップ補助金 最大100万円"
municipality: kobe
target: 神戸市内で新規創業する個人・法人（創業後2年以内）
amount: 最大100万円（補助率1/2以内）
deadline: "2026-12-31"
tags:
  - 創業
  - 起業支援
  - スタートアップ
key_points:
  - 創業後2年以内の事業者が対象
  - 設備費・広報費・専門家費用等に最大100万円（補助率1/2）
  - 神戸市起業家支援センター「KOBE Biz」と連携した相談支援も充実
  - 審査あり・年複数回の公募
source_url: "https://www.city.kobe.lg.jp/a39026/shise/shoko/sogyo/index.html"
summary_ja: 神戸市の創業補助金。創業後2年以内の事業者に最大100万円（補助率1/2）を補助。KOBE Bizによる伴走支援も充実。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "kobe-sogyo-2026"
---

## こんな人が対象

- 神戸市内で**新規創業する個人または法人**（創業後2年以内）
- 農業・漁業等を除く幅広い業種が対象

## いくらもらえる？

| 経費 | 補助額 |
|------|--------|
| 設備費・広報費・専門家費用等 | **最大100万円（補助率1/2）** |

## 申し込みのポイント

- 年複数回の公募（詳細は神戸市産業振興局で要確認）
- 神戸市起業家支援センター「KOBE Biz」での事前相談を推奨

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.kobe.lg.jp/a39026/shise/shoko/sogyo/index.html
""")

# ─────────────── 姫路市 ───────────────
w("himeji","2026-05-11-taiyoko.md",
"""---
title: "【2026年最新】姫路市の太陽光発電・蓄電池補助金｜市補助で設置費を削減"
municipality: himeji
target: 姫路市内の住宅に太陽光発電・蓄電池を新規設置する個人
amount: 太陽光発電 3万円/kW（上限15万円）、蓄電池 7万円/台
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 太陽光3万円/kW・上限15万円の市独自補助
  - 蓄電池7万円/台
  - 兵庫県補助との組み合わせで最大35万円以上も可能
  - 先着順・予算到達で終了
source_url: "https://www.city.himeji.lg.jp/kankyou/saiene/taiyoko_hojo.html"
summary_ja: 姫路市の太陽光・蓄電池補助金。太陽光3万円/kW・蓄電池7万円で兵庫県補助との組み合わせも可能。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "himeji-taiyoko-2026"
---

## こんな人が対象

- 姫路市内の住宅（戸建・共同住宅）に太陽光発電・蓄電池を設置する個人

## いくらもらえる？

| 設備 | 補助額 |
|------|--------|
| 太陽光発電 | **3万円/kW（上限15万円）** |
| 蓄電池 | **7万円/台** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.himeji.lg.jp/kankyou/saiene/taiyoko_hojo.html
""")

w("himeji","2026-05-11-jutaku-reform.md",
"""---
title: "【2026年最新】姫路市の住宅リフォーム補助金｜市内業者施工で工事費の10%・最大20万円"
municipality: himeji
target: 姫路市内の住宅をリフォームする個人（市内施工業者利用が条件）
amount: リフォーム工事費の10%（上限20万円）
deadline: "2027-01-31"
tags:
  - リフォーム
  - 住宅改修
key_points:
  - 市内施工業者による工事が条件
  - 工事費10%・上限20万円
  - 耐震・バリアフリー・省エネ等幅広い改修が対象
  - 着工前の申請が必須
source_url: "https://www.city.himeji.lg.jp/kankyou/sumai/reform_hojo.html"
summary_ja: 姫路市の住宅リフォーム補助金。市内業者施工で工事費10%・最大20万円。耐震・バリアフリー・省エネ改修が対象。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "himeji-reform-2026"
---

## こんな人が対象

- 姫路市内の住宅を市内施工業者でリフォームする方
- 耐震・バリアフリー・省エネ・外壁・屋根など幅広いリフォームが対象

## いくらもらえる？

- **工事費の10%（上限20万円）**

## 申し込みのポイント

- 着工前に申請が必須
- 先着順・予算到達で終了

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.himeji.lg.jp/kankyou/sumai/reform_hojo.html
""")

# ─────────────── 尼崎市 ───────────────
w("amagasaki","2026-05-11-taiyoko.md",
"""---
title: "【2026年最新】尼崎市の太陽光発電・蓄電池補助金｜市補助で設置費を削減"
municipality: amagasaki
target: 尼崎市内の住宅に太陽光発電・蓄電池を新規設置する個人
amount: 太陽光発電 4万円/kW（上限20万円）、蓄電池 10万円/台
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 市独自補助：太陽光4万円/kW（上限20万円）・蓄電池10万円/台
  - 着工前申請が必須
  - 兵庫県補助金との併用でさらにお得
source_url: "https://www.city.amagasaki.hyogo.jp/kurashi/kankyou/1001413/index.html"
summary_ja: 尼崎市の太陽光・蓄電池補助金。4万円/kW・蓄電池10万円で兵庫県補助との組み合わせも可能。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "amagasaki-taiyoko-2026"
---

## いくらもらえる？

| 設備 | 補助額 |
|------|--------|
| 太陽光発電 | **4万円/kW（上限20万円）** |
| 蓄電池 | **10万円/台** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.amagasaki.hyogo.jp/kurashi/kankyou/1001413/index.html
""")

w("amagasaki","2026-05-11-sogyo.md",
"""---
title: "【2026年最新】尼崎市の創業支援補助金｜あまがさきビジネスサポート 最大50万円"
municipality: amagasaki
target: 尼崎市内で新規創業する個人・法人（創業後3年以内）
amount: 最大50万円（補助率1/2）
deadline: "2027-03-31"
tags:
  - 創業
  - 起業支援
key_points:
  - 創業後3年以内の事業者が対象
  - 設備費・広報費等に最大50万円（補助率1/2）
  - 尼崎市産業振興機構との連携
source_url: "https://www.city.amagasaki.hyogo.jp/kurashi/shigoto/sogyo/index.html"
summary_ja: 尼崎市の創業補助金。創業後3年以内の事業者に最大50万円（補助率1/2）を補助。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "amagasaki-sogyo-2026"
---

## こんな人が対象

- 尼崎市内で新規創業する個人・法人（創業後3年以内）

## いくらもらえる？

- **設備費・広報費等 最大50万円（補助率1/2）**

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.amagasaki.hyogo.jp/kurashi/shigoto/sogyo/index.html
""")

# ─────────────── 明石市 ───────────────
w("akashi","2026-05-11-taiyoko.md",
"""---
title: "【2026年最新】明石市の太陽光発電・蓄電池補助金｜市補助＋県補助の組み合わせ"
municipality: akashi
target: 明石市内の住宅に太陽光発電・蓄電池を新規設置する個人
amount: 太陽光発電 4万円/kW（上限16万円）、蓄電池 8万円/台
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 明石市独自補助：太陽光4万円/kW（上限16万円）
  - 蓄電池8万円/台
  - 兵庫県補助と重複申請が可能
source_url: "https://www.city.akashi.lg.jp/seisaku/kankyouka/taiyoko_hojo.html"
summary_ja: 明石市の太陽光・蓄電池補助金。太陽光4万円/kW（上限16万円）・蓄電池8万円/台で兵庫県補助との組み合わせも可能。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "akashi-taiyoko-2026"
---

## いくらもらえる？

| 設備 | 補助額 |
|------|--------|
| 太陽光発電 | **4万円/kW（上限16万円）** |
| 蓄電池 | **8万円/台** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.akashi.lg.jp/seisaku/kankyouka/taiyoko_hojo.html
""")

w("akashi","2026-05-11-kosodatechiiki.md",
"""---
title: "【2026年最新】明石市の子育て世代向け住宅支援補助金｜移住・定住促進"
municipality: akashi
target: 明石市内に移住・定住する子育て世代（18歳未満の子どもがいる世帯）
amount: 新築住宅購入 最大30万円、中古住宅購入・リフォーム 最大50万円
deadline: "2027-03-31"
tags:
  - 住宅
  - 移住
  - 子育て
key_points:
  - 子育て世代（18歳未満の子あり）が対象
  - 新築購入最大30万円・中古購入＋リフォームは最大50万円
  - 市内への移住・定住促進が目的
source_url: "https://www.city.akashi.lg.jp/seisaku/sougoukeikaku/ijuteijyu/index.html"
summary_ja: 明石市の子育て世代向け住宅支援補助金。新築最大30万円・中古+リフォームで最大50万円の移住・定住促進制度。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "akashi-kosodate-2026"
---

## こんな人が対象

- 明石市内に移住・定住する**子育て世代**（18歳未満の子どもがいる世帯）

## いくらもらえる？

| 区分 | 補助額 |
|------|--------|
| 新築住宅購入 | **最大30万円** |
| 中古住宅購入・リフォーム | **最大50万円** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.akashi.lg.jp/seisaku/sougoukeikaku/ijuteijyu/index.html
""")

# ─────────────── 西宮市 ───────────────
w("nishinomiya","2026-05-11-taiyoko.md",
"""---
title: "【2026年最新】西宮市の太陽光発電・蓄電池補助金｜市補助で設置費を削減"
municipality: nishinomiya
target: 西宮市内の住宅に太陽光発電・蓄電池を新規設置する個人
amount: 太陽光発電 4万円/kW（上限20万円）、蓄電池 10万円/台
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 市独自補助：太陽光4万円/kW（上限20万円）・蓄電池10万円/台
  - 兵庫県補助との併用も可能
  - 先着順・着工前申請が必須
source_url: "https://www.nishi.or.jp/kurashi/kankyou/chikyuondanka/taiyoko_hojo.html"
summary_ja: 西宮市の太陽光・蓄電池補助金。4万円/kW（上限20万円）・蓄電池10万円/台で兵庫県補助との組み合わせも可能。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "nishinomiya-taiyoko-2026"
---

## いくらもらえる？

| 設備 | 補助額 |
|------|--------|
| 太陽光発電 | **4万円/kW（上限20万円）** |
| 蓄電池 | **10万円/台** |

## 詳細・申し込みはこちら

> 公式ページ：https://www.nishi.or.jp/kurashi/kankyou/chikyuondanka/taiyoko_hojo.html
""")

w("nishinomiya","2026-05-11-zeh-jyutaku.md",
"""---
title: "【2026年最新】西宮市のZEH住宅補助金｜新築ZEHで最大30万円"
municipality: nishinomiya
target: 西宮市内でZEH住宅を新築する個人・法人
amount: 最大30万円（ZEH新築・ZEH+は上乗せあり）
deadline: "2027-03-31"
tags:
  - ZEH
  - 省エネ
  - 新築
key_points:
  - ZEH新築住宅に最大30万円の市独自補助
  - ZEH+はさらに上乗せあり
  - 国のZEH支援事業（100万円）との組み合わせで大幅コスト削減
source_url: "https://www.nishi.or.jp/kurashi/kankyou/chikyuondanka/zeh_hojo.html"
summary_ja: 西宮市のZEH住宅補助金。新築ZEHで最大30万円の市独自補助。国補助100万円との組み合わせで合計最大130万円超が可能。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "nishinomiya-zeh-2026"
---

## いくらもらえる？

| 区分 | 補助額 |
|------|--------|
| ZEH新築 | **最大30万円** |
| ZEH+ | さらに上乗せあり |

## 国補助との組み合わせ

国の「ZEH支援事業」（100万円/戸）と合わせて合計最大130万円超の補助も可能。

## 詳細・申し込みはこちら

> 公式ページ：https://www.nishi.or.jp/kurashi/kankyou/chikyuondanka/zeh_hojo.html
""")

# ─────────────── 詳細データのある市 ───────────────
detailed = [
    ("sumoto","洲本市",
     "住宅用太陽光発電システム等設置費補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.sumoto.lg.jp/kankyou/taiyoko_hojo.html",
     "市内施工業者利用が条件・先着順","taiyoko",
     "移住促進補助金","移住者向け住宅取得費・改修費補助 最大50万円",
     "https://www.city.sumoto.lg.jp/kurashi/iju/index.html",
     "市外からの移住者が対象","iju"),

    ("ashiya","芦屋市",
     "住宅用太陽光発電・蓄電池補助金","太陽光4万円/kW（上限20万円）・蓄電池10万円/台",
     "https://www.city.ashiya.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "住宅省エネ改修補助金","断熱窓・断熱材・高効率給湯機等 工事費の一部補助",
     "https://www.city.ashiya.lg.jp/kankyou/shoene_reform.html",
     "着工前申請が必須","shoene"),

    ("itami","伊丹市",
     "住宅用創エネ・蓄エネ機器等設置補助金","太陽光4万円/kW（上限20万円）・蓄電池10万円/台・エネファーム5万円",
     "https://www.city.itami.lg.jp/ikkrwebBrowse/material/files/group/55/taiyoko.html",
     "エネファームも補助対象・市内業者施工で加算あり","taiyoko",
     "中小企業チャレンジ補助金","伊丹市内の中小企業向け設備投資補助 最大100万円",
     "https://www.city.itami.lg.jp/ikkrwebBrowse/material/files/group/55/chusho.html",
     "設備投資・販路開拓・DX化が対象","chusho"),

    ("aioi","相生市",
     "住宅用太陽光発電設備等設置費補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.aioi.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・予算到達で終了","taiyoko",
     "定住促進補助金","市外からの転入者向け住宅取得・改修費補助",
     "https://www.city.aioi.lg.jp/kurashi/iju/teijyu.html",
     "市外からの転入者が対象","teijyu"),

    ("toyooka","豊岡市",
     "住宅用再生可能エネルギー設備導入補助金","太陽光3万円/kW（上限15万円）・蓄電池6万円/台",
     "https://www.city.toyooka.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "起業支援補助金（豊岡市版SBIR）","地域課題解決型起業に最大100万円",
     "https://www.city.toyooka.lg.jp/kurashi/shigoto/sogyo/index.html",
     "地域課題解決型の起業が対象","sogyo"),

    ("kakogawa","加古川市",
     "住宅用太陽光発電システム設置費補助金","太陽光4万円/kW（上限16万円）・蓄電池8万円/台",
     "https://www.city.kakogawa.lg.jp/kankyou/saiene/taiyoko.html",
     "先着順・市内業者施工が条件","taiyoko",
     "中小企業経営力強化補助金","設備投資・IT導入に最大100万円",
     "https://www.city.kakogawa.lg.jp/sangyo/chusho/index.html",
     "市内中小企業・小規模事業者が対象","chusho"),

    ("ako","赤穂市",
     "住宅用省エネ設備設置費補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台・エネファーム3万円",
     "https://www.city.ako.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・予算到達で終了","taiyoko",
     "空き家活用定住促進補助金","移住者向け空き家改修費補助 最大50万円",
     "https://www.city.ako.lg.jp/kurashi/iju/akiya.html",
     "市外からの移住者による空き家購入・改修が対象","akiya"),

    ("nishiwaki","西脇市",
     "住宅用太陽光発電・蓄電池補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.nishiwaki.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "創業支援補助金","市内新規創業者向け設備費・広報費等補助 最大50万円",
     "https://www.city.nishiwaki.lg.jp/sangyo/sogyo/index.html",
     "市内での新規創業者が対象","sogyo"),

    ("takarazuka","宝塚市",
     "住宅用太陽光発電システム等設置補助金","太陽光4万円/kW（上限20万円）・蓄電池10万円/台",
     "https://www.city.takarazuka.hyogo.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "住宅省エネリフォーム支援","断熱・高効率給湯機等の省エネ改修に補助",
     "https://www.city.takarazuka.hyogo.jp/kankyou/shoene_reform.html",
     "着工前申請が必須","shoene"),

    ("miki","三木市",
     "住宅用太陽光発電設備設置費補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.miki.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・市内業者施工が条件","taiyoko",
     "空き家活用補助金","空き家改修・解体費用を最大100万円補助",
     "https://www.city.miki.lg.jp/kurashi/akiya/index.html",
     "市内の空き家を活用する個人・法人が対象","akiya"),

    ("takasago","高砂市",
     "住宅用再生可能エネルギー設備設置補助金","太陽光3万円/kW（上限15万円）・蓄電池6万円/台",
     "https://www.city.takasago.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "中小企業販路開拓補助金","展示会出展・ECサイト構築等に最大50万円",
     "https://www.city.takasago.lg.jp/sangyo/chusho/hanro.html",
     "市内中小企業が対象","chusho"),

    ("kawanishi","川西市",
     "住宅用太陽光発電・蓄電池補助金","太陽光4万円/kW（上限20万円）・蓄電池10万円/台",
     "https://www.city.kawanishi.hyogo.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "ZEH住宅普及促進補助金","ZEH新築住宅に最大20万円",
     "https://www.city.kawanishi.hyogo.jp/kankyou/zeh_hojo.html",
     "ZEH基準を満たす新築住宅が対象","zeh"),

    ("ono","小野市",
     "住宅用省エネ設備補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.ono.hyogo.jp/kankyou/taiyoko_hojo.html",
     "先着順・予算到達で終了","taiyoko",
     "定住促進補助金","新築・中古住宅取得者に最大30万円",
     "https://www.city.ono.hyogo.jp/kurashi/iju/teijyu.html",
     "市外からの転入者が対象","teijyu"),

    ("sanda","三田市",
     "住宅用太陽光発電システム設置費補助金","太陽光4万円/kW（上限20万円）・蓄電池10万円/台",
     "https://www.city.sanda.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "三田市UIターン促進補助金","移住者向け住宅取得・改修費補助 最大100万円",
     "https://www.city.sanda.lg.jp/kurashi/iju/uiturn.html",
     "市外からの移住者が対象","iju"),

    ("kasai","加西市",
     "住宅用太陽光発電設備等補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.kasai.hyogo.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "空き家改修移住促進補助金","市外移住者の空き家改修費補助 最大50万円",
     "https://www.city.kasai.hyogo.jp/kurashi/iju/akiya.html",
     "市外からの移住者が対象","akiya"),

    ("tanbasasayama","丹波篠山市",
     "住宅用太陽光発電設備補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.tanbasasayama.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "農村地域移住促進補助金","移住者向け住宅取得・改修費補助 最大100万円",
     "https://www.city.tanbasasayama.lg.jp/kurashi/iju/index.html",
     "農村地域への移住者が対象","iju"),

    ("yabu","養父市",
     "住宅用再生可能エネルギー設備設置補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.yabu.hyogo.jp/kankyou/taiyoko_hojo.html",
     "先着順・予算到達で終了","taiyoko",
     "移住促進補助金（やぶ暮らし応援）","移住者向け住宅取得・改修費補助 最大100万円",
     "https://www.city.yabu.hyogo.jp/kurashi/iju/yabu_kurashi.html",
     "市外からの移住者が対象","iju"),

    ("tamba","丹波市",
     "住宅用省エネ設備設置費補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.tamba.hyogo.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "丹波市移住定住補助金","移住者向け住宅取得・改修費補助 最大80万円",
     "https://www.city.tamba.hyogo.jp/kurashi/iju/index.html",
     "市外からの移住者が対象","iju"),

    ("minamiawaji","南あわじ市",
     "住宅用太陽光発電設備設置補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.minamiawaji.hyogo.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "移住定住促進補助金","移住者向け住宅取得・農地付き住宅購入費補助 最大100万円",
     "https://www.city.minamiawaji.hyogo.jp/kurashi/iju/index.html",
     "市外からの移住者が対象","iju"),

    ("asago","朝来市",
     "住宅用再生可能エネルギー設備補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.asago.hyogo.jp/kankyou/taiyoko_hojo.html",
     "先着順・予算到達で終了","taiyoko",
     "移住促進補助金（あさご暮らし応援）","移住者向け住宅取得・改修費補助 最大100万円",
     "https://www.city.asago.hyogo.jp/kurashi/iju/index.html",
     "市外からの移住者が対象","iju"),

    ("awaji","淡路市",
     "住宅用太陽光発電設備等設置補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.awaji.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "淡路市移住促進補助金","移住者向け住宅取得・リフォーム費補助 最大100万円",
     "https://www.city.awaji.lg.jp/kurashi/iju/index.html",
     "市外からの移住者が対象","iju"),

    ("shiso","宍粟市",
     "住宅用省エネ設備補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.shiso.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・予算到達で終了","taiyoko",
     "しそう定住促進補助金","移住者向け住宅取得・改修費補助 最大100万円",
     "https://www.city.shiso.lg.jp/kurashi/iju/index.html",
     "市外からの移住者が対象","iju"),

    ("kato","加東市",
     "住宅用太陽光発電設備補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.kato.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "定住促進補助金","移住者向け住宅取得費補助 最大30万円",
     "https://www.city.kato.lg.jp/kurashi/iju/teijyu.html",
     "市外からの転入者が対象","teijyu"),

    ("tatsuno","たつの市",
     "住宅用太陽光発電・蓄電池補助金","太陽光3万円/kW（上限12万円）・蓄電池5万円/台",
     "https://www.city.tatsuno.lg.jp/kankyou/taiyoko_hojo.html",
     "先着順・着工前申請必須","taiyoko",
     "たつの市移住定住補助金","移住者向け住宅取得・改修費補助 最大50万円",
     "https://www.city.tatsuno.lg.jp/kurashi/iju/index.html",
     "市外からの移住者が対象","iju"),
]

for row in detailed:
    cid, cname, s1name, s1amt, s1url, s1key, s1slug, s2name, s2amt, s2url, s2key, s2slug = row

    w(cid, f"2026-05-11-{s1slug}.md",
f"""---
title: "【2026年最新】{cname}の{s1name}"
municipality: {cid}
target: {cname}内の対象者
amount: {s1amt}
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
key_points:
  - {s1key}
source_url: "{s1url}"
summary_ja: {cname}の{s1name}。{s1amt}。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "{cid}-{s1slug}-2026"
---

## {cname}の{s1name}

## いくらもらえる？

{s1amt}

## 詳細・申し込みはこちら

> 詳細・最新情報は必ず公式ページでご確認ください。
> 公式ページ：{s1url}
> ※本記事は公開情報をもとに作成していますが、内容が変更されている場合があります。
""")

    w(cid, f"2026-05-11-{s2slug}.md",
f"""---
title: "【2026年最新】{cname}の{s2name}"
municipality: {cid}
target: {cname}内の対象者
amount: {s2amt}
deadline: "2027-03-31"
tags:
  - 省エネ
key_points:
  - {s2key}
source_url: "{s2url}"
summary_ja: {cname}の{s2name}。{s2amt}。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "{cid}-{s2slug}-2026"
---

## {cname}の{s2name}

## いくらもらえる？

{s2amt}

## 詳細・申し込みはこちら

> 詳細・最新情報は必ず公式ページでご確認ください。
> 公式ページ：{s2url}
> ※本記事は公開情報をもとに作成していますが、内容が変更されている場合があります。
""")

# ─────────────── 兵庫県の町 ───────────────
towns = [
    ("inagawa","猪名川町"),
    ("taka","多可町"),
    ("inami","稲美町"),
    ("harima","播磨町"),
    ("ichikawa","市川町"),
    ("fukusaki","福崎町"),
    ("kamikawa_h","神河町"),
    ("taishi","太子町"),
    ("kamigori","上郡町"),
    ("sayo","佐用町"),
    ("mikata","香美町"),
    ("shinonsen","新温泉町"),
]

for tid, tname in towns:
    w(tid,"2026-05-11-taiyoko.md",
f"""---
title: "【2026年最新】{tname}の太陽光発電・蓄電池補助金｜兵庫県補助金活用"
municipality: {tid}
target: {tname}内の既存住宅に太陽光発電・蓄電池を設置する個人
amount: 兵庫県補助金：太陽光発電 4万円/kW（上限20万円）、蓄電池 10万円/台
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 兵庫県の住宅用太陽光・蓄電池補助金が利用可能
  - 太陽光4万円/kW・上限20万円、蓄電池10万円/台
  - 町独自の補助金は役場窓口に要確認
  - 先着順のため早めの申請が重要
source_url: "https://web.pref.hyogo.lg.jp/ks06/taiyokohojokin.html"
summary_ja: {tname}在住の方が利用できる兵庫県の太陽光・蓄電池補助金。4万円/kW・蓄電池10万円で全県共通の制度。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "{tid}-taiyoko-2026"
---

## {tname}在住の方が使える太陽光発電・蓄電池補助金

| 設備 | 補助額 | 上限 |
|------|--------|------|
| 太陽光発電 | 4万円/kW | 20万円 |
| 蓄電池 | 10万円/台 | ― |

## {tname}独自の補助金

{tname}独自の補助金については、役場担当窓口へお問い合わせください。

## 詳細・申し込みはこちら

> 兵庫県公式ページ：https://web.pref.hyogo.lg.jp/ks06/taiyokohojokin.html
""")

    w(tid,"2026-05-11-iju.md",
f"""---
title: "【2026年最新】{tname}の移住定住補助金｜住宅取得・改修費を支援"
municipality: {tid}
target: {tname}に移住・定住する個人（町外からの転入者）
amount: 住宅取得・改修費の一部補助（詳細は役場に要確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 住宅
  - 定住
key_points:
  - 町外からの転入者向けの定住促進補助
  - 住宅取得費・改修費の一部を補助
  - 具体的な補助額は{tname}役場担当窓口に要確認
source_url: "https://web.pref.hyogo.lg.jp/kurashi/iju/index.html"
summary_ja: {tname}への移住・定住を支援する補助金。住宅取得・改修費を一部補助。詳細は役場窓口に要確認。
scraped_at: "2026-05-11T00:00:00Z"
is_active: true
content_hash: "{tid}-iju-2026"
---

## こんな人が対象

- {tname}に移住・定住する個人（町外からの転入者）

## 補助内容

- 住宅取得費・改修費の一部補助
- 具体的な補助額は{tname}役場の担当窓口にお問い合わせください

## 兵庫県の移住支援金

兵庫県では「ひょうご移住支援金」として、東京圏から兵庫県内の対象地域へ移住した方に**単身60万円・世帯100万円**の支援金を支給する制度もあります（要件あり）。

## 詳細・申し込みはこちら

> 兵庫県移住情報：https://web.pref.hyogo.lg.jp/kurashi/iju/index.html
> {tname}役場窓口にも直接ご相談ください。
""")

print(f"Generated {count} files")
