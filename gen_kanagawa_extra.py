#!/usr/bin/env python3
"""神奈川県全市町村への追加補助金記事生成（2本目・3本目）"""
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
# 横浜市 — 子育て・住宅取得支援
# ============================================================
w("yokohama", "2026-05-12-kosodatesumai.md", """\
---
title: "【2026年最新】横浜市の子育て世帯住宅取得・リフォーム補助金｜次世代育成支援"
municipality: yokohama
target: 横浜市内で住宅取得・リフォームをする子育て世帯
amount: 補助制度多数（詳細は横浜市HP確認）
deadline: "2027-03-31"
tags:
  - 子育て
  - 住宅
  - 省エネ
key_points:
  - 横浜市すまい給付金・子育て世帯向け補助多数
  - 省エネリフォーム・断熱改修補助金も利用可能
  - 神奈川県の住宅用太陽光・蓄電池補助（7万円/kW・蓄電池15万円）との併用も検討可
source_url: "https://www.city.yokohama.lg.jp/"
summary_ja: 横浜市の子育て世帯向け住宅取得・リフォーム補助。神奈川県補助との組み合わせで多重に活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "yokohama-kosodatesumai-2026"
---

## 詳細・申し込みはこちら

> 横浜市公式HP：https://www.city.yokohama.lg.jp/
> 神奈川県補助金：https://www.pref.kanagawa.jp/
""")

w("yokohama", "2026-05-12-chusho.md", """\
---
title: "【2026年最新】横浜市の中小企業向け省エネ・脱炭素補助金｜太陽光最大500万円に加え追加支援"
municipality: yokohama
target: 横浜市内の中小企業・小規模事業者
amount: 省エネ設備導入・脱炭素化支援（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 中小企業
  - 省エネ
  - 創業
key_points:
  - 太陽光発電導入支援（最大500万円・既存記事参照）のほか追加の省エネ補助あり
  - 横浜市中小企業融資制度・信用保証制度も利用可能
  - 横浜市ビジネス支援センター（B-PLUS）で相談可
source_url: "https://www.city.yokohama.lg.jp/business/"
summary_ja: 横浜市の中小企業支援。太陽光発電補助（最大500万円）のほか、省エネ・脱炭素・融資など多彩な支援制度を用意。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "yokohama-chusho-2026"
---

## 詳細・申し込みはこちら

> 横浜市産業振興：https://www.city.yokohama.lg.jp/business/
""")

# ============================================================
# 川崎市 — 子育て・創業支援
# ============================================================
w("kawasaki", "2026-05-12-kosodatesumai.md", """\
---
title: "【2026年最新】川崎市の子育て世帯向け住宅・省エネ支援補助金"
municipality: kawasaki
target: 川崎市内の子育て世帯、住宅購入・リフォームをする方
amount: 省エネ設備・住宅取得支援（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 子育て
  - 住宅
  - 省エネ
key_points:
  - 川崎市の太陽光・蓄電池補助（ZEH最大40万円・蓄電池最大70万円・既存記事参照）
  - 子育て世帯向けの住宅取得・改修支援制度も別途あり
  - 神奈川県補助との組み合わせで多重活用可能
source_url: "https://www.city.kawasaki.jp/"
summary_ja: 川崎市の子育て世帯向け住宅・省エネ支援。ZEH・蓄電池補助に加え子育て世帯向けの追加支援制度も利用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "kawasaki-kosodatesumai-2026"
---

## 詳細・申し込みはこちら

> 川崎市公式HP：https://www.city.kawasaki.jp/
""")

# ============================================================
# 相模原市 — 中小企業・創業支援
# ============================================================
w("sagamihara", "2026-05-12-chusho.md", """\
---
title: "【2026年最新】相模原市の中小企業・創業支援補助金｜さがみはらスタートアップ支援"
municipality: sagamihara
target: 相模原市内の中小企業・創業者
amount: 創業補助・省エネ設備補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 中小企業
  - 創業
  - 省エネ
key_points:
  - さがみはらスタートアップ支援センターでの創業相談・補助
  - 中小企業省エネ設備導入補助金あり
  - 神奈川県小規模事業者デジタル化支援補助（最大50万円）との組み合わせも可
source_url: "https://www.city.sagamihara.kanagawa.jp/"
summary_ja: 相模原市の中小企業・創業支援。スタートアップ支援センターを拠点に、創業から省エネまで幅広く補助。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "sagamihara-chusho-2026"
---

## 詳細・申し込みはこちら

> 相模原市公式HP：https://www.city.sagamihara.kanagawa.jp/
""")

# ============================================================
# 小田原市 — 移住支援
# ============================================================
w("odawara", "2026-05-12-iju.md", """\
---
title: "【2026年最新】小田原市の移住・定住支援補助金｜空き家活用・リフォーム補助"
municipality: odawara
target: 小田原市への移住・定住を希望する方
amount: 移住・空き家活用補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 小田原市空き家バンク・移住支援窓口を設置
  - 空き家リフォーム補助金あり
  - 神奈川県西部の交通アクセス良好（新幹線停車駅）
  - 太陽光7万円/kWの既存補助金（既存記事参照）との組み合わせ可
source_url: "https://www.city.odawara.kanagawa.jp/"
summary_ja: 小田原市の移住・定住支援。新幹線停車駅で交通至便、空き家バンクや移住相談窓口も充実。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "odawara-iju-2026"
---

## 小田原市の特徴

- 新幹線「こだま」停車駅・東京から約35分
- 小田原城下町の歴史的街並み・豊かな自然
- 空き家バンク制度・移住相談窓口あり

## 詳細・申し込みはこちら

> 小田原市公式HP：https://www.city.odawara.kanagawa.jp/
""")

# ============================================================
# 横須賀市 — 移住・子育て支援
# ============================================================
w("yokosuka", "2026-05-12-iju.md", """\
---
title: "【2026年最新】横須賀市の移住・子育て支援補助金｜「住みよこすか」定住促進"
municipality: yokosuka
target: 横須賀市への移住・定住を希望する方、子育て世帯
amount: 移住・子育て支援（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 子育て
key_points:
  - 横須賀市の定住促進事業「住みよこすか」
  - 子育て世帯への各種支援制度あり
  - 三浦半島の豊かな自然・海岸環境
  - 太陽光7万円/kWの既存補助金との組み合わせ可
source_url: "https://www.city.yokosuka.kanagawa.jp/"
summary_ja: 横須賀市の移住・子育て支援。「住みよこすか」定住促進事業で三浦半島での暮らしを支援。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "yokosuka-iju-2026"
---

## 詳細・申し込みはこちら

> 横須賀市公式HP：https://www.city.yokosuka.kanagawa.jp/
""")

# ============================================================
# 鎌倉市 — 移住支援
# ============================================================
w("kamakura", "2026-05-12-iju.md", """\
---
title: "【2026年最新】鎌倉市の移住・定住支援補助金｜古都の暮らし・空き家活用"
municipality: kamakura
target: 鎌倉市への移住・定住を希望する方
amount: 移住・空き家活用補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 古都鎌倉の歴史的環境での生活
  - 空き家バンク・移住支援窓口あり
  - 太陽光7万円/kWの三浦半島連携補助（既存記事参照）との組み合わせ可
source_url: "https://www.city.kamakura.kanagawa.jp/"
summary_ja: 鎌倉市の移住・定住支援。古都の歴史的環境の中で、空き家バンクや移住相談を通じた定住を支援。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "kamakura-iju-2026"
---

## 詳細・申し込みはこちら

> 鎌倉市公式HP：https://www.city.kamakura.kanagawa.jp/
""")

# ============================================================
# 藤沢市 — 子育て・住宅支援
# ============================================================
w("fujisawa", "2026-05-12-kosodatesumai.md", """\
---
title: "【2026年最新】藤沢市の子育て世帯・住宅取得支援補助金"
municipality: fujisawa
target: 藤沢市内で住宅取得・リフォームをする子育て世帯
amount: 子育て・住宅支援（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 子育て
  - 住宅
  - 省エネ
key_points:
  - 藤沢市の子育て世帯向け住宅取得・改修支援
  - 太陽光1.5万円/kWの既存補助金との組み合わせ可
  - 神奈川県補助との多重活用も検討可
source_url: "https://www.city.fujisawa.kanagawa.jp/"
summary_ja: 藤沢市の子育て世帯向け住宅支援。太陽光補助との組み合わせで省エネ住宅への補助を最大化できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "fujisawa-kosodatesumai-2026"
---

## 詳細・申し込みはこちら

> 藤沢市公式HP：https://www.city.fujisawa.kanagawa.jp/
""")

# ============================================================
# 三浦市 — 移住支援
# ============================================================
w("miura", "2026-05-12-iju.md", """\
---
title: "【2026年最新】三浦市の移住・定住支援補助金｜海辺の暮らし・漁師町への移住"
municipality: miura
target: 三浦市への移住・定住を希望する方
amount: 移住・定住補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 三浦半島最南端・マグロ・野菜の産地として有名
  - 移住支援窓口・空き家バンクあり
  - 三浦半島連携の太陽光7万円/kW補助（既存記事参照）との組み合わせ可
  - 東京から約60〜70分でアクセス可能
source_url: "https://www.city.miura.kanagawa.jp/"
summary_ja: 三浦市の移住・定住支援。海に囲まれた三浦半島最南端で、空き家バンクや移住支援を活用した暮らしを実現できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "miura-iju-2026"
---

## 三浦市の特徴

- 三浦半島最南端・新鮮な海の幸（マグロ）と農産物
- 東京から約60〜70分でアクセス
- 空き家バンク・移住体験住宅あり

## 詳細・申し込みはこちら

> 三浦市公式HP：https://www.city.miura.kanagawa.jp/
""")

# ============================================================
# 逗子市 — 移住支援
# ============================================================
w("zushi", "2026-05-12-iju.md", """\
---
title: "【2026年最新】逗子市の移住・定住支援補助金｜海と山に囲まれた逗子での暮らし"
municipality: zushi
target: 逗子市への移住・定住を希望する方
amount: 移住・定住補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 逗子海岸・葉山に隣接する人気の住宅地
  - 移住支援窓口あり
  - 三浦半島連携の太陽光7万円/kW補助（既存記事参照）との組み合わせ可
source_url: "https://www.city.zushi.kanagawa.jp/"
summary_ja: 逗子市の移住・定住支援。逗子海岸・葉山に隣接する人気エリアへの移住を支援する制度あり。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "zushi-iju-2026"
---

## 詳細・申し込みはこちら

> 逗子市公式HP：https://www.city.zushi.kanagawa.jp/
""")

# ============================================================
# 南足柄市 — 移住支援
# ============================================================
w("minamiashigara", "2026-05-12-iju.md", """\
---
title: "【2026年最新】南足柄市の移住・定住支援補助金｜金太郎の故郷・足柄山麓への移住"
municipality: minamiashigara
target: 南足柄市への移住・定住を希望する方
amount: 移住・定住補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 金太郎伝説の地・足柄山麓の豊かな自然
  - 移住支援窓口・空き家バンクあり
  - 既存の省エネ補助（ZEH・断熱等・既存記事参照）との組み合わせ可
source_url: "https://www.city.minamiashigara.kanagawa.jp/"
summary_ja: 南足柄市の移住・定住支援。足柄山麓の豊かな自然の中で空き家バンクを活用した定住が可能。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "minamiashigara-iju-2026"
---

## 詳細・申し込みはこちら

> 南足柄市公式HP：https://www.city.minamiashigara.kanagawa.jp/
""")

# ============================================================
# 厚木市 — 中小企業支援
# ============================================================
w("atsugi", "2026-05-12-chusho.md", """\
---
title: "【2026年最新】厚木市の中小企業・創業支援補助金｜ものづくりのまちの産業支援"
municipality: atsugi
target: 厚木市内の中小企業・小規模事業者・創業者
amount: 中小企業補助・創業支援（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 中小企業
  - 創業
  - 省エネ
key_points:
  - ものづくり企業の集積地・厚木市の産業支援充実
  - 中小企業省エネ・DX化補助あり
  - 神奈川県デジタル化補助（最大50万円）との組み合わせ可
  - 厚木商工会議所による創業支援も充実
source_url: "https://www.city.atsugi.kanagawa.jp/"
summary_ja: 厚木市の中小企業・創業支援。ものづくり産業が集積する厚木で、省エネからDXまで幅広い補助制度を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "atsugi-chusho-2026"
---

## 詳細・申し込みはこちら

> 厚木市公式HP：https://www.city.atsugi.kanagawa.jp/
""")

# ============================================================
# 秦野市 — 移住支援
# ============================================================
w("hadano", "2026-05-12-iju.md", """\
---
title: "【2026年最新】秦野市の移住・定住支援補助金｜丹沢山麓・名水のまちへの移住"
municipality: hadano
target: 秦野市への移住・定住を希望する方
amount: 移住・定住補助（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 丹沢山麓・環境省「名水百選」秦野盆地湧水群の自然豊かな環境
  - 移住支援窓口・空き家バンクあり
  - 既存の省エネ補助（ゼロカーボン補助・既存記事参照）との組み合わせ可
source_url: "https://www.city.hadano.kanagawa.jp/"
summary_ja: 秦野市の移住・定住支援。丹沢山麓・名水の地で空き家バンクを活用した定住が可能。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "hadano-iju-2026"
---

## 詳細・申し込みはこちら

> 秦野市公式HP：https://www.city.hadano.kanagawa.jp/
""")

# ============================================================
# 葉山町 — 移住支援
# ============================================================
w("hayama", "2026-05-12-iju.md", """\
---
title: "【2026年最新】葉山町の移住・定住支援補助金｜三浦半島の高級住宅地への移住"
municipality: hayama
target: 葉山町への移住・定住を希望する方
amount: 移住・定住補助（詳細は町HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
key_points:
  - 御用邸のある閑静な高級住宅地
  - 三浦半島連携の太陽光7万円/kW補助（既存記事参照）との組み合わせ可
  - 移住支援・空き家バンクあり
source_url: "https://www.town.hayama.lg.jp/"
summary_ja: 葉山町の移住・定住支援。御用邸のある閑静な高級住宅地への移住を支援する制度あり。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "hayama-iju-2026"
---

## 詳細・申し込みはこちら

> 葉山町公式HP：https://www.town.hayama.lg.jp/
""")

# ============================================================
# 山北町 — 移住支援
# ============================================================
w("yamakita", "2026-05-12-iju.md", """\
---
title: "【2026年最新】山北町の移住・定住支援補助金｜丹沢・箱根の玄関口への移住"
municipality: yamakita
target: 山北町への移住・定住を希望する方
amount: 移住・定住補助（詳細は町HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 丹沢・箱根の玄関口・松田駅近くの自然豊かな環境
  - 空き家バンク・移住支援窓口あり
  - 都市部から移住しやすい交通アクセス（小田急線・JR御殿場線）
source_url: "https://www.town.yamakita.kanagawa.jp/"
summary_ja: 山北町の移住・定住支援。丹沢・箱根の玄関口で空き家バンクを活用した田舎暮らしが可能。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "yamakita-iju-2026"
---

## 詳細・申し込みはこちら

> 山北町公式HP：https://www.town.yamakita.kanagawa.jp/
""")

# ============================================================
# 箱根町 — 移住支援
# ============================================================
w("hakone", "2026-05-12-iju.md", """\
---
title: "【2026年最新】箱根町の移住・定住支援補助金｜温泉地・観光地での田舎暮らし"
municipality: hakone
target: 箱根町への移住・定住を希望する方
amount: 移住・定住補助（詳細は町HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 温泉・芦ノ湖・富士山の絶景を誇る観光地での暮らし
  - 空き家バンク・移住支援窓口あり
  - 既存の太陽光補助（再生可能エネルギー補助・既存記事参照）との組み合わせ可
source_url: "https://www.town.hakone.kanagawa.jp/"
summary_ja: 箱根町の移住・定住支援。温泉と自然に囲まれた観光地で、空き家バンクを活用した移住が可能。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "hakone-iju-2026"
---

## 詳細・申し込みはこちら

> 箱根町公式HP：https://www.town.hakone.kanagawa.jp/
""")

# ============================================================
# 湯河原町 — 移住支援
# ============================================================
w("yugawara", "2026-05-12-iju.md", """\
---
title: "【2026年最新】湯河原町の移住・定住支援補助金｜温泉の里への移住"
municipality: yugawara
target: 湯河原町への移住・定住を希望する方
amount: 移住・定住補助（詳細は町HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 文豪ゆかりの温泉地・湯河原での暮らし
  - 空き家バンク・移住支援あり
  - 熱海・伊豆方面のアクセス良好
source_url: "https://www.town.yugawara.kanagawa.jp/"
summary_ja: 湯河原町の移住・定住支援。温泉地として有名な湯河原で、空き家活用を通じた定住を支援。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "yugawara-iju-2026"
---

## 詳細・申し込みはこちら

> 湯河原町公式HP：https://www.town.yugawara.kanagawa.jp/
""")

# ============================================================
# 清川村 — 移住支援
# ============================================================
w("kiyokawa", "2026-05-12-iju.md", """\
---
title: "【2026年最新】清川村の移住・定住支援補助金｜神奈川県唯一の村への移住"
municipality: kiyokawa
target: 清川村への移住・定住を希望する方
amount: 移住・定住補助（詳細は村HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 定住
  - 空き家
key_points:
  - 神奈川県唯一の「村」・丹沢山麓の自然豊かな環境
  - 空き家バンク・移住支援窓口あり
  - 既存の太陽光補助（1.5万円/kW・既存記事参照）との組み合わせ可
source_url: "https://www.vill.kiyokawa.kanagawa.jp/"
summary_ja: 清川村の移住・定住支援。神奈川県唯一の村・丹沢山麓で空き家バンクを活用した田舎暮らしを実現できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "kiyokawa-iju-2026"
---

## 清川村の特徴

- **神奈川県唯一の「村」**・人口約3,000人の丹沢山麓
- 宮ヶ瀬湖・清川七沢温泉など自然豊かな環境

## 詳細・申し込みはこちら

> 清川村公式HP：https://www.vill.kiyokawa.kanagawa.jp/
""")

# ============================================================
# 茅ヶ崎市 — 移住・子育て支援
# ============================================================
w("chigasaki", "2026-05-12-iju.md", """\
---
title: "【2026年最新】茅ヶ崎市の移住・子育て支援補助金｜湘南の海辺への定住"
municipality: chigasaki
target: 茅ヶ崎市への移住・定住を希望する方、子育て世帯
amount: 移住・子育て支援（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 子育て
  - 定住
key_points:
  - 湘南の海辺・サザンオールスターズの地として有名
  - 子育て世帯への各種支援制度あり
  - 移住支援・定住促進窓口あり
source_url: "https://www.city.chigasaki.kanagawa.jp/"
summary_ja: 茅ヶ崎市の移住・子育て支援。湘南の海辺での暮らしを子育て世帯向け支援と組み合わせて実現できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "chigasaki-iju-2026"
---

## 詳細・申し込みはこちら

> 茅ヶ崎市公式HP：https://www.city.chigasaki.kanagawa.jp/
""")

# 大和市 — 移住支援
w("yamato", "2026-05-12-iju.md", """\
---
title: "【2026年最新】大和市の移住・子育て支援補助金"
municipality: yamato
target: 大和市への移住・定住を希望する方、子育て世帯
amount: 移住・子育て支援（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 移住
  - 子育て
  - 定住
key_points:
  - 大和市の子育て支援・移住支援窓口あり
  - 自家消費型太陽光7万円/kW（既存記事参照）との組み合わせ可
source_url: "https://www.city.yamato.lg.jp/"
summary_ja: 大和市の移住・子育て支援。太陽光高額補助（7万円/kW）と組み合わせた省エネ生活を支援。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "yamato-iju-2026"
---

## 詳細・申し込みはこちら

> 大和市公式HP：https://www.city.yamato.lg.jp/
""")

# 海老名市 — 中小企業支援
w("ebina", "2026-05-12-chusho.md", """\
---
title: "【2026年最新】海老名市の中小企業・創業支援補助金｜産業集積地の企業支援"
municipality: ebina
target: 海老名市内の中小企業・小規模事業者・創業者
amount: 中小企業補助・創業支援（詳細は市HP確認）
deadline: "2027-03-31"
tags:
  - 中小企業
  - 創業
  - 省エネ
key_points:
  - 海老名市の産業集積地・企業誘致・支援充実
  - 神奈川県デジタル化補助（最大50万円）との組み合わせ可
  - 既存の太陽光補助（2万円/kW・蓄電池7万円・既存記事参照）との組み合わせ可
source_url: "https://www.city.ebina.kanagawa.jp/"
summary_ja: 海老名市の中小企業・創業支援。産業集積地として企業誘致・省エネ・DXまで幅広い支援制度を活用できる。
scraped_at: "2026-05-12T00:00:00Z"
is_active: true
content_hash: "ebina-chusho-2026"
---

## 詳細・申し込みはこちら

> 海老名市公式HP：https://www.city.ebina.kanagawa.jp/
""")

print("\n神奈川追加 全ファイル生成完了!")
