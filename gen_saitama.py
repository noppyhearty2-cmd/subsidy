#!/usr/bin/env python3
"""埼玉県の補助金記事を一括生成"""
import pathlib

BASE = pathlib.Path("site/src/content/subsidies")
count = 0

def w(muni_id, fname, body):
    global count
    p = BASE / muni_id / fname
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body, encoding="utf-8")
    count += 1


# ─────────────── 埼玉県 ───────────────
w("saitama_pref","2026-05-10-shoene-saiene.md",
"""---
title: "【2026年最新】埼玉県の省エネ・再エネ設備導入補助金｜太陽光7万円/kW・蓄電池10万円"
municipality: saitama_pref
target: 埼玉県内の既存住宅に太陽光発電・蓄電池・エネファーム等を新規設置する個人
amount: 太陽光発電 7万円/kW（上限35万円）、蓄電池 10万円/件
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
  - 再エネ
key_points:
  - 太陽光発電は7万円/kW・上限35万円
  - 蓄電池は10万円/件の一律補助
  - エネファーム・太陽熱利用システムも対象
  - 先着順・予算到達で早期終了
  - 令和8年度分は2026年5月以降受付開始予定
source_url: "https://www.pref.saitama.lg.jp/a0503/hojyokin2.html"
summary_ja: 埼玉県が実施する住宅用太陽光・蓄電池補助金。7万円/kWの太陽光と10万円一律の蓄電池補助が目玉で、予算到達で早期終了する人気の制度。
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "saitama_pref-shoene-saiene-2026"
---

## 埼玉県の省エネ・再エネ補助金とは

埼玉県では「家庭における省エネ・再エネ活用設備導入補助金」を毎年実施しています。

## 補助金額（令和7年度実績）

| 設備 | 補助額 |
|------|--------|
| 太陽光発電 | 7万円/kW（上限35万円） |
| 蓄電池 | 10万円/件 |
| エネファーム | 10万円/件 |
| 太陽熱利用システム | 4万円/件 |

## 注意点

- **先着順・予算到達で終了**（令和7年度は太陽光が早期終了）
- 一部市町村の独自補助金と重複利用不可の場合あり

## 詳細・申し込みはこちら

> 公式ページ：https://www.pref.saitama.lg.jp/a0503/hojyokin2.html
""")

w("saitama_pref","2026-05-10-co2-chusho.md",
"""---
title: "【2026年最新】埼玉県のCO2排出削減設備導入補助金｜中小企業向け緊急対策枠"
municipality: saitama_pref
target: 埼玉県内の中小企業・小規模事業者
amount: 補助対象経費の一定率（緊急対策枠：初回枠11億円・リピーター枠9億円）
deadline: "2027-03-31"
tags:
  - 中小企業
  - 省エネ
  - 太陽光発電
  - 脱炭素
key_points:
  - 空調・ボイラー等高効率機器への更新が対象
  - 太陽光発電と蓄電池の同時設置も対象
  - 令和8年4月27日から先着順で受付開始
  - 補助対象経費は30万円以上が要件
source_url: "https://www.pref.saitama.lg.jp/a0502/hojokin/r7co2hojo-kinkyutaisaku.html"
summary_ja: 埼玉県内の中小企業向けCO2削減設備補助。空調・ボイラー更新や太陽光+蓄電池設置に対して補助対象経費の一定率を補助。
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "saitama_pref-co2-chusho-2026"
---

## 埼玉県CO2排出削減設備導入補助金（緊急対策枠）

埼玉県内の中小企業を対象に、CO2削減につながる設備更新・導入費用を補助します。

## 対象設備

- 高効率空調・ボイラーへの更新
- 太陽光発電と蓄電池の導入
- EMS（エネルギー管理システム）

## 詳細・申し込みはこちら

> 公式ページ：https://www.pref.saitama.lg.jp/a0502/hojokin/r7co2hojo-kinkyutaisaku.html
""")

w("saitama_pref","2026-05-10-kigyo-shien.md",
"""---
title: "【2026年最新】埼玉県の起業支援金｜最大200万円・地域課題解決型創業を後押し"
municipality: saitama_pref
target: 人口減少地域で地域課題解決を目指し起業する個人・法人
amount: 最大200万円（人件費・設備費・外注費・広報費等）
deadline: "2027-03-31"
tags:
  - 創業
  - 起業支援
key_points:
  - 最大200万円の補助
  - 人口減少地域での地域課題解決型起業が対象
source_url: "https://www.pref.saitama.lg.jp/shigoto/sangyo/sogyo/index.html"
summary_ja: 埼玉県の起業支援金。人口減少地域での地域課題解決型創業に最大200万円を補助する県独自の制度。
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "saitama_pref-kigyo-shien-2026"
---

## 埼玉県起業支援金

## 詳細・申し込みはこちら

> 公式ページ：https://www.pref.saitama.lg.jp/shigoto/sangyo/sogyo/index.html
""")

# ─────────────── さいたま市 ───────────────
w("saitama","2026-05-10-shoene-jutaku.md",
"""---
title: "【2026年最新】さいたま市の省エネ・断熱住宅普及促進補助金｜ZEH新築最大30万円"
municipality: saitama
target: さいたま市内の住宅に省エネ・ZEH設備を設置する個人
amount: ZEH新築 最大30万円（補助率1/2）、断熱改修・高効率給湯機も対象
deadline: "2027-03-03"
tags:
  - 省エネ
  - ZEH
  - 断熱
key_points:
  - ZEH新築住宅に最大30万円（補助率1/2）
  - 断熱改修・高効率給湯機も対象
  - 着工前申請が必須
source_url: "https://www.city.saitama.lg.jp/001/009/015/010/002/p119491.html"
summary_ja: さいたま市の住宅省エネ補助金。ZEH新築最大30万円・断熱改修・高効率給湯機等に対応。
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "saitama-shoene-jutaku-2026"
---

## さいたま市の省エネ・断熱住宅普及促進補助金

| 区分 | 補助額 |
|------|--------|
| ZEH新築 | 最大30万円（工事費の1/2） |
| 断熱改修 | 要件に応じた補助 |
| 高効率給湯機 | 要件に応じた補助 |

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.saitama.lg.jp/001/009/015/010/002/p119491.html
""")

w("saitama","2026-05-10-taiyoko.md",
"""---
title: "【2026年最新】さいたま市の太陽光発電共同購入「みんなのおうちに太陽光」"
municipality: saitama
target: さいたま市内の戸建住宅に太陽光発電を設置したい市民
amount: 共同購入による割引価格での設置（埼玉県補助金と組み合わせ可）
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 省エネ
key_points:
  - 市が主導する共同購入で設置費を削減
  - 埼玉県補助金（7万円/kW）と組み合わせ可能
source_url: "https://www.city.saitama.lg.jp/001/009/015/010/002/p119491.html"
summary_ja: さいたま市の太陽光発電共同購入事業。市が主導して安価に設置でき、埼玉県補助金との組み合わせも可能。
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "saitama-taiyoko-2026"
---

## さいたま市「みんなのおうちに太陽光」共同購入事業

- 市認定施工業者から一括見積もりが取得可能
- 埼玉県補助金（7万円/kW）との併用でさらにお得

## 詳細・申し込みはこちら

> 公式ページ：https://www.city.saitama.lg.jp/001/009/015/010/002/p119491.html
""")

# ─────────────── 個別詳細データのある市 ───────────────
detailed = [
  ("kawagoe","川越市",
   "住宅用脱炭素化設備等導入奨励金","太陽光・蓄電池・ZEHで抽選交付（予算約712万円）",
   "https://www.city.kawagoe.saitama.jp/kurashi/kankyo/1002642/1002702/1017472.html",
   "前期・後期の2回に分けた抽選方式","datsutanso",
   "住宅改修補助金","リフォーム費用の一部補助",
   "https://www.city.kawagoe.saitama.jp/kurashi/jyutaku/1003030.html",
   "市内業者による施工・着工前申請が必須","reform"),

  ("kumagaya","熊谷市",
   "再生可能エネルギー・省エネルギー設備設置費補助金","補助金は地域電子マネー「クマPAY」で交付",
   "https://www.city.kumagaya.lg.jp/about/soshiki/kankyo/kankyoseisaku/kankyoseisakuhojo/06saienehojyo.html",
   "補助金が「クマPAY」で交付される独自制度・太陽光・蓄電池・エネファーム対象","saiene",
   "創業者応援補助金","法人設立費・備品・広報費の一部補助",
   "https://hojyokin-portal.jp/subsidies/58385",
   "農林漁業・金融・保険業は対象外","sogyo"),

  ("kawaguchi","川口市",
   "地球温暖化対策活動支援金","太陽光設置費の1/2（市内業者で上限16万円）",
   "https://www.city.kawaguchi.lg.jp/soshiki/01100/010/2/1/34382.html",
   "令和8年5月11日〜令和9年3月15日受付（先着順）・市税滞納なしが条件","ondanka",
   "住宅リフォーム補助金","リフォーム費用の一部補助",
   "https://www.city.kawaguchi.lg.jp/soshiki/01130/040/2_2/48993.html",
   "市内業者による施工・着工前申請が必須","reform"),

  ("tokorozawa","所沢市",
   "スマートハウス化推進補助金","太陽光 3万円/kW（上限15万円）、蓄電池 3万円/kWh（上限24万円）",
   "https://www.city.tokorozawa.saitama.jp/kurashi/seikatukankyo/kankyo/ekojyosei/sumatohausu.html",
   "条件を満たすと最大33%加算・非FIT太陽光は10万円/kW","smart",
   "非FIT太陽光発電・蓄電池補助金","非FIT太陽光 10万円/kW（上限50万円）",
   "https://www.city.tokorozawa.saitama.jp/kurashi/seikatukankyo/kankyo/ekojyosei/non-fit.html",
   "自家消費型の太陽光が対象","non_fit"),

  ("kazo","加須市",
   "住宅用再生可能エネルギー設備等設置補助金","太陽光 7万円/kW（上限35万円）・蓄電池 10万円/件（予算1,100万円）",
   "https://www.city.kazo.lg.jp/soshiki/kakyou_seisaku/ondanka/hojyoseido/38360.html",
   "先着順・令和9年3月24日までに設置完了が条件","saiene",
   "創業支援補助金","創業に要する費用の一部補助",
   "https://www.city.kazo.lg.jp/soshiki/sangyokoyou/hozyokin/35738.html",
   "市内での新規創業が対象","sogyo"),

  ("kasukabe","春日部市",
   "個人住宅における太陽光発電設備・蓄電池設置補助金",
   "太陽光 5万円/kW（重点区域・上限25万円）または4万円/kW、蓄電池 4万円/kWh（上限24万円）",
   "https://www.city.kasukabe.lg.jp/soshikikarasagasu/kankyoseisakuka/gyomuannai/2/2/30493.html",
   "令和8年5月11日から申請受付開始","taiyoko",
   "企業向け太陽光発電設備・蓄電池設置補助金","事業所への太陽光・蓄電池設置費の一部補助",
   "https://www.city.kasukabe.lg.jp/soshikikarasagasu/kankyoseisakuka/gyomuannai/2/2/28003.html",
   "市内事業者の事業所への設置が対象","jigyosha"),

  ("niiza","新座市",
   "太陽光発電設備等設置費補助金","太陽光発電 9万円/kW（個人5kW・事業者20kWまで）",
   "https://www.city.niiza.lg.jp/soshiki/15/taiyokohojokin.html",
   "令和8年5月1日受付開始・埼玉県内でも高水準の9万円/kW","taiyoko",
   "ゼロカーボン推進補助金","令和7年度は最大90万円超の交付例あり",
   "https://www.city.niiza.lg.jp/soshiki/15/zerokabonhojokin.html",
   "太陽光+蓄電池の同時設置が対象","zero_carbon"),

  ("koshigaya","越谷市",
   "省エネ家電買換促進補助金","家電量販店購入4万円・市内事業者購入7万円（エアコン・冷蔵庫各1台）",
   "https://www.smart-hojokin.jp/subsidies/84270",
   "市内事業者購入で7万円・量販店は4万円","kaden",
   "ゼロカーボン推進補助金","太陽光・蓄電池等の設置費補助",
   "https://hojyokin-portal.jp/subsidies/41243",
   "越谷市独自のゼロカーボン推進補助制度","saiene"),

  ("sakado","坂戸市",
   "住宅用太陽光・省エネルギー機器設置費補助金","上限5万円（うち2万円は地域通貨で交付）",
   "https://www.city.sakado.lg.jp/soshiki/20/8841.html",
   "着工前申請が必須・先着順","taiyoko",
   "省エネ家電購入費補助金","省エネ家電購入費の一部補助",
   "https://www.city.sakado.lg.jp/soshiki/20/36971.html",
   "省エネ性能の高い家電への買換えが対象","kaden"),

  ("okegawa","桶川市",
   "脱炭素事業推進奨励金","各機器に応じた補助（複数申請の場合の合計上限なし）",
   "https://www.city.okegawa.lg.jp/soshiki/shiminseikatsu/kanri/gomi/eco/14816.html",
   "令和8年4月1日から新名称で受付開始・複数設備申請でも合計上限なし","datsutanso",
   "省エネ・再エネ補助金（埼玉県）","太陽光 7万円/kW（上限35万円）",
   "https://www.pref.saitama.lg.jp/a0503/hojyokin2.html",
   "桶川市独自補助との重複は要確認","pref"),

  ("gyoda","行田市",
   "住宅用省エネ設備等補助金","太陽光 7万円/kW（上限35万円）・蓄電池 10万円/件・太陽熱 2/3（上限20万円）",
   "https://www.city.gyoda.lg.jp/kurashi/hojokin_joseikin/index.html",
   "埼玉県補助金と同水準の手厚い補助","saiene",
   "木造住宅耐震改修補助金","最大36万円",
   "https://www.city.gyoda.lg.jp/kurashi/sumai/hojo_josei/index.html",
   "昭和56年5月31日以前建築の木造住宅が対象","taishin"),

  ("chichibu","秩父市",
   "屋根置き太陽光・蓄電池及び高効率照明機器補助金",
   "太陽光 10万円/kW（上限200万円）・蓄電池 設置費の1/3（上限40万円）・LED照明 費の1/2（上限50万円）",
   "https://www.city.chichibu.lg.jp/10908.html",
   "太陽光10万円/kWと上限200万円は埼玉県内でも高水準","taiyoko",
   "創業支援補助金","法人設立費・備品・広報費の一部補助",
   "https://www.city.chichibu.lg.jp/",
   "市内での新規創業が対象","sogyo"),

  ("hanno","飯能市",
   "住宅用省エネ設備推進補助制度","太陽光・蓄電池・エネファーム等対象",
   "https://www.city.hanno.lg.jp/soshikikarasagasu/kankyokeizaibu/kankyomidorimizuka/1541.html",
   "戸建住宅・店舗兼用住宅が対象","saiene",
   "住まい事業補助金（移住・定住支援）","定住促進補助",
   "https://www.city.hanno.lg.jp/soshikikarasagaru/kensetsubu/toshikeikakuka/iju/2396.html",
   "市外からの転入者向け定住促進補助","iju"),

  ("honjo","本庄市",
   "住宅用太陽光発電システム設置補助金","設置費の一部補助（太陽光・蓄電池・エネファーム等）",
   "https://www.city.honjo.lg.jp/soshiki/keizaikankyo/kankyosuishin/tantoujouhou/global_warming/hojo/index.html",
   "令和8年3月31日までに電力受給開始のもの（令和7年度条件）","taiyoko",
   "住宅省エネ改修補助金","工事費10万円以上の省エネ改修（屋根遮熱塗装・断熱ガラス等）",
   "https://www.smart-hojokin.jp/subsidies/24014",
   "屋根高遮熱塗装・断熱ガラス・断熱材・遮熱フィルムが対象","reform"),

  ("higashimatsuyama","東松山市",
   "既存住宅太陽光発電設備設置奨励金","地域通貨「ぼたん圓」で交付",
   "https://www.solar-partners.jp/region/11212",
   "補助金が地域通貨「ぼたん圓」で交付される独自制度","taiyoko",
   "省エネ設備補助（埼玉県）","太陽光 7万円/kW（上限35万円）",
   "https://www.pref.saitama.lg.jp/a0503/hojyokin2.html",
   "埼玉県補助金と組み合わせ可能","pref"),

  ("sayama","狭山市",
   "クリーンエネルギー推進補助制度","太陽光 10万円（10kW未満は4万円）・蓄電池 5万円・EV 10万円",
   "https://www.city.sayama.saitama.jp/kurashi/ecopet/jyosei/hozyoseido.html",
   "PPA・リース契約も対象・EV・FCVも補助対象","saiene",
   "省エネエアコン普及促進補助金","省エネエアコンへの買換えに補助",
   "https://www.city.sayama.saitama.jp/",
   "令和8年度継続は市役所環境課に要確認","kaden"),

  ("hanyu","羽生市",
   "住宅用再生可能エネルギー設備等設置補助金","太陽光 2万円/kW（上限5万円）・蓄電池も対象",
   "https://www.city.hanyu.lg.jp/docs/2014122600060/",
   "太陽光は2万円/kW・上限5万円のシンプルな制度","taiyoko",
   "省エネ家電買換促進補助金","エアコン・冷蔵庫への買換えに補助",
   "https://www.city.hanyu.lg.jp/docs/2026030400014",
   "2026年3月から受付開始","kaden"),

  ("konosu","鴻巣市",
   "住宅用省エネルギー設備設置費補助金","蓄電システム上限5万円・蓄電システム+太陽光 上限10万円・V2H 上限5万円",
   "http://www.city.kounosu.saitama.jp/page/22570.html",
   "太陽光単独は対象外・蓄電池とセットで申請が必要","saiene",
   "新築住宅向け省エネ設備補助","蓄電システム+太陽光 上限10万円（新築住宅）",
   "https://hojyokin-portal.jp/subsidies/58019",
   "新築住宅専用の補助制度","new_build"),

  ("fukaya","深谷市",
   "住宅用省エネ設備設置費補助金","太陽光 6万円/件・蓄電池 10万円/件",
   "https://www.city.fukaya.saitama.jp/topics/17864.html",
   "市独自の補助制度・令和8年度詳細は2026年4月以降公表予定","saiene",
   "創業支援（商工会議所連携）","創業セミナー・補助金申請支援",
   "https://www.city.fukaya.saitama.jp/",
   "埼玉県産業振興公社等と連携した創業支援体制","sogyo"),

  ("ageo","上尾市",
   "省エネ・再エネ補助金（埼玉県）","太陽光 7万円/kW（上限35万円）・蓄電池 10万円/件",
   "https://www.pref.saitama.lg.jp/a0503/hojyokin2.html",
   "先着順・予算到達で早期終了","taiyoko",
   "創業支援（特定創業支援等事業）","セミナー・窓口相談・専門家派遣",
   "https://www.city.ageo.lg.jp/page/041116040501.html",
   "市内での創業予定者が対象","sogyo"),
]

for row in detailed:
    cid, cname, s1name, s1amt, s1url, s1key, s1slug, s2name, s2amt, s2url, s2key, s2slug = row

    w(cid, f"2026-05-10-{s1slug}.md",
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
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "{cid}-{s1slug}-2026"
---

## {cname}の{s1name}

## 詳細・申し込みはこちら

> 公式ページ：{s1url}
""")

    w(cid, f"2026-05-10-{s2slug}.md",
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
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "{cid}-{s2slug}-2026"
---

## {cname}の{s2name}

## 詳細・申し込みはこちら

> 公式ページ：{s2url}
""")

# ─────────────── 標準テンプレート市 ───────────────
std_cities = [
    ("soka","草加市"), ("warabi","蕨市"), ("toda","戸田市"), ("iruma","入間市"),
    ("asaka","朝霞市"), ("shiki","志木市"), ("wako","和光市"), ("kuki","久喜市"),
    ("kitamoto","北本市"), ("yashio","八潮市"), ("fujimi","富士見市"),
    ("misato","三郷市"), ("hasuda","蓮田市"), ("satte","幸手市"),
    ("tsurugashima","鶴ヶ島市"), ("hidaka","日高市"), ("yoshikawa","吉川市"),
    ("fujimino","ふじみ野市"), ("shiraoka","白岡市"),
]

for cid, cname in std_cities:
    w(cid,"2026-05-10-taiyoko.md",
f"""---
title: "【2026年最新】{cname}の太陽光発電・蓄電池補助金｜埼玉県補助金7万円/kW活用"
municipality: {cid}
target: {cname}内の既存住宅に太陽光発電・蓄電池を設置する個人
amount: 埼玉県補助金：太陽光発電 7万円/kW（上限35万円）、蓄電池 10万円/件
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 埼玉県「家庭における省エネ・再エネ活用設備導入補助金」が利用可能
  - 太陽光7万円/kW・上限35万円、蓄電池10万円/件
  - 市独自の補助金は市役所窓口に要確認
  - 先着順のため早めの申請が重要
source_url: "https://www.pref.saitama.lg.jp/a0503/hojyokin2.html"
summary_ja: {cname}在住の方が利用できる埼玉県の太陽光・蓄電池補助金。7万円/kW・蓄電池10万円で全県共通の制度。
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "{cid}-taiyoko-2026"
---

## {cname}在住の方が使える太陽光発電・蓄電池補助金

| 設備 | 補助額 | 上限 |
|------|--------|------|
| 太陽光発電 | 7万円/kW | 35万円 |
| 蓄電池 | 10万円/件 | ― |

## 詳細・申し込みはこちら

> 埼玉県公式ページ：https://www.pref.saitama.lg.jp/a0503/hojyokin2.html
""")

    w(cid,"2026-05-10-shoene.md",
f"""---
title: "【2026年最新】{cname}の省エネリフォーム支援｜国の住宅省エネ2026キャンペーン活用"
municipality: {cid}
target: {cname}内の住宅で省エネリフォームを行う方
amount: 断熱改修・高効率給湯機等で最大120万円（国事業）
deadline: "2027-03-31"
tags:
  - 省エネ
  - リフォーム
  - 断熱
key_points:
  - 国の「住宅省エネ2026キャンペーン」で最大120万円
  - 埼玉県補助金と組み合わせ可能
source_url: "https://jutaku-shoene2026.mlit.go.jp/"
summary_ja: {cname}在住の方も使える国の住宅省エネ2026キャンペーン。断熱改修・給湯機等に最大120万円。
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "{cid}-shoene-2026"
---

## {cname}在住の方も使える「住宅省エネ2026キャンペーン」

> 公式ページ：https://jutaku-shoene2026.mlit.go.jp/
""")

# ─────────────── 町村 ───────────────
towns = [
    ("ina_machi","伊奈町"), ("miyoshi_machi","三芳町"), ("moroyama","毛呂山町"),
    ("ogose","越生町"), ("namegawa","滑川町"), ("ranzan","嵐山町"),
    ("ogawa_s","小川町"), ("kawashima_s","川島町"), ("yoshimi","吉見町"),
    ("hatoyama","鳩山町"), ("tokigawa","ときがわ町"), ("yokoze","横瀬町"),
    ("minano","皆野町"), ("nagatoro","長瀞町"), ("ogano","小鹿野町"),
    ("higashichichibu","東秩父村"), ("misato_machi","美里町"),
    ("kamikawa_s","神川町"), ("kamisato","上里町"), ("yorii","寄居町"),
    ("miyashiro","宮代町"), ("sugito","杉戸町"), ("matsubushi","松伏町"),
]

for tid, tname in towns:
    w(tid,"2026-05-10-taiyoko.md",
f"""---
title: "【2026年最新】{tname}の太陽光発電・蓄電池補助金｜埼玉県補助金7万円/kW活用"
municipality: {tid}
target: {tname}内の既存住宅に太陽光発電・蓄電池を設置する個人
amount: 埼玉県補助金：太陽光発電 7万円/kW（上限35万円）、蓄電池 10万円/件
deadline: "2027-03-31"
tags:
  - 太陽光発電
  - 蓄電池
  - 省エネ
key_points:
  - 埼玉県「家庭における省エネ・再エネ活用設備導入補助金」が利用可能
  - 太陽光7万円/kW・上限35万円、蓄電池10万円/件
  - 町独自の補助金は役場窓口に要確認
source_url: "https://www.pref.saitama.lg.jp/a0503/hojyokin2.html"
summary_ja: {tname}在住の方が利用できる埼玉県の太陽光・蓄電池補助金。7万円/kW・蓄電池10万円で全県共通の制度。
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "{tid}-taiyoko-2026"
---

## {tname}在住の方が使える太陽光発電・蓄電池補助金

| 設備 | 補助額 | 上限 |
|------|--------|------|
| 太陽光発電 | 7万円/kW | 35万円 |
| 蓄電池 | 10万円/件 | ― |

## 詳細・申し込みはこちら

> 埼玉県公式ページ：https://www.pref.saitama.lg.jp/a0503/hojyokin2.html
""")

    w(tid,"2026-05-10-shoene.md",
f"""---
title: "【2026年最新】{tname}の省エネリフォーム支援｜国の住宅省エネキャンペーン活用"
municipality: {tid}
target: {tname}内の住宅で省エネリフォームを行う方
amount: 断熱改修・高効率給湯機等で最大120万円（国事業）
deadline: "2027-03-31"
tags:
  - 省エネ
  - リフォーム
key_points:
  - 国の「住宅省エネ2026キャンペーン」で最大120万円
  - 埼玉県補助金と組み合わせ可能
source_url: "https://jutaku-shoene2026.mlit.go.jp/"
summary_ja: {tname}在住の方も使える国の住宅省エネキャンペーン。断熱改修・給湯機等に最大120万円。
scraped_at: "2026-05-10T00:00:00Z"
is_active: true
content_hash: "{tid}-shoene-2026"
---

## {tname}在住の方も使える「住宅省エネ2026キャンペーン」

> 公式ページ：https://jutaku-shoene2026.mlit.go.jp/
""")

print(f"Generated {count} files")
