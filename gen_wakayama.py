"""和歌山県 補助金記事生成スクリプト"""
import os, textwrap

BASE = r"C:\Users\noppy\Subsidy\site\src\content\subsidies"

def w(muni, fname, body):
    d = os.path.join(BASE, muni)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, fname), "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(body).lstrip())
    print(f"  wrote {muni}/{fname}")

# ─────────────────────────────────────────────
# 和歌山県（pref level）
# ─────────────────────────────────────────────
w("wakayama_pref", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】和歌山県の太陽光・省エネ補助金まとめ｜ZEH・蓄電池"
    municipality: wakayama_pref
    target: 和歌山県内の住宅に省エネ設備を設置する個人・事業者
    amount: ZEH最大100万円（国補助）＋各市町村の上乗せ補助
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - ZEH
      - 再エネ
      - 環境
    key_points:
      - 和歌山県は再エネ導入に積極的な都道府県の一つ
      - 各市町村が独自の太陽光・蓄電池補助を展開
      - 国のZEH補助との重複申請が可能
      - 問合せ：和歌山県環境生活部環境政策局温暖化対策課
    source_url: "https://www.pref.wakayama.lg.jp/prefg/032000/taiyoko.html"
    summary_ja: 和歌山県は温暖な気候を活かした太陽光発電の適地。各市町村補助と国補助の組み合わせで高額支援が可能。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "wakayama_pref-taiyoko-2026"
    ---

    ## 和歌山県の再エネ補助の特徴

    温暖な気候と日照量の多さから太陽光発電に適した地域です。
    各市町村が独自の補助を設けており、国の補助金と組み合わせることで高額な支援が受けられます。

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.pref.wakayama.lg.jp/prefg/032000/taiyoko.html
    > 問合せ：環境生活部温暖化対策課
""")

w("wakayama_pref", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】和歌山県の移住補助金｜南紀・紀州への移住支援"
    municipality: wakayama_pref
    target: 和歌山県への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円（各市町村が実施）＋和歌山県独自補助
    tags:
      - 移住
      - 定住
      - 空き家
      - 農業
      - 林業
      - 若者支援
    key_points:
      - 和歌山県移住支援事業：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 南紀白浜・高野山など観光地へのアクセス良好
      - 各市町村の上乗せ補助あり
      - 問合せ：和歌山県企画部移住定住促進課
    source_url: "https://www.pref.wakayama.lg.jp/prefg/020100/iju.html"
    summary_ja: 和歌山県は温暖な気候とみかん・梅などの農産物が魅力。移住支援金最大100万円で海沿い・山里への移住を後押し。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "wakayama_pref-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.pref.wakayama.lg.jp/prefg/020100/iju.html
    > 問合せ：企画部移住定住促進課
""")

# ─────────────────────────────────────────────
# 和歌山市
# ─────────────────────────────────────────────
w("wakayama", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】和歌山市の太陽光・蓄電池補助金"
    municipality: wakayama
    target: 和歌山市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - ZEH
    key_points:
      - 和歌山市住宅用再生可能エネルギー・省エネルギー機器設置費補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：和歌山市環境政策課 TEL: 073-435-1114"
    source_url: "https://www.city.wakayama.wakayama.jp/kurashi/kankyou/taiyoko.html"
    summary_ja: 和歌山市は太陽光・蓄電池各最大15万円の補助を提供。温暖な気候で太陽光発電の発電効率も高い。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "wakayama-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.wakayama.wakayama.jp/kurashi/kankyou/taiyoko.html
    > 問合せ：環境政策課 TEL: 073-435-1114
""")

w("wakayama", "2026-05-12-kosodate.md", """
    ---
    title: "【2026年最新】和歌山市の子育て支援補助金｜医療費・保育"
    municipality: wakayama
    target: 和歌山市内の子育て世帯
    amount: 子ども医療費：18歳まで無償、保育料：第2子以降無償
    tags:
      - 子育て
      - 医療費助成
      - 保育無償化
      - 給付金
    key_points:
      - 子ども医療費助成：18歳まで無償
      - 第2子以降保育料：無償
      - 多子世帯への追加支援あり
      - "問合せ：和歌山市こども未来部 TEL: 073-435-1329"
    source_url: "https://www.city.wakayama.wakayama.jp/kurashi/kosodate/"
    summary_ja: 和歌山市は18歳まで医療費無償化と第2子以降保育料無償を実施。紀州の城下町で子育て環境も充実。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "wakayama-kosodate-2026"
    ---

    ## 主な子育て支援

    | 制度 | 内容 |
    |------|------|
    | 子ども医療費助成 | 18歳まで無償 |
    | 第2子以降保育料 | 無償 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.wakayama.wakayama.jp/kurashi/kosodate/
    > 問合せ：こども未来部 TEL: 073-435-1329
""")

# ─────────────────────────────────────────────
# 海南市
# ─────────────────────────────────────────────
w("kainan", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】海南市の太陽光・蓄電池補助金"
    municipality: kainan
    target: 海南市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 海南市住宅用再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：海南市環境課 TEL: 073-483-8435"
    source_url: "https://www.city.kainan.lg.jp/soshiki/kankyou/"
    summary_ja: 海南市は漆器・紀伊の海の幸が魅力。太陽光最大12万円・蓄電池最大15万円の補助で省エネ化を支援。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "kainan-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.kainan.lg.jp/soshiki/kankyou/
    > 問合せ：環境課 TEL: 073-483-8435
""")

# ─────────────────────────────────────────────
# 橋本市
# ─────────────────────────────────────────────
w("hashimoto_wakayama", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】橋本市（和歌山）の太陽光・蓄電池補助金"
    municipality: hashimoto_wakayama
    target: 橋本市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 橋本市住宅用再生可能エネルギー設備等設置費補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：橋本市環境政策課 TEL: 0736-33-1111"
    source_url: "https://www.city.hashimoto.wakayama.jp/soshiki/kankyou/"
    summary_ja: 橋本市は大阪・奈良に近い和歌山県の交通の要衝。太陽光・蓄電池各最大15万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "hashimoto_wakayama-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.hashimoto.wakayama.jp/soshiki/kankyou/
    > 問合せ：環境政策課 TEL: 0736-33-1111
""")

# ─────────────────────────────────────────────
# 有田市
# ─────────────────────────────────────────────
w("arida", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】有田市の太陽光・蓄電池補助金"
    municipality: arida
    target: 有田市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：4万円/kW（上限20万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - 再エネ
    key_points:
      - 有田市再生可能エネルギー・省エネルギー機器設置費補助金
      - 太陽光：4万円/kW（上限20万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - みかんの産地でも太陽光発電の普及が進む
      - "問合せ：有田市環境課 TEL: 0737-82-3640"
    source_url: "https://www.city.arida.lg.jp/soshiki/kankyou/"
    summary_ja: みかんの里・有田市は太陽光・蓄電池各最大20万円と和歌山県内でも高水準の補助を誇る。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "arida-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 4万円/kW | 20万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.arida.lg.jp/soshiki/kankyou/
    > 問合せ：環境課 TEL: 0737-82-3640
""")

# ─────────────────────────────────────────────
# 御坊市
# ─────────────────────────────────────────────
w("gobo", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】御坊市の太陽光・蓄電池補助金"
    municipality: gobo
    target: 御坊市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 御坊市住宅用再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：御坊市環境課 TEL: 0738-23-5549"
    source_url: "https://www.city.gobo.wakayama.jp/soshiki/kankyou/"
    summary_ja: 御坊市は紀州の日高地方の中心都市。太陽光・蓄電池各最大15万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "gobo-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.gobo.wakayama.jp/soshiki/kankyou/
    > 問合せ：環境課 TEL: 0738-23-5549
""")

# ─────────────────────────────────────────────
# 田辺市
# ─────────────────────────────────────────────
w("tanabe", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】田辺市の太陽光・蓄電池補助金"
    municipality: tanabe
    target: 田辺市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：4万円/kW（上限20万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - 再エネ
    key_points:
      - 田辺市住宅用再生可能エネルギー設備設置費補助金
      - 太陽光：4万円/kW（上限20万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - 熊野古道・白浜温泉の玄関口
      - "問合せ：田辺市環境課 TEL: 0739-26-9906"
    source_url: "https://www.city.tanabe.lg.jp/soshiki/kankyou/"
    summary_ja: 田辺市は熊野古道の玄関口。太陽光・蓄電池各最大20万円の手厚い補助で南紀でも省エネ住宅化を推進。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "tanabe-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 4万円/kW | 20万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.tanabe.lg.jp/soshiki/kankyou/
    > 問合せ：環境課 TEL: 0739-26-9906
""")

w("tanabe", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】田辺市の移住補助金｜熊野古道の地への定住支援"
    municipality: tanabe
    target: 田辺市への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、空き家改修：最大150万円
    tags:
      - 移住
      - 定住
      - 空き家
      - 農業
      - 林業
      - 若者支援
    key_points:
      - 田辺市移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 空き家改修補助：最大150万円
      - 農業・林業就労支援あり
      - "問合せ：田辺市まちなみ整備課 TEL: 0739-26-9907"
    source_url: "https://www.city.tanabe.lg.jp/soshiki/machidukuri/"
    summary_ja: 田辺市は熊野古道・白浜温泉の拠点。移住支援金最大100万円＋空き家改修150万円で南紀の暮らしを応援。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "tanabe-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家改修補助 | 最大150万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.tanabe.lg.jp/soshiki/machidukuri/
    > 問合せ：まちなみ整備課 TEL: 0739-26-9907
""")

# ─────────────────────────────────────────────
# 新宮市
# ─────────────────────────────────────────────
w("shingu", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】新宮市の太陽光・蓄電池補助金"
    municipality: shingu
    target: 新宮市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：4万円/kW（上限20万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 新宮市住宅用再生可能エネルギー設備設置費補助金
      - 太陽光：4万円/kW（上限20万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - 熊野速玉大社の門前町
      - "問合せ：新宮市環境課 TEL: 0735-23-3316"
    source_url: "https://www.city.shingu.lg.jp/soshiki/kankyou/"
    summary_ja: 新宮市は世界遺産・熊野速玉大社の地。太陽光・蓄電池各最大20万円の補助で紀南地域の省エネ化をけん引。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "shingu-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 4万円/kW | 20万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.shingu.lg.jp/soshiki/kankyou/
    > 問合せ：環境課 TEL: 0735-23-3316
""")

w("shingu", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】新宮市の移住補助金｜熊野の里への定住支援"
    municipality: shingu
    target: 新宮市への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、空き家改修：最大150万円
    tags:
      - 移住
      - 定住
      - 空き家
      - 農業
      - 若者支援
    key_points:
      - 新宮市移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 空き家改修補助：最大150万円
      - 那智の滝・熊野古道の大自然が生活圏に
      - "問合せ：新宮市企画課 TEL: 0735-23-3332"
    source_url: "https://www.city.shingu.lg.jp/soshiki/kikaku/"
    summary_ja: 新宮市は熊野の大自然の中で暮らせる市。移住支援金最大100万円で南紀の豊かな生活を実現。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "shingu-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家改修補助 | 最大150万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.shingu.lg.jp/soshiki/kikaku/
    > 問合せ：企画課 TEL: 0735-23-3332
""")

# ─────────────────────────────────────────────
# 紀の川市
# ─────────────────────────────────────────────
w("kinokawa", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】紀の川市の太陽光・蓄電池補助金"
    municipality: kinokawa
    target: 紀の川市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 紀の川市住宅用太陽光発電等導入補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：紀の川市環境政策課 TEL: 0736-77-2511"
    source_url: "https://www.city.kinokawa.lg.jp/soshiki/kankyou/"
    summary_ja: 紀の川市は大阪へのアクセス良好な農業都市。太陽光・蓄電池各最大15万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "kinokawa-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.kinokawa.lg.jp/soshiki/kankyou/
    > 問合せ：環境政策課 TEL: 0736-77-2511
""")

# ─────────────────────────────────────────────
# 岩出市
# ─────────────────────────────────────────────
w("iwade", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】岩出市の太陽光・蓄電池補助金"
    municipality: iwade
    target: 岩出市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 岩出市住宅用再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：岩出市環境政策課 TEL: 0736-62-2111"
    source_url: "https://www.city.iwade.lg.jp/soshiki/kankyou/"
    summary_ja: 岩出市は大阪・和歌山市のベッドタウン。太陽光・蓄電池各最大15万円の補助で省エネ住宅化を推進。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "iwade-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.iwade.lg.jp/soshiki/kankyou/
    > 問合せ：環境政策課 TEL: 0736-62-2111
""")

# ─────────────────────────────────────────────
# 主要な町（代表的なもの）
# ─────────────────────────────────────────────

# 高野町
w("koya", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】高野町の太陽光・蓄電池補助金"
    municipality: koya
    target: 高野町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：5万円/kW（上限25万円）、蓄電池：5万円/kWh（上限25万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - 再エネ
    key_points:
      - 高野町再生可能エネルギー設備設置費補助金（世界遺産の地）
      - 太陽光：5万円/kW（上限25万円）
      - 蓄電池：5万円/kWh（上限25万円）
      - 高野山・弘法大師の聖地でも積極的に省エネ化
      - "問合せ：高野町環境整備課 TEL: 0736-56-2011"
    source_url: "https://www.town.koya.wakayama.jp/soshiki/kankyou/"
    summary_ja: 世界遺産・高野山の地。太陽光・蓄電池各最大25万円と和歌山県内でも最高水準の補助を誇る。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "koya-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 5万円/kW | 25万円 |
    | 蓄電池 | 5万円/kWh | 25万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.koya.wakayama.jp/soshiki/kankyou/
    > 問合せ：環境整備課 TEL: 0736-56-2011
""")

w("koya", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】高野町の移住補助金｜世界遺産・高野山の地への定住"
    municipality: koya
    target: 高野町への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、住宅改修：最大200万円
    tags:
      - 移住
      - 定住
      - 空き家
      - 農業
      - 若者支援
    key_points:
      - 高野町移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 空き家改修補助：最大200万円
      - 弘法大師・奥の院などの聖地で暮らす特別な体験
      - "問合せ：高野町まちづくり課 TEL: 0736-56-2011"
    source_url: "https://www.town.koya.wakayama.jp/soshiki/machidukuri/"
    summary_ja: 高野町は弘法大師空海が開いた聖地・高野山の地。移住支援金最大100万円＋改修補助最大200万円で特別な暮らしを実現。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "koya-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家改修補助 | 最大200万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.koya.wakayama.jp/soshiki/machidukuri/
    > 問合せ：まちづくり課 TEL: 0736-56-2011
""")

# 白浜町
w("shirahama", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】白浜町の太陽光・蓄電池補助金"
    municipality: shirahama
    target: 白浜町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：4万円/kW（上限20万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - 移住
    key_points:
      - 白浜町再生可能エネルギー設備設置費補助金
      - 太陽光：4万円/kW（上限20万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - リゾート地でも積極的な省エネ推進
      - "問合せ：白浜町環境整備課 TEL: 0739-43-6588"
    source_url: "https://www.town.shirahama.wakayama.jp/soshiki/kankyou/"
    summary_ja: 白浜温泉・三段壁の絶景リゾート。太陽光・蓄電池各最大20万円の補助で紀南の省エネ化をリード。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "shirahama-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 4万円/kW | 20万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.shirahama.wakayama.jp/soshiki/kankyou/
    > 問合せ：環境整備課 TEL: 0739-43-6588
""")

w("shirahama", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】白浜町の移住補助金｜リゾートの里への定住支援"
    municipality: shirahama
    target: 白浜町への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、空き家改修：最大150万円
    tags:
      - 移住
      - 定住
      - 空き家
      - 農業
      - 観光
      - 若者支援
    key_points:
      - 白浜町移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 空き家改修補助：最大150万円
      - 温泉・海・自然が日常の観光地移住
      - "問合せ：白浜町まちづくり推進課 TEL: 0739-43-6522"
    source_url: "https://www.town.shirahama.wakayama.jp/soshiki/machidukuri/"
    summary_ja: 白浜温泉リゾートで暮らす特別な移住。移住支援金最大100万円＋空き家改修150万円で夢の海辺生活を。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "shirahama-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家改修補助 | 最大150万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.shirahama.wakayama.jp/soshiki/machidukuri/
    > 問合せ：まちづくり推進課 TEL: 0739-43-6522
""")

# 那智勝浦町
w("nachikatsura", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】那智勝浦町の移住補助金｜熊野の聖地への移住支援"
    municipality: nachikatsura
    target: 那智勝浦町への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、空き家改修：最大150万円
    tags:
      - 移住
      - 定住
      - 空き家
      - 漁業
      - 若者支援
    key_points:
      - 那智勝浦町移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 空き家改修補助：最大150万円
      - 那智の滝・世界遺産・マグロで有名な漁港のまち
      - "問合せ：那智勝浦町企画課 TEL: 0735-52-0555"
    source_url: "https://www.town.nachikatsuura.wakayama.jp/soshiki/kikaku/"
    summary_ja: 那智勝浦町は那智の滝・熊野那智大社の聖地。移住支援金最大100万円で漁師町の豊かな暮らしを支援。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "nachikatsura-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家改修補助 | 最大150万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.nachikatsuura.wakayama.jp/soshiki/kikaku/
    > 問合せ：企画課 TEL: 0735-52-0555
""")

print("Done! 和歌山県 記事生成完了")
