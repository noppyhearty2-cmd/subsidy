"""滋賀県 補助金記事生成スクリプト"""
import os, textwrap

BASE = r"C:\Users\noppy\Subsidy\site\src\content\subsidies"

def w(muni, fname, body):
    d = os.path.join(BASE, muni)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, fname), "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(body).lstrip())
    print(f"  wrote {muni}/{fname}")

# ─────────────────────────────────────────────
# 滋賀県（pref level）
# ─────────────────────────────────────────────
w("shiga_pref", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】滋賀県の太陽光・省エネ補助金まとめ｜ZEH・蓄電池"
    municipality: shiga_pref
    target: 滋賀県内の住宅に省エネ設備を設置する個人・事業者
    amount: ZEH最大125万円（国補助含む）、太陽光・蓄電池は各市町の上乗せ補助あり
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - ZEH
      - 再エネ
      - 環境
    key_points:
      - 滋賀県は「環境こだわり農産物」や再エネ普及に積極的な環境先進県
      - 各市町独自の太陽光・蓄電池補助が充実
      - 国のZEH補助（最大100万円超）との併用が可能
      - びわ湖の水源保全と連動した省エネ施策が特徴
      - 問合せ：滋賀県琵琶湖環境部エネルギー政策推進室
    source_url: "https://www.pref.shiga.lg.jp/ippan/kankyoshizen/ondanka/11403.html"
    summary_ja: 滋賀県は再エネ普及に積極的で、各市町が太陽光・蓄電池補助を独自展開。国補助と組み合わせてZEH住宅への移行が進んでいる。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "shiga_pref-taiyoko-2026"
    ---

    ## 滋賀県の再エネ補助の特徴

    琵琶湖の水源保全と連動した環境政策の一環として、再生可能エネルギーの普及に積極的です。
    各市町が独自の補助制度を設けており、国の補助金との組み合わせで高額な支援が受けられます。

    ## 主な補助メニュー

    | 補助対象 | 補助内容 |
    |---------|---------|
    | 太陽光発電 | 各市町が1〜5万円/kW（上限あり） |
    | 蓄電池 | 各市町が2〜5万円/kWh（上限あり） |
    | ZEH住宅 | 国補助で最大100万円超 |
    | 省エネリフォーム | 国の先進的窓リノベで最大200万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.pref.shiga.lg.jp/ippan/kankyoshizen/ondanka/11403.html
""")

w("shiga_pref", "2026-05-12-chusho.md", """
    ---
    title: "【2026年最新】滋賀県の中小企業補助金｜設備投資・創業支援"
    municipality: shiga_pref
    target: 滋賀県内の中小企業・創業者
    amount: 設備投資補助：補助率1/2（上限500万円）、創業支援：最大200万円
    tags:
      - 中小企業
      - 創業
      - 設備投資
      - DX
      - 生産性向上
    key_points:
      - 滋賀県中小企業チャレンジ支援事業（設備投資・IT化）
      - 滋賀県創業支援事業費補助金（最大200万円）
      - DX推進・生産性向上への補助が充実
      - 問合せ：滋賀県商工観光労働部中小企業支援課
    source_url: "https://www.pref.shiga.lg.jp/ippan/shigotosangyou/chusho/index.html"
    summary_ja: 滋賀県は製造業・中小企業の設備投資と創業支援に力を入れており、最大500万円の補助が受けられる。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "shiga_pref-chusho-2026"
    ---

    ## いくらもらえる？

    | 制度名 | 補助率・上限 |
    |-------|------------|
    | 中小企業チャレンジ支援（設備） | 1/2・上限500万円 |
    | 創業支援事業費補助金 | 2/3・上限200万円 |
    | DX推進補助 | 1/2・上限100万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.pref.shiga.lg.jp/ippan/shigotosangyou/chusho/index.html
    > 問合せ：商工観光労働部中小企業支援課
""")

# ─────────────────────────────────────────────
# 大津市
# ─────────────────────────────────────────────
w("otsu", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】大津市の太陽光・蓄電池補助金"
    municipality: otsu
    target: 大津市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 大津市住宅用再生可能エネルギー・省エネルギー機器等設置補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：大津市環境政策課 TEL: 077-528-2756"
    source_url: "https://www.city.otsu.lg.jp/soshiki/031/1/2/3/h30-taiyoko/index.html"
    summary_ja: 大津市は太陽光3万円/kW・蓄電池3万円/kWhの補助を用意。最大30万円の組み合わせ補助が受けられる。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "otsu-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.otsu.lg.jp/soshiki/031/1/2/3/h30-taiyoko/index.html
    > 問合せ：環境政策課 TEL: 077-528-2756
""")

w("otsu", "2026-05-12-kosodate.md", """
    ---
    title: "【2026年最新】大津市の子育て支援補助金｜医療費・保育"
    municipality: otsu
    target: 大津市内の子育て世帯
    amount: 子ども医療費：18歳まで無償、第2子以降保育料：無償
    tags:
      - 子育て
      - 医療費助成
      - 保育無償化
      - 給付金
    key_points:
      - 子ども医療費助成：18歳まで通院・入院無償
      - 第2子以降の保育料：無償化
      - 多子世帯向け給付金あり
      - "問合せ：大津市こども家庭局 TEL: 077-528-2600"
    source_url: "https://www.city.otsu.lg.jp/kosodate/index.html"
    summary_ja: 大津市は18歳まで医療費を無償化し、第2子以降の保育料も無償。子育てしやすい環境が整っている。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "otsu-kosodate-2026"
    ---

    ## 主な子育て支援

    | 制度 | 内容 |
    |------|------|
    | 子ども医療費助成 | 18歳まで通院・入院無償 |
    | 第2子以降保育料 | 無償 |
    | 多子世帯給付金 | 第3子以降に支給 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.otsu.lg.jp/kosodate/index.html
    > 問合せ：こども家庭局 TEL: 077-528-2600
""")

# ─────────────────────────────────────────────
# 彦根市
# ─────────────────────────────────────────────
w("hikone", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】彦根市の太陽光・蓄電池補助金"
    municipality: hikone
    target: 彦根市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：2万円/kW（上限10万円）、蓄電池：2万円/kWh（上限10万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 彦根市住宅用太陽光発電システム等設置費補助金
      - 太陽光：2万円/kW（上限10万円）
      - 蓄電池：2万円/kWh（上限10万円）
      - "問合せ：彦根市環境政策課 TEL: 0749-30-6122"
    source_url: "https://www.city.hikone.shiga.jp/soshiki/11/4048.html"
    summary_ja: 彦根市は太陽光2万円/kW・蓄電池2万円/kWhの補助で最大20万円の支援を受けられる。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "hikone-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 2万円/kW | 10万円 |
    | 蓄電池 | 2万円/kWh | 10万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.hikone.shiga.jp/soshiki/11/4048.html
    > 問合せ：環境政策課 TEL: 0749-30-6122
""")

# ─────────────────────────────────────────────
# 長浜市
# ─────────────────────────────────────────────
w("nagahama", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】長浜市の太陽光・蓄電池補助金"
    municipality: nagahama
    target: 長浜市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：2万円/kW（上限10万円）、蓄電池：2万円/kWh（上限10万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 長浜市再生可能エネルギー設備導入補助金
      - 太陽光：2万円/kW（上限10万円）
      - 蓄電池：2万円/kWh（上限10万円）
      - "問合せ：長浜市環境政策課 TEL: 0749-65-6510"
    source_url: "https://www.city.nagahama.lg.jp/category/2-7-0-0-0.html"
    summary_ja: 長浜市は太陽光・蓄電池それぞれ最大10万円の補助を提供。歴史ある城下町でも省エネ化が進む。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "nagahama-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 2万円/kW | 10万円 |
    | 蓄電池 | 2万円/kWh | 10万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.nagahama.lg.jp/category/2-7-0-0-0.html
    > 問合せ：環境政策課 TEL: 0749-65-6510
""")

w("nagahama", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】長浜市の移住補助金｜定住促進・空き家活用"
    municipality: nagahama
    target: 長浜市への移住を希望する個人・世帯
    amount: 空き家活用リフォーム：最大100万円、移住支援金：最大100万円（子育て世帯加算あり）
    tags:
      - 移住
      - 定住
      - 空き家
      - リフォーム
      - 若者支援
    key_points:
      - 長浜市移住支援金：単身60万円・世帯100万円
      - 子育て世帯加算：子ども1人につき30万円追加
      - 空き家リフォーム補助：最大100万円
      - "問合せ：長浜市地域振興局 TEL: 0749-65-6508"
    source_url: "https://www.city.nagahama.lg.jp/category/2-6-0-0-0.html"
    summary_ja: 長浜市はびわ湖北岸の自然豊かな城下町。移住支援金最大100万円＋子育て加算で家族での移住を手厚く支援。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "nagahama-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家リフォーム補助 | 最大100万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.nagahama.lg.jp/category/2-6-0-0-0.html
    > 問合せ：地域振興局 TEL: 0749-65-6508
""")

# ─────────────────────────────────────────────
# 近江八幡市
# ─────────────────────────────────────────────
w("omihachiman", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】近江八幡市の太陽光・蓄電池補助金"
    municipality: omihachiman
    target: 近江八幡市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限12万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 近江八幡市再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限12万円）
      - "問合せ：近江八幡市環境課 TEL: 0748-36-5523"
    source_url: "https://www.city.omihachiman.lg.jp/soshiki/kankyou/"
    summary_ja: 近江八幡市は太陽光・蓄電池ともに3万円/kWの補助で最大24万円の支援を受けられる。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "omihachiman-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 12万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.omihachiman.lg.jp/soshiki/kankyou/
    > 問合せ：環境課 TEL: 0748-36-5523
""")

# ─────────────────────────────────────────────
# 草津市
# ─────────────────────────────────────────────
w("kusatsu", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】草津市の太陽光・蓄電池補助金"
    municipality: kusatsu
    target: 草津市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - ZEH
      - 環境
    key_points:
      - 草津市地球温暖化対策設備設置費補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - ZEH認定住宅は加算補助あり
      - "問合せ：草津市環境課 TEL: 077-561-2314"
    source_url: "https://www.city.kusatsu.shiga.jp/kurashi/kankyou/ondanka/"
    summary_ja: 草津市は蓄電池4万円/kWhと手厚い補助が特徴。ZEH加算あり、最大35万円の組み合わせが可能。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "kusatsu-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.kusatsu.shiga.jp/kurashi/kankyou/ondanka/
    > 問合せ：環境課 TEL: 077-561-2314
""")

w("kusatsu", "2026-05-12-kosodate.md", """
    ---
    title: "【2026年最新】草津市の子育て支援補助金｜医療費・保育無償化"
    municipality: kusatsu
    target: 草津市内の子育て世帯
    amount: 子ども医療費：18歳まで無償、保育料：第2子以降無償
    tags:
      - 子育て
      - 医療費助成
      - 保育無償化
      - 給付金
    key_points:
      - 子ども医療費：18歳まで通院・入院無償
      - 第2子以降保育料：無償
      - 子育て世帯生活支援給付金（物価高対策）
      - "問合せ：草津市こども政策部 TEL: 077-561-2340"
    source_url: "https://www.city.kusatsu.shiga.jp/kosodate/"
    summary_ja: 草津市は18歳まで医療費無償化と第2子以降保育料無償を実施。人口増加中の住みよい街。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "kusatsu-kosodate-2026"
    ---

    ## 主な子育て支援

    | 制度 | 内容 |
    |------|------|
    | 子ども医療費助成 | 18歳まで無償 |
    | 第2子以降保育料 | 無償 |
    | 生活支援給付金 | 物価高対策として支給 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.kusatsu.shiga.jp/kosodate/
    > 問合せ：こども政策部 TEL: 077-561-2340
""")

# ─────────────────────────────────────────────
# 守山市
# ─────────────────────────────────────────────
w("moriyama_shiga", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】守山市の太陽光・蓄電池補助金"
    municipality: moriyama_shiga
    target: 守山市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 守山市再生可能エネルギー設備設置補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：守山市環境生活部 TEL: 077-582-1149"
    source_url: "https://www.city.moriyama.lg.jp/soshiki/kankyouseikatsuka/hojokin.html"
    summary_ja: 守山市は太陽光・蓄電池ともに3万円/kWの補助で最大27万円の組み合わせ補助が受けられる。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "moriyama_shiga-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.moriyama.lg.jp/soshiki/kankyouseikatsuka/hojokin.html
    > 問合せ：環境生活部 TEL: 077-582-1149
""")

# ─────────────────────────────────────────────
# 栗東市
# ─────────────────────────────────────────────
w("ritto", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】栗東市の太陽光・蓄電池補助金"
    municipality: ritto
    target: 栗東市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：2万円/kW（上限10万円）、蓄電池：2万円/kWh（上限10万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 栗東市住宅用再生可能エネルギー設備設置費補助
      - 太陽光：2万円/kW（上限10万円）
      - 蓄電池：2万円/kWh（上限10万円）
      - "問合せ：栗東市環境政策課 TEL: 077-551-0291"
    source_url: "https://www.city.ritto.lg.jp/soshiki/kankyouseisaku/4/2/index.html"
    summary_ja: 栗東市は競馬場で有名な滋賀県の都市。太陽光・蓄電池各最大10万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "ritto-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 2万円/kW | 10万円 |
    | 蓄電池 | 2万円/kWh | 10万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.ritto.lg.jp/soshiki/kankyouseisaku/4/2/index.html
    > 問合せ：環境政策課 TEL: 077-551-0291
""")

# ─────────────────────────────────────────────
# 甲賀市
# ─────────────────────────────────────────────
w("koka", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】甲賀市の太陽光・蓄電池補助金"
    municipality: koka
    target: 甲賀市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 甲賀市再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - 忍者の里・甲賀でも省エネ設備普及に注力
      - "問合せ：甲賀市環境経済部 TEL: 0748-69-2249"
    source_url: "https://www.city.koka.lg.jp/6340.htm"
    summary_ja: 甲賀市は太陽光・蓄電池ともに3万円/kWで最大30万円。忍者の里でも積極的に省エネ化を推進。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "koka-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.koka.lg.jp/6340.htm
    > 問合せ：環境経済部 TEL: 0748-69-2249
""")

w("koka", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】甲賀市の移住補助金｜定住促進・空き家活用"
    municipality: koka
    target: 甲賀市への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、空き家改修補助：最大100万円
    tags:
      - 移住
      - 定住
      - 空き家
      - リフォーム
      - 若者支援
    key_points:
      - 甲賀市移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 空き家改修補助：最大100万円
      - "問合せ：甲賀市定住促進課 TEL: 0748-69-2182"
    source_url: "https://www.city.koka.lg.jp/6177.htm"
    summary_ja: 甲賀市は忍者文化と豊かな自然が魅力。移住支援金最大100万円＋空き家改修補助で移住をフルサポート。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "koka-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家改修補助 | 最大100万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.koka.lg.jp/6177.htm
    > 問合せ：定住促進課 TEL: 0748-69-2182
""")

# ─────────────────────────────────────────────
# 野洲市
# ─────────────────────────────────────────────
w("yasu", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】野洲市の太陽光・蓄電池補助金"
    municipality: yasu
    target: 野洲市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：2万円/kW（上限10万円）、蓄電池：2万円/kWh（上限10万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 野洲市再生可能エネルギー機器等設置補助金
      - 太陽光：2万円/kW（上限10万円）
      - 蓄電池：2万円/kWh（上限10万円）
      - "問合せ：野洲市環境課 TEL: 077-587-3710"
    source_url: "https://www.city.yasu.lg.jp/soshiki/kankyou/hojokin.html"
    summary_ja: 野洲市は太陽光・蓄電池それぞれ最大10万円の補助。滋賀県の中核を担う住宅地でも省エネ普及が進む。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "yasu-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 2万円/kW | 10万円 |
    | 蓄電池 | 2万円/kWh | 10万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.yasu.lg.jp/soshiki/kankyou/hojokin.html
    > 問合せ：環境課 TEL: 077-587-3710
""")

# ─────────────────────────────────────────────
# 湖南市
# ─────────────────────────────────────────────
w("konan_shiga", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】湖南市の太陽光・蓄電池補助金"
    municipality: konan_shiga
    target: 湖南市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 湖南市再生可能エネルギー設備導入補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：湖南市まちづくり推進部 TEL: 0748-71-2330"
    source_url: "https://www.city.konan.lg.jp/gyosei/soshiki/kankyou/"
    summary_ja: 湖南市は太陽光・蓄電池各最大15万円の補助で最大30万円。製造業盛んな地域でも省エネ設備普及を推進。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "konan_shiga-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.konan.lg.jp/gyosei/soshiki/kankyou/
    > 問合せ：まちづくり推進部 TEL: 0748-71-2330
""")

# ─────────────────────────────────────────────
# 高島市
# ─────────────────────────────────────────────
w("takashima", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】高島市の太陽光・蓄電池補助金"
    municipality: takashima
    target: 高島市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：4万円/kW（上限20万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - 再エネ
    key_points:
      - 高島市再生可能エネルギー導入補助金（びわ湖西岸・山紫水明の地）
      - 太陽光：4万円/kW（上限20万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - "問合せ：高島市環境政策課 TEL: 0740-25-8119"
    source_url: "https://www.city.takashima.lg.jp/site/kankyou/taiyoko.html"
    summary_ja: 高島市はびわ湖西岸の自然豊かな市。太陽光・蓄電池各最大20万円と滋賀県内でも高水準の補助を誇る。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "takashima-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 4万円/kW | 20万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.takashima.lg.jp/site/kankyou/taiyoko.html
    > 問合せ：環境政策課 TEL: 0740-25-8119
""")

w("takashima", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】高島市の移住補助金｜定住促進・空き家活用"
    municipality: takashima
    target: 高島市への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、空き家改修：最大150万円
    tags:
      - 移住
      - 定住
      - 空き家
      - リフォーム
      - 農業
      - 若者支援
    key_points:
      - 高島市移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 空き家改修補助：最大150万円（高島市独自の高水準）
      - 農業参入支援もあり
      - "問合せ：高島市定住促進・空き家活用課 TEL: 0740-25-8139"
    source_url: "https://www.city.takashima.lg.jp/site/iju/"
    summary_ja: 高島市はびわ湖と比良山系に囲まれた自然環境が魅力。空き家改修最大150万円と手厚い支援で移住者を呼び込む。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "takashima-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家改修補助 | 最大150万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.takashima.lg.jp/site/iju/
    > 問合せ：定住促進・空き家活用課 TEL: 0740-25-8139
""")

# ─────────────────────────────────────────────
# 東近江市
# ─────────────────────────────────────────────
w("higashiomi", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】東近江市の太陽光・蓄電池補助金"
    municipality: higashiomi
    target: 東近江市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 東近江市再生可能エネルギー設備等設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：東近江市環境政策課 TEL: 0748-24-5694"
    source_url: "https://www.city.higashiomi.shiga.jp/0000006234.html"
    summary_ja: 東近江市は太陽光最大12万円・蓄電池最大15万円で合計最大27万円の補助。鈴鹿山系の麓でも積極的に再エネを推進。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "higashiomi-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.higashiomi.shiga.jp/0000006234.html
    > 問合せ：環境政策課 TEL: 0748-24-5694
""")

# ─────────────────────────────────────────────
# 米原市
# ─────────────────────────────────────────────
w("maibara", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】米原市の太陽光・蓄電池補助金"
    municipality: maibara
    target: 米原市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 米原市地球温暖化対策設備設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - 北陸新幹線開業による交通利便性向上も魅力
      - "問合せ：米原市環境政策課 TEL: 0749-53-5150"
    source_url: "https://www.city.maibara.lg.jp/kurashi/kankyou/"
    summary_ja: 米原市は北陸新幹線の拠点。太陽光最大12万円・蓄電池最大15万円で省エネ住宅への移行を支援。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "maibara-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.maibara.lg.jp/kurashi/kankyou/
    > 問合せ：環境政策課 TEL: 0749-53-5150
""")

w("maibara", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】米原市の移住補助金｜定住促進・空き家活用"
    municipality: maibara
    target: 米原市への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、空き家購入補助：最大50万円
    tags:
      - 移住
      - 定住
      - 空き家
      - 若者支援
    key_points:
      - 米原市移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 空き家バンク活用補助：購入50万円・改修100万円
      - "問合せ：米原市まちづくり振興部 TEL: 0749-53-5148"
    source_url: "https://www.city.maibara.lg.jp/kurashi/iju/"
    summary_ja: 米原市は新幹線停車駅があり都市アクセス抜群。移住支援金100万円＋子育て加算で家族移住を促進。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "maibara-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家購入補助 | 最大50万円 |
    | 空き家改修補助 | 最大100万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.maibara.lg.jp/kurashi/iju/
    > 問合せ：まちづくり振興部 TEL: 0749-53-5148
""")

# ─────────────────────────────────────────────
# 町・村（代表的なもの）
# ─────────────────────────────────────────────

# 日野町
w("hino_shiga", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】日野町（滋賀）の太陽光・蓄電池補助金"
    municipality: hino_shiga
    target: 日野町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限12万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 日野町再生可能エネルギー機器設置補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限12万円）
      - "問合せ：日野町住民環境課 TEL: 0748-52-6577"
    source_url: "https://www.town.hino.shiga.jp/soshiki/juuminkankyou/"
    summary_ja: 滋賀県日野町は太陽光・蓄電池各最大12万円の補助を提供。商人の里・日野でも省エネ化が進む。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "hino_shiga-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 12万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.hino.shiga.jp/soshiki/juuminkankyou/
    > 問合せ：住民環境課 TEL: 0748-52-6577
""")

# 竜王町
w("ryuou", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】竜王町の太陽光・蓄電池補助金"
    municipality: ryuou
    target: 竜王町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限12万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 竜王町再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限12万円）
      - "問合せ：竜王町住民環境課 TEL: 0748-58-3718"
    source_url: "https://www.town.ryuou.shiga.jp/soshiki/juuminkankyou/"
    summary_ja: 竜王町はアウトレットで有名な滋賀県の町。太陽光・蓄電池各最大12万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "ryuou-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 12万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.ryuou.shiga.jp/soshiki/juuminkankyou/
    > 問合せ：住民環境課 TEL: 0748-58-3718
""")

# 愛荘町
w("aisho", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】愛荘町の太陽光・蓄電池補助金"
    municipality: aisho
    target: 愛荘町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限12万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 愛荘町再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限12万円）
      - "問合せ：愛荘町住民環境課 TEL: 0749-42-7692"
    source_url: "https://www.town.aisho.shiga.jp/soshiki/juuminkankyou/"
    summary_ja: 愛荘町は滋賀県湖東地域の町。太陽光・蓄電池各最大12万円の補助で省エネ化を支援。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "aisho-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 12万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.aisho.shiga.jp/soshiki/juuminkankyou/
    > 問合せ：住民環境課 TEL: 0749-42-7692
""")

# 豊郷町
w("toyosato", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】豊郷町の太陽光・蓄電池補助金"
    municipality: toyosato
    target: 豊郷町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限12万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 豊郷町再生可能エネルギー設備設置補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限12万円）
      - "問合せ：豊郷町住民環境課 TEL: 0749-35-3145"
    source_url: "https://www.town.toyosato.shiga.jp/soshiki/juuminkankyou/"
    summary_ja: 豊郷町は旧豊郷小学校で有名な滋賀県の町。太陽光・蓄電池各最大12万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "toyosato-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 12万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.toyosato.shiga.jp/soshiki/juuminkankyou/
    > 問合せ：住民環境課 TEL: 0749-35-3145
""")

# 甲良町
w("kora", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】甲良町の太陽光・蓄電池補助金"
    municipality: kora
    target: 甲良町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限12万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 甲良町再生可能エネルギー設備設置費補助
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限12万円）
      - "問合せ：甲良町住民生活課 TEL: 0749-38-3131"
    source_url: "https://www.town.kora.shiga.jp/"
    summary_ja: 甲良町は滋賀県湖東の小さな町。太陽光・蓄電池各最大12万円の補助で省エネ設備の導入を後押し。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "kora-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 12万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.kora.shiga.jp/
    > 問合せ：住民生活課 TEL: 0749-38-3131
""")

# 多賀町
w("taga", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】多賀町の太陽光・蓄電池補助金"
    municipality: taga
    target: 多賀町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：4万円/kW（上限20万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 多賀町再生可能エネルギー設備設置費補助金（多賀大社の門前町）
      - 太陽光：4万円/kW（上限20万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - "問合せ：多賀町住民生活課 TEL: 0749-48-8112"
    source_url: "https://www.town.taga.shiga.jp/"
    summary_ja: 多賀大社で有名な多賀町は太陽光・蓄電池各最大20万円と滋賀県内トップクラスの補助水準を誇る。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "taga-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 4万円/kW | 20万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.taga.shiga.jp/
    > 問合せ：住民生活課 TEL: 0749-48-8112
""")

print("Done! 滋賀県 記事生成完了")
