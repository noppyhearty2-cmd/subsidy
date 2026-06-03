"""奈良県 補助金記事生成スクリプト"""
import os, textwrap

BASE = r"C:\Users\noppy\Subsidy\site\src\content\subsidies"

def w(muni, fname, body):
    d = os.path.join(BASE, muni)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, fname), "w", encoding="utf-8") as f:
        f.write(textwrap.dedent(body).lstrip())
    print(f"  wrote {muni}/{fname}")

# ─────────────────────────────────────────────
# 奈良県（pref level）
# ─────────────────────────────────────────────
w("nara_pref", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】奈良県の太陽光・省エネ補助金まとめ｜ZEH・蓄電池"
    municipality: nara_pref
    target: 奈良県内の住宅に省エネ設備を設置する個人・事業者
    amount: 各市町村の補助＋国補助（ZEH最大100万円超）を組み合わせ可能
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - ZEH
      - 再エネ
      - 環境
    key_points:
      - 奈良県は世界遺産を擁する歴史都市でも省エネ設備普及に積極的
      - 各市町村が独自の太陽光・蓄電池補助を展開
      - 国のZEH補助との重複申請が可能
      - 問合せ：奈良県くらし創造部スマートエネルギー課
    source_url: "https://www.pref.nara.jp/44372.htm"
    summary_ja: 奈良県は各市町村が太陽光・蓄電池補助を独自展開。歴史ある古都でも最新の省エネ設備導入を積極支援。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "nara_pref-taiyoko-2026"
    ---

    ## 奈良県の再エネ補助の特徴

    奈良市・橿原市などの主要都市から山村部まで、各市町村が独自の補助制度を設けています。
    国のZEH補助や先進的窓リノベ補助との組み合わせが可能です。

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.pref.nara.jp/44372.htm
    > 問合せ：くらし創造部スマートエネルギー課
""")

w("nara_pref", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】奈良県の移住補助金｜関西・奈良への移住を支援"
    municipality: nara_pref
    target: 奈良県への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円（各市町村が実施）、奈良県独自加算あり
    tags:
      - 移住
      - 定住
      - 空き家
      - 若者支援
      - 地域活性化
    key_points:
      - 奈良県移住支援事業：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 世界遺産・豊かな自然・大阪へのアクセスが魅力
      - 各市町村の上乗せ補助あり
      - 問合せ：奈良県地域振興部ふるさと・移住定住促進課
    source_url: "https://www.pref.nara.jp/iju/"
    summary_ja: 奈良県は大阪・京都のベッドタウンとして移住先として人気。移住支援金最大100万円で古都への移住を後押し。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "nara_pref-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.pref.nara.jp/iju/
    > 問合せ：地域振興部ふるさと・移住定住促進課
""")

# ─────────────────────────────────────────────
# 奈良市
# ─────────────────────────────────────────────
w("nara", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】奈良市の太陽光・蓄電池補助金"
    municipality: nara
    target: 奈良市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - ZEH
    key_points:
      - 奈良市住宅用再生可能エネルギー設備等設置費補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：奈良市環境政策課 TEL: 0742-34-5194"
    source_url: "https://www.city.nara.lg.jp/site/kankyou/taiyoko.html"
    summary_ja: 古都・奈良市は太陽光・蓄電池各最大15万円の補助を提供。世界遺産の街でも積極的に省エネ化を推進。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "nara-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.nara.lg.jp/site/kankyou/taiyoko.html
    > 問合せ：環境政策課 TEL: 0742-34-5194
""")

w("nara", "2026-05-12-kosodate.md", """
    ---
    title: "【2026年最新】奈良市の子育て支援補助金｜医療費・保育"
    municipality: nara
    target: 奈良市内の子育て世帯
    amount: 子ども医療費：18歳まで無償、保育料：第2子以降無償
    tags:
      - 子育て
      - 医療費助成
      - 保育無償化
      - 給付金
    key_points:
      - 子ども医療費助成：18歳まで無償
      - 第2子以降保育料：無償
      - ひとり親家庭への追加支援あり
      - "問合せ：奈良市こども未来部 TEL: 0742-34-4804"
    source_url: "https://www.city.nara.lg.jp/site/kosodate/"
    summary_ja: 奈良市は18歳まで医療費無償化と第2子以降保育料無償を実施。歴史ある古都で子育て環境も充実。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "nara-kosodate-2026"
    ---

    ## 主な子育て支援

    | 制度 | 内容 |
    |------|------|
    | 子ども医療費助成 | 18歳まで無償 |
    | 第2子以降保育料 | 無償 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.nara.lg.jp/site/kosodate/
    > 問合せ：こども未来部 TEL: 0742-34-4804
""")

# ─────────────────────────────────────────────
# 大和高田市
# ─────────────────────────────────────────────
w("yamatotakada", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】大和高田市の太陽光・蓄電池補助金"
    municipality: yamatotakada
    target: 大和高田市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限12万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 大和高田市住宅用再生可能エネルギー設備設置補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限12万円）
      - "問合せ：大和高田市環境政策課 TEL: 0745-22-1101"
    source_url: "https://www.city.yamatotakada.nara.jp/soshiki/kankyou/"
    summary_ja: 大和高田市は太陽光・蓄電池各最大12万円の補助を提供。奈良県中部の交通の要衝でも省エネ化を推進。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "yamatotakada-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 12万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.yamatotakada.nara.jp/soshiki/kankyou/
    > 問合せ：環境政策課 TEL: 0745-22-1101
""")

# ─────────────────────────────────────────────
# 大和郡山市
# ─────────────────────────────────────────────
w("yamatokoriyama", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】大和郡山市の太陽光・蓄電池補助金"
    municipality: yamatokoriyama
    target: 大和郡山市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
    key_points:
      - 大和郡山市住宅用太陽光発電等導入補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：大和郡山市環境政策課 TEL: 0743-53-1151"
    source_url: "https://www.city.yamatokoriyama.nara.jp/soshiki/kankyou/"
    summary_ja: 金魚の里・大和郡山市は太陽光最大12万円・蓄電池最大15万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "yamatokoriyama-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.yamatokoriyama.nara.jp/soshiki/kankyou/
    > 問合せ：環境政策課 TEL: 0743-53-1151
""")

# ─────────────────────────────────────────────
# 天理市
# ─────────────────────────────────────────────
w("tenri", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】天理市の太陽光・蓄電池補助金"
    municipality: tenri
    target: 天理市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 天理市住宅用再生可能エネルギー導入補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：天理市環境政策課 TEL: 0743-63-1001"
    source_url: "https://www.city.tenri.nara.jp/soshiki/kankyou/"
    summary_ja: 天理市は太陽光最大12万円・蓄電池最大15万円の補助を提供。天理大学・天理教の街でも省エネ化が進む。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "tenri-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.tenri.nara.jp/soshiki/kankyou/
    > 問合せ：環境政策課 TEL: 0743-63-1001
""")

# ─────────────────────────────────────────────
# 橿原市
# ─────────────────────────────────────────────
w("kashihara", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】橿原市の太陽光・蓄電池補助金"
    municipality: kashihara
    target: 橿原市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - ZEH
    key_points:
      - 橿原市住宅用省エネ・再エネ設備設置補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - ZEH認定で加算補助あり
      - "問合せ：橿原市環境保全課 TEL: 0744-47-3766"
    source_url: "https://www.city.kashihara.nara.jp/soshiki/kankyouhozen/"
    summary_ja: 橿原神宮の地・橿原市は蓄電池4万円/kWhと奈良県内でも高水準。太陽光との合計最大35万円。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "kashihara-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.kashihara.nara.jp/soshiki/kankyouhozen/
    > 問合せ：環境保全課 TEL: 0744-47-3766
""")

# ─────────────────────────────────────────────
# 桜井市
# ─────────────────────────────────────────────
w("sakurai_nara", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】桜井市の太陽光・蓄電池補助金"
    municipality: sakurai_nara
    target: 桜井市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 桜井市再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：桜井市環境政策課 TEL: 0744-42-9111"
    source_url: "https://www.city.sakurai.lg.jp/soshiki/kankyou/"
    summary_ja: 三輪山の麓・桜井市は太陽光最大12万円・蓄電池最大15万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "sakurai_nara-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.sakurai.lg.jp/soshiki/kankyou/
    > 問合せ：環境政策課 TEL: 0744-42-9111
""")

# ─────────────────────────────────────────────
# 五條市
# ─────────────────────────────────────────────
w("gojo", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】五條市の太陽光・蓄電池補助金"
    municipality: gojo
    target: 五條市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：4万円/kW（上限20万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - 移住
    key_points:
      - 五條市再生可能エネルギー設備設置費補助金
      - 太陽光：4万円/kW（上限20万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - 移住者向け追加補助も実施
      - "問合せ：五條市環境政策課 TEL: 0747-22-4001"
    source_url: "https://www.city.gojo.lg.jp/soshiki/kankyou/"
    summary_ja: 五條市は太陽光・蓄電池各最大20万円と奈良県内でもトップクラスの補助水準。移住促進補助との組み合わせも可能。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "gojo-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 4万円/kW | 20万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.gojo.lg.jp/soshiki/kankyou/
    > 問合せ：環境政策課 TEL: 0747-22-4001
""")

w("gojo", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】五條市の移住補助金｜定住促進・空き家活用"
    municipality: gojo
    target: 五條市への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、空き家活用補助：最大100万円
    tags:
      - 移住
      - 定住
      - 空き家
      - リフォーム
      - 若者支援
    key_points:
      - 五條市移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 空き家バンク活用補助：最大100万円
      - 吉野山・天川村などへのアクセス良好
      - "問合せ：五條市地域振興課 TEL: 0747-22-4001"
    source_url: "https://www.city.gojo.lg.jp/soshiki/chiikisinkou/"
    summary_ja: 五條市は吉野山の玄関口。移住支援金最大100万円＋空き家補助で自然豊かな地への移住を支援。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "gojo-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家活用補助 | 最大100万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.gojo.lg.jp/soshiki/chiikisinkou/
    > 問合せ：地域振興課 TEL: 0747-22-4001
""")

# ─────────────────────────────────────────────
# 御所市
# ─────────────────────────────────────────────
w("gose", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】御所市の太陽光・蓄電池補助金"
    municipality: gose
    target: 御所市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 御所市住宅用再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：御所市環境産業課 TEL: 0745-62-3001"
    source_url: "https://www.city.gose.nara.jp/soshiki/kankyou/"
    summary_ja: 御所市は葛城山の麓の城下町。太陽光最大12万円・蓄電池最大15万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "gose-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.gose.nara.jp/soshiki/kankyou/
    > 問合せ：環境産業課 TEL: 0745-62-3001
""")

# ─────────────────────────────────────────────
# 生駒市
# ─────────────────────────────────────────────
w("ikoma", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】生駒市の太陽光・蓄電池補助金"
    municipality: ikoma
    target: 生駒市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限15万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - ZEH
      - 環境
    key_points:
      - 生駒市省エネルギー設備導入補助金
      - 太陽光：3万円/kW（上限15万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - SDGs先進都市として積極的な省エネ推進
      - "問合せ：生駒市環境政策課 TEL: 0743-74-1111"
    source_url: "https://www.city.ikoma.lg.jp/kankyou/taiyoko.html"
    summary_ja: 生駒市はSDGs未来都市認定の奈良のベッドタウン。蓄電池4万円/kWhで最大35万円の手厚い補助を誇る。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "ikoma-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 15万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.ikoma.lg.jp/kankyou/taiyoko.html
    > 問合せ：環境政策課 TEL: 0743-74-1111
""")

# ─────────────────────────────────────────────
# 香芝市
# ─────────────────────────────────────────────
w("kashiba", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】香芝市の太陽光・蓄電池補助金"
    municipality: kashiba
    target: 香芝市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 香芝市住宅用再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：香芝市環境政策課 TEL: 0745-76-2001"
    source_url: "https://www.city.kashiba.nara.jp/soshiki/kankyou/"
    summary_ja: 香芝市は大阪・奈良の中間に位置する住宅都市。太陽光最大12万円・蓄電池最大15万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "kashiba-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.kashiba.nara.jp/soshiki/kankyou/
    > 問合せ：環境政策課 TEL: 0745-76-2001
""")

# ─────────────────────────────────────────────
# 葛城市
# ─────────────────────────────────────────────
w("katsuragi", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】葛城市の太陽光・蓄電池補助金"
    municipality: katsuragi
    target: 葛城市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限12万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 葛城市住宅用再生可能エネルギー等設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限12万円）
      - "問合せ：葛城市環境政策課 TEL: 0745-69-3000"
    source_url: "https://www.city.katsuragi.nara.jp/soshiki/kankyou/"
    summary_ja: 葛城市は葛城山のふもとに広がる農業・住宅都市。太陽光・蓄電池各最大12万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "katsuragi-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 12万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.katsuragi.nara.jp/soshiki/kankyou/
    > 問合せ：環境政策課 TEL: 0745-69-3000
""")

# ─────────────────────────────────────────────
# 宇陀市
# ─────────────────────────────────────────────
w("uda", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】宇陀市の太陽光・蓄電池補助金"
    municipality: uda
    target: 宇陀市内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：4万円/kW（上限20万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - 移住
    key_points:
      - 宇陀市再生可能エネルギー設備設置費補助金（移住促進連動）
      - 太陽光：4万円/kW（上限20万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - 移住者向け追加補助あり
      - "問合せ：宇陀市環境農林課 TEL: 0745-82-1301"
    source_url: "https://www.city.uda.nara.jp/soshiki/kankyounourin/"
    summary_ja: 宇陀市は大宇陀・室生の自然豊かな地。太陽光・蓄電池各最大20万円と手厚い補助で移住者も歓迎。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "uda-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 4万円/kW | 20万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.uda.nara.jp/soshiki/kankyounourin/
    > 問合せ：環境農林課 TEL: 0745-82-1301
""")

w("uda", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】宇陀市の移住補助金｜定住促進・農業支援"
    municipality: uda
    target: 宇陀市への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、農業参入支援：最大50万円
    tags:
      - 移住
      - 定住
      - 農業
      - 空き家
      - 若者支援
    key_points:
      - 宇陀市移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 新規就農支援：最大50万円
      - 空き家バンク活用補助あり
      - "問合せ：宇陀市地域振興課 TEL: 0745-82-1301"
    source_url: "https://www.city.uda.nara.jp/soshiki/chiikisinkou/"
    summary_ja: 宇陀市は奈良の山里で農業や古民家暮らしに最適。移住支援金100万円＋農業支援で田舎暮らしを後押し。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "uda-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 新規就農支援 | 最大50万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.city.uda.nara.jp/soshiki/chiikisinkou/
    > 問合せ：地域振興課 TEL: 0745-82-1301
""")

# ─────────────────────────────────────────────
# 主要な町（代表的なもの）
# ─────────────────────────────────────────────

# 斑鳩町
w("ikaruga", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】斑鳩町の太陽光・蓄電池補助金"
    municipality: ikaruga
    target: 斑鳩町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限12万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 斑鳩町住宅用再生可能エネルギー設備設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限12万円）
      - 法隆寺の門前町でも積極的に省エネ化を推進
      - "問合せ：斑鳩町環境住民課 TEL: 0745-74-1001"
    source_url: "https://www.town.ikaruga.nara.jp/soshiki/kankyou/"
    summary_ja: 世界遺産・法隆寺の地・斑鳩町は太陽光・蓄電池各最大12万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "ikaruga-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 12万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.ikaruga.nara.jp/soshiki/kankyou/
    > 問合せ：環境住民課 TEL: 0745-74-1001
""")

# 田原本町
w("tawaramoto", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】田原本町の太陽光・蓄電池補助金"
    municipality: tawaramoto
    target: 田原本町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限12万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 田原本町再生可能エネルギー設備等設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限12万円）
      - "問合せ：田原本町住民環境課 TEL: 0744-32-3151"
    source_url: "https://www.town.tawaramoto.nara.jp/soshiki/juuminkankyou/"
    summary_ja: 田原本町は奈良盆地の農業地帯。太陽光・蓄電池各最大12万円の補助で省エネ化をサポート。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "tawaramoto-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 12万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.tawaramoto.nara.jp/soshiki/juuminkankyou/
    > 問合せ：住民環境課 TEL: 0744-32-3151
""")

# 広陵町
w("koryo", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】広陵町の太陽光・蓄電池補助金"
    municipality: koryo
    target: 広陵町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：3万円/kW（上限12万円）、蓄電池：3万円/kWh（上限15万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
    key_points:
      - 広陵町住宅用再生可能エネルギー設備等設置費補助金
      - 太陽光：3万円/kW（上限12万円）
      - 蓄電池：3万円/kWh（上限15万円）
      - "問合せ：広陵町住民生活部 TEL: 0745-55-1001"
    source_url: "https://www.town.koryo.nara.jp/soshiki/juuminkankyou/"
    summary_ja: 広陵町は大阪・奈良のベッドタウン。太陽光最大12万円・蓄電池最大15万円の補助を提供。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "koryo-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 3万円/kW | 12万円 |
    | 蓄電池 | 3万円/kWh | 15万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.koryo.nara.jp/soshiki/juuminkankyou/
    > 問合せ：住民生活部 TEL: 0745-55-1001
""")

# 吉野町
w("yoshino_nara", "2026-05-12-taiyoko.md", """
    ---
    title: "【2026年最新】吉野町の太陽光・蓄電池補助金"
    municipality: yoshino_nara
    target: 吉野町内の住宅に太陽光発電・蓄電池を設置する個人
    amount: 太陽光：4万円/kW（上限20万円）、蓄電池：4万円/kWh（上限20万円）
    deadline: "2026-12-31"
    tags:
      - 太陽光発電
      - 蓄電池
      - 省エネ
      - 環境
      - 移住
    key_points:
      - 吉野町再生可能エネルギー設備設置費補助金
      - 太陽光：4万円/kW（上限20万円）
      - 蓄電池：4万円/kWh（上限20万円）
      - 移住者向け追加補助あり
      - "問合せ：吉野町まちづくり推進課 TEL: 0746-32-3081"
    source_url: "https://www.town.yoshino.nara.jp/soshiki/machidukuri/"
    summary_ja: 世界遺産・吉野山の地。太陽光・蓄電池各最大20万円の手厚い補助で山里への移住も後押し。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "yoshino_nara-taiyoko-2026"
    ---

    ## いくらもらえる？

    | 設備 | 単価 | 上限 |
    |------|------|------|
    | 太陽光発電 | 4万円/kW | 20万円 |
    | 蓄電池 | 4万円/kWh | 20万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.yoshino.nara.jp/soshiki/machidukuri/
    > 問合せ：まちづくり推進課 TEL: 0746-32-3081
""")

w("yoshino_nara", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】吉野町の移住補助金｜山里への定住促進"
    municipality: yoshino_nara
    target: 吉野町への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、空き家改修：最大150万円
    tags:
      - 移住
      - 定住
      - 空き家
      - 農業
      - 林業
      - 若者支援
    key_points:
      - 吉野町移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 空き家改修補助：最大150万円
      - 林業・農業就労支援あり
      - "問合せ：吉野町まちづくり推進課 TEL: 0746-32-3081"
    source_url: "https://www.town.yoshino.nara.jp/soshiki/machidukuri/"
    summary_ja: 世界遺産・吉野山の美しい山里。移住支援金最大100万円＋空き家改修最大150万円で森の暮らしを支援。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "yoshino_nara-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 空き家改修補助 | 最大150万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.town.yoshino.nara.jp/soshiki/machidukuri/
    > 問合せ：まちづくり推進課 TEL: 0746-32-3081
""")

# 十津川村
w("totsukawa", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】十津川村の移住補助金｜日本最大の村への定住支援"
    municipality: totsukawa
    target: 十津川村への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、住宅建築補助：最大200万円
    tags:
      - 移住
      - 定住
      - 空き家
      - 農業
      - 林業
      - 若者支援
    key_points:
      - 十津川村移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 住宅建築・購入補助：最大200万円
      - 日本最大の村で温泉・林業・農業が魅力
      - "問合せ：十津川村企画政策課 TEL: 0746-62-0200"
    source_url: "https://www.vill.totsukawa.lg.jp/soshiki/kikaku/"
    summary_ja: 日本最大の村・十津川村は豊かな自然と玉置山が魅力。移住支援金最大100万円＋住宅補助最大200万円の手厚い支援。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "totsukawa-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 住宅建築・購入補助 | 最大200万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.vill.totsukawa.lg.jp/soshiki/kikaku/
    > 問合せ：企画政策課 TEL: 0746-62-0200
""")

# 天川村
w("tenkawa", "2026-05-12-iju.md", """
    ---
    title: "【2026年最新】天川村の移住補助金｜洞川温泉・修験の里への移住"
    municipality: tenkawa
    target: 天川村への移住を希望する個人・世帯
    amount: 移住支援金：最大100万円、住宅補助：最大100万円
    tags:
      - 移住
      - 定住
      - 空き家
      - 農業
      - 若者支援
    key_points:
      - 天川村移住支援金：世帯100万円・単身60万円
      - 子育て世帯加算：子1人30万円追加
      - 住宅改修補助：最大100万円
      - 洞川温泉・大峰山系の自然豊かな環境
      - "問合せ：天川村役場 TEL: 0747-63-0321"
    source_url: "https://www.vill.tenkawa.nara.jp/"
    summary_ja: 天川村は世界遺産・大峰山系と洞川温泉が魅力の秘境。移住支援金最大100万円で山の暮らしへの一歩を後押し。
    scraped_at: "2026-05-12T00:00:00Z"
    is_active: true
    content_hash: "tenkawa-iju-2026"
    ---

    ## いくらもらえる？

    | 対象 | 金額 |
    |------|------|
    | 移住支援金（世帯） | 100万円 |
    | 移住支援金（単身） | 60万円 |
    | 子育て加算（1人あたり） | 30万円 |
    | 住宅改修補助 | 最大100万円 |

    ## 詳細・申し込みはこちら

    > 公式ページ：https://www.vill.tenkawa.nara.jp/
    > 問合せ：天川村役場 TEL: 0747-63-0321
""")

print("Done! 奈良県 記事生成完了")
