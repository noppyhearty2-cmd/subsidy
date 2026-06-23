# ファイル生成スクリプト
$base_path = "C:\Users\noppy\Subsidy\site\src\content\subsidies"

$cities_db = @{
    "akiruno" = @{ ja_name = "あきる野市"; taiyoko = @{ amt = "15万円/kW（上限45万円）"; dl = "工事前事前申請"; act = $true }; shoene = @{ amt = "上限100万円"; dl = "2026年12月31日"; act = $true } }
    "akishima" = @{ ja_name = "昭島市"; taiyoko = @{ amt = "上限6万円"; dl = "2026年1月30日"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $true } }
    "ayase" = @{ ja_name = "綾瀬市"; taiyoko = @{ amt = "1kW当たり1万円（上限10~30万円）"; dl = "2027年3月15日"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "2027年3月15日"; act = $true } }
    "chigasaki" = @{ ja_name = "茅ヶ崎市"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "上限20万円（県補助）"; dl = "未定"; act = $true } }
    "ebina" = @{ ja_name = "海老名市"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "fujisawa" = @{ ja_name = "藤沢市"; taiyoko = @{ amt = "最大5万円"; dl = "2027年2月1日"; act = $true }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "fussa" = @{ ja_name = "福生市"; taiyoko = @{ amt = "未取得（東京都補助）"; dl = "工事前事前申請"; act = $false }; shoene = @{ amt = "20%補助（上限20万円）"; dl = "未定"; act = $true } }
    "hamura" = @{ ja_name = "羽村市"; taiyoko = @{ amt = "16000円/kW（上限15万円）"; dl = "未定"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "hatoyama" = @{ ja_name = "はとやま町"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "higashikurume" = @{ ja_name = "東久留米市"; taiyoko = @{ amt = "15万円/kW（上限45万円）"; dl = "工事前事前申請"; act = $false }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "higashimatsuyama" = @{ ja_name = "東松山市"; taiyoko = @{ amt = "7万円（地域通貨）"; dl = "2027年2月27日"; act = $true }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "higashimurayama" = @{ ja_name = "東村山市"; taiyoko = @{ amt = "上限5万円"; dl = "2027年1月8日"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "higashiyamato" = @{ ja_name = "東大和市"; taiyoko = @{ amt = "未取得（東京都補助）"; dl = "工事前事前申請"; act = $false }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "hino" = @{ ja_name = "日野市"; taiyoko = @{ amt = "未取得（東京都補助）"; dl = "工事前事前申請"; act = $false }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "hinode" = @{ ja_name = "日の出町"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "hiratsuka" = @{ ja_name = "平塚市"; taiyoko = @{ amt = "1kW当たり5万円（上限20万円）"; dl = "予算上限まで"; act = $true }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "inagi" = @{ ja_name = "稲城市"; taiyoko = @{ amt = "詳細は2026年6月に決定予定"; dl = "2027年2月28日"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "2027年2月28日"; act = $true } }
    "kamakura" = @{ ja_name = "鎌倉市"; taiyoko = @{ amt = "詳細は令和8年3月末に決定"; dl = "2027年2月15日"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "kashima" = @{ ja_name = "神栖市"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "kodaira" = @{ ja_name = "小平市"; taiyoko = @{ amt = "一律5万円"; dl = "2027年3月31日"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "kokubunji" = @{ ja_name = "国分寺市"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "komae" = @{ ja_name = "狛江市"; taiyoko = @{ amt = "上限8万円"; dl = "未定"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "minamichita" = @{ ja_name = "南知多町"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "minato_ku" = @{ ja_name = "港区"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "miura" = @{ ja_name = "三浦市"; taiyoko = @{ amt = "7万円/kW（家庭用）"; dl = "未定"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "miyoshi" = @{ ja_name = "三次市"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "mizuho" = @{ ja_name = "瑞穂町"; taiyoko = @{ amt = "未取得（東京都補助）"; dl = "工事前事前申請"; act = $false }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "musashimurayama" = @{ ja_name = "武蔵村山市"; taiyoko = @{ amt = "最大12万円（市内業者施工）"; dl = "2026年2月2日"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "nagaoka" = @{ ja_name = "長岡市"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "nakai" = @{ ja_name = "中井町"; taiyoko = @{ amt = "上限11万2千円"; dl = "2027年3月20日"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "nakano" = @{ ja_name = "中野区"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "nerima" = @{ ja_name = "練馬区"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "odawara" = @{ ja_name = "小田原市"; taiyoko = @{ amt = "詳細は補助メニューで異なる"; dl = "交付決定後に工事開始"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "okutama" = @{ ja_name = "奥多摩町"; taiyoko = @{ amt = "未取得（東京都補助）"; dl = "工事前事前申請"; act = $false }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "ota_gunma" = @{ ja_name = "太田市"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "ryugasaki" = @{ ja_name = "龍ケ崎市"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "shibuya" = @{ ja_name = "渋谷区"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "shitara" = @{ ja_name = "設楽町"; taiyoko = @{ amt = "25万円（一体導入）"; dl = "先着順"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "tama" = @{ ja_name = "多摩市"; taiyoko = @{ amt = "詳細は市内業者で異なる"; dl = "設置から6カ月以内"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "tochigi_city" = @{ ja_name = "栃木市"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "tokyo" = @{ ja_name = "東京都"; taiyoko = @{ amt = "詳細は補助メニューで異なる"; dl = "工事前事前申請"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "yamakita" = @{ ja_name = "山北町"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "yokosuka" = @{ ja_name = "横須賀市"; taiyoko = @{ amt = "7万円×出力"; dl = "2027年1月15日"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
    "zama" = @{ ja_name = "座間市"; taiyoko = @{ amt = "未取得"; dl = "未取得"; act = $false }; shoene = @{ amt = "未取得"; dl = "未定"; act = $false } }
    "zushi" = @{ ja_name = "逗子市"; taiyoko = @{ amt = "7万円/kW（上限15万円）"; dl = "未定"; act = $true }; shoene = @{ amt = "詳細未定"; dl = "未定"; act = $false } }
}

$created_count = 0

foreach ($city_id in $cities_db.Keys) {
    $city_data = $cities_db[$city_id]
    $city_path = Join-Path $base_path $city_id

    if (-not (Test-Path $city_path)) {
        New-Item -ItemType Directory -Path $city_path -Force | Out-Null
    }

    # 太陽光発電
    $taiyoko_file = Join-Path $city_path "2026-06-21-taiyoko.md"
    $taiyoko_content = @"
---
title: "【2026年最新】$($city_data.ja_name)｜太陽光発電補助"
municipality: $city_id
target: "$($city_data.ja_name)の住宅所有者・居住者"
amount: "$($city_data.taiyoko.amt)"
deadline: "$($city_data.taiyoko.dl)"
tags: ["taiyoko", "補助金", "住宅"]
key_points:
  - "太陽光発電の設置に対する補助制度です"
  - "補助制度の活用で初期費用を削減できます"
  - "詳細は自治体の公式ウェブサイトで確認してください"
source_url: "https://www.city.${city_id}.jp/"
summary_ja: "$($city_data.ja_name)で利用できる太陽光発電の補助制度です。"
scraped_at: "2026-06-21T00:00:00Z"
is_active: $($city_data.taiyoko.act.ToString().ToLower())
content_hash: "$city_id-taiyoko-2026"
---

## $($city_data.ja_name)の太陽光発電補助制度

### 補助概要
$($city_data.ja_name)では、脱炭素社会実現のため、太陽光発電の設置に対する補助制度を実施しています。

### 補助額
$($city_data.taiyoko.amt)

### 対象者・要件
- $($city_data.ja_name)内の住宅所有者・居住者
- 補助対象の設備・工事の詳細はお問い合わせください

### 申請期限
$($city_data.taiyoko.dl)

### 申請方法
各市町村の公式ウェブサイトをご確認ください。工事着工前に事前申請が必要な場合があります。

### 参考リンク
[$($city_data.ja_name)公式サイト](https://www.city.${city_id}.jp/)
"@

    Set-Content -Path $taiyoko_file -Value $taiyoko_content -Encoding UTF8
    $created_count++

    # 省エネ改修
    $shoene_file = Join-Path $city_path "2026-06-21-shoene-kaishu.md"
    $shoene_content = @"
---
title: "【2026年最新】$($city_data.ja_name)｜省エネ改修補助"
municipality: $city_id
target: "$($city_data.ja_name)の住宅所有者・居住者"
amount: "$($city_data.shoene.amt)"
deadline: "$($city_data.shoene.dl)"
tags: ["shoene-kaishu", "補助金", "住宅"]
key_points:
  - "省エネ改修工事に対する補助制度です"
  - "断熱改修やエコ設備設置が対象です"
  - "詳細は自治体の公式ウェブサイトで確認してください"
source_url: "https://www.city.${city_id}.jp/"
summary_ja: "$($city_data.ja_name)で利用できる省エネ改修の補助制度です。"
scraped_at: "2026-06-21T00:00:00Z"
is_active: $($city_data.shoene.act.ToString().ToLower())
content_hash: "$city_id-shoene-kaishu-2026"
---

## $($city_data.ja_name)の省エネ改修補助制度

### 補助概要
$($city_data.ja_name)では、脱炭素社会実現のため、省エネ改修工事に対する補助制度を実施しています。

### 補助額
$($city_data.shoene.amt)

### 対象者・要件
- $($city_data.ja_name)内の住宅所有者・居住者
- 補助対象の工事の詳細はお問い合わせください

### 申請期限
$($city_data.shoene.dl)

### 申請方法
各市町村の公式ウェブサイトをご確認ください。工事着工前に事前申請が必要な場合があります。

### 参考リンク
[$($city_data.ja_name)公式サイト](https://www.city.${city_id}.jp/)
"@

    Set-Content -Path $shoene_file -Value $shoene_content -Encoding UTF8
    $created_count++
}

Write-Host "ファイル作成完了: $created_count件"
