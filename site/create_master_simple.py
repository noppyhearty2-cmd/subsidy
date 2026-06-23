import csv
from pathlib import Path

# 実装済みフォルダをスキャン
subsidies_path = Path("C:/Users/noppy/Subsidy/site/src/content/subsidies")
impl = set()
for item in subsidies_path.iterdir():
    if item.is_dir() and "_pref" not in item.name:
        impl.add(item.name)

# 簡潔な市データ
cities = [
    ("sapporo", "札幌市", "北海道"),
    ("tokyo", "東京市", "東京都"),
]

print(f"Implemented: {len(impl)}")
print("Done")
