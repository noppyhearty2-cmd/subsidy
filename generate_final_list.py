#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv

# Get implemented directories
subs_dir = r"C:\Users\noppy\Subsidy\site\src\content\subsidies"
implemented = set()
for d in os.listdir(subs_dir):
    if os.path.isdir(os.path.join(subs_dir, d)) and not d.endswith('_pref'):
        implemented.add(d.lower())

print(f"Implemented directories: {len(implemented)}")

# Read current CSV
current_csv = r"C:\Users\noppy\Subsidy\all_cities_final.csv"
all_rows = []
seen_ids = set()

try:
    with open(current_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            city_id = row.get('city_id', '').strip().lower()
            if city_id and city_id not in seen_ids:
                seen_ids.add(city_id)
                city_name = row.get('市名', '').strip()
                pref = row.get('都道府県', '').strip()
                status = "実装済" if city_id in implemented else "未実装"
                all_rows.append((city_id, city_name, pref, status))
except Exception as e:
    print(f"Error reading CSV: {e}")

print(f"Deduplicated rows: {len(all_rows)}")

# Sort by prefecture, then city name
all_rows_sorted = sorted(all_rows, key=lambda x: (x[2], x[1]))

# Write to final output
output_file = r"C:\Users\noppy\Subsidy\japanese_cities_564.csv"
with open(output_file, 'w', encoding='utf-8') as f:
    f.write('city_id,市名,都道府県,実装状態\n')
    for city_id, city_name, pref, status in all_rows_sorted:
        f.write(f'{city_id},{city_name},{pref},{status}\n')

print(f"\nFinal output: {output_file}")
print(f"Total rows: {len(all_rows_sorted)}")

# Count
impl_count = sum(1 for r in all_rows_sorted if r[3] == "実装済")
unimpl_count = sum(1 for r in all_rows_sorted if r[3] == "未実装")

print(f"Implemented: {impl_count}")
print(f"Not implemented: {unimpl_count}")

# Summary by prefecture
print("\n未実装 cities by prefecture:")
by_pref = {}
for city_id, city_name, pref, status in all_rows_sorted:
    if status == "未実装":
        if pref not in by_pref:
            by_pref[pref] = 0
        by_pref[pref] += 1

for pref in sorted(by_pref.keys()):
    print(f"  {pref}: {by_pref[pref]}")
