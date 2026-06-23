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

# Read the complete CSV
csv_file = r"C:\Users\noppy\Subsidy\all_cities_complete.csv"
rows = []
seen = set()

with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        city_id = row['city_id'].strip().lower()
        city_name = row['市名'].strip()
        pref = row['都道府県'].strip()

        if city_id and city_name and pref and city_id not in seen:
            seen.add(city_id)
            status = "実装済" if city_id in implemented else "未実装"
            rows.append((city_id, city_name, pref, status))

print(f"Total deduplicated rows: {len(rows)}")

# Sort by prefecture, then city
rows_sorted = sorted(rows, key=lambda x: (x[2], x[1]))

# Write output
output = r"C:\Users\noppy\Subsidy\japanese_cities_final.csv"
with open(output, 'w', encoding='utf-8') as f:
    f.write('city_id,市名,都道府県,実装状態\n')
    for city_id, city_name, pref, status in rows_sorted:
        f.write(f'{city_id},{city_name},{pref},{status}\n')

print(f"Output file: {output}")

# Statistics
impl = sum(1 for r in rows_sorted if r[3] == "実装済")
unimpl = sum(1 for r in rows_sorted if r[3] == "未実装")
print(f"\nImplementation status:")
print(f"  Implemented: {impl}")
print(f"  Not implemented: {unimpl}")
print(f"  Total: {len(rows_sorted)}")

# Group not implemented by prefecture
print("\nNot implemented cities by prefecture:")
unimpl_by_pref = {}
for city_id, city_name, pref, status in rows_sorted:
    if status == "未実装":
        if pref not in unimpl_by_pref:
            unimpl_by_pref[pref] = 0
        unimpl_by_pref[pref] += 1

for pref in sorted(unimpl_by_pref.keys()):
    count = unimpl_by_pref[pref]
    print(f"  {pref}: {count}")
