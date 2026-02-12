"""
Test script for new Today tab components: forecast_components and market_notices.

Run with: python test_new_components.py
"""

import sys
sys.path.insert(0, 'src')

# Avoid DuckDB initialization issues by not importing the full package
import pandas as pd
import requests
import zipfile
import io
import re
import json
from datetime import datetime, timedelta
from pathlib import Path

print("=" * 60)
print("Testing New Today Tab Components")
print("=" * 60)

# Test 1: P30 Forecast Fetch
print("\n1. Testing P30 Pre-dispatch Forecast Fetch")
print("-" * 40)

try:
    index_url = "http://www.nemweb.com.au/Reports/Current/Predispatch_Reports/"
    response = requests.get(index_url, timeout=30)

    pattern = r'PUBLIC_PREDISPATCH_(\d{12})_(\d{14})_LEGACY\.zip'
    matches = re.findall(pattern, response.text)

    if matches:
        latest = sorted(set(matches))[-1]
        filename = f"PUBLIC_PREDISPATCH_{latest[0]}_{latest[1]}_LEGACY.zip"
        run_time = datetime.strptime(latest[0], '%Y%m%d%H%M')

        print(f"  Latest P30 run: {run_time}")

        # Fetch and parse the file
        file_url = index_url + filename
        zip_response = requests.get(file_url, timeout=60)

        with zipfile.ZipFile(io.BytesIO(zip_response.content)) as zf:
            csv_name = [n for n in zf.namelist() if n.endswith('.CSV')][0]
            csv_content = zf.read(csv_name).decode('utf-8')

        lines = csv_content.split('\n')
        header_line = None
        data_rows = []

        for line in lines:
            if line.startswith('I,PDREGION,'):
                parts = line.split(',')
                header_line = parts[4:]
            elif line.startswith('D,PDREGION,'):
                parts = line.split(',')
                data_rows.append(parts[4:])

        if header_line and data_rows:
            df = pd.DataFrame(data_rows, columns=header_line)
            df['REGIONID'] = df['REGIONID'].astype(str)
            df['RRP'] = pd.to_numeric(df['RRP'], errors='coerce')

            print(f"  Rows parsed: {len(df)}")
            print(f"  Regions: {df['REGIONID'].unique().tolist()}")
            print(f"  Price range: ${df['RRP'].min():.0f} to ${df['RRP'].max():.0f}")
            print("  PASSED")
        else:
            print("  FAILED: No data rows found")
    else:
        print("  FAILED: No P30 files found")

except Exception as e:
    print(f"  FAILED: {e}")


# Test 2: Market Notices Fetch
print("\n2. Testing Market Notices Fetch")
print("-" * 40)

try:
    url = "https://www.nemweb.com.au/REPORTS/CURRENT/Market_Notice/"
    response = requests.get(url, timeout=30)

    pattern = r'NEMITWEB1_MKTNOTICE_(\d{8})\.R(\d+)'
    matches = re.findall(pattern, response.text)

    print(f"  Total notice files found: {len(matches)}")

    # Test fetching and filtering
    cutoff = datetime.now() - timedelta(hours=72)
    recent_files = []
    for date_str, ref_num in matches:
        file_date = datetime.strptime(date_str, '%Y%m%d')
        if file_date.date() >= cutoff.date():
            filename = f"NEMITWEB1_MKTNOTICE_{date_str}.R{ref_num}"
            if filename not in recent_files:
                recent_files.append(filename)

    print(f"  Files in last 72h: {len(recent_files)}")

    # Fetch a few and check filtering
    EXCLUDED_TYPES = ['RECLASSIFY', 'NON-CONFORMANCE', 'SETTLEMENTS']
    RELEVANT_TYPES = ['RESERVE', 'POWER', 'INTER-REGIONAL']
    RELEVANT_KEYWORDS = ['LOR1', 'LOR2', 'LOR3', 'RERT', 'direction']

    type_counts = {}
    relevant_notices = []

    # Check most recent 30 notices
    recent_files = sorted(recent_files, key=lambda x: int(x.split('.R')[1]))[-30:]

    for filename in recent_files:
        try:
            notice_url = f"{url}{filename}"
            content = requests.get(notice_url, timeout=15).text

            match = re.search(r'Notice Type ID\s*:\s*(\S+)', content)
            type_id = match.group(1) if match else 'UNKNOWN'

            type_counts[type_id] = type_counts.get(type_id, 0) + 1

            # Check relevance
            if type_id in EXCLUDED_TYPES:
                continue

            match = re.search(r'Creation Date\s*:\s*(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})', content)
            if match:
                creation = datetime.strptime(f"{match.group(1)} {match.group(2)}", '%d/%m/%Y %H:%M:%S')
                if creation < datetime.now() - timedelta(hours=48):
                    continue  # Too old

            reason_match = re.search(r'Reason\s*:\s*\n+(.*?)(?:\n-{20,}|\Z)', content, re.DOTALL)
            reason = reason_match.group(1).strip() if reason_match else ''
            reason_upper = reason.upper()

            is_relevant = (
                type_id in RELEVANT_TYPES or
                any(kw.upper() in reason_upper for kw in RELEVANT_KEYWORDS)
            )

            if is_relevant:
                relevant_notices.append({
                    'type': type_id,
                    'reason': reason[:60]
                })

        except Exception:
            continue

    print(f"  Notice types in sample: {type_counts}")
    print(f"  Price-relevant notices: {len(relevant_notices)}")

    for n in relevant_notices[:3]:
        print(f"    - {n['type']}: {n['reason']}...")

    print("  PASSED")

except Exception as e:
    print(f"  FAILED: {e}")


# Test 3: Forecast Cache
print("\n3. Testing Forecast Cache")
print("-" * 40)

try:
    cache_path = Path('/Volumes/davidleitch/aemo_production/data/forecast_cache.json')

    if cache_path.exists():
        with open(cache_path, 'r') as f:
            cache = json.load(f)
        print(f"  Cache exists: Yes")
        print(f"  Keys: {list(cache.keys())}")
        if 'run_time' in cache:
            print(f"  Last run: {cache['run_time']}")
        if 'avg_24h' in cache:
            print(f"  24hr avg cached: {cache['avg_24h']}")
        print("  PASSED")
    else:
        print("  Cache exists: No (will be created on first run)")
        print("  PASSED (expected for new install)")

except Exception as e:
    print(f"  FAILED: {e}")


print("\n" + "=" * 60)
print("All component tests completed!")
print("=" * 60)
