#!/usr/bin/env python3
"""Check DUID mapping column names"""

import pickle
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / 'src'))
from aemo_dashboard.shared.config import config

# Load DUID mapping
with open(config.gen_info_file, 'rb') as f:
    duid_mapping = pickle.load(f)

print("DUID mapping columns:")
print(duid_mapping.columns.tolist())
print(f"\nShape: {duid_mapping.shape}")
print(f"\nSample data:")
print(duid_mapping.head())