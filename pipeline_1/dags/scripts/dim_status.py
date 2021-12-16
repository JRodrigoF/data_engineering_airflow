#!/usr/bin/env python

import argparse
import pandas as pd

"""
Creates Status dimension table
"""

parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

filename = args.file
output = args.out

df = (
    pd.read_csv(filename, sep="\t")
    .filter(items=['details_status'])
    .sort_values('details_status').drop_duplicates().dropna()
    .assign(status_id=lambda x: range(1, len(x) + 1))
    .filter(items=['status_id', 'details_status'])
)
df = df[~df['details_status'].isin(['None'])]

df.to_csv(output, sep="\t", encoding='utf-8', index=False)
# display(df)
