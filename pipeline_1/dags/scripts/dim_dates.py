#!/usr/bin/env python

import argparse
import pandas as pd

"""
Creates Dates dimension table
"""

parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

filename = args.file
output = args.out

df = (pd.read_csv(filename, sep="\t")
        .rename(columns={'added': 'added_year',
                        'details_year': 'details_historical_year',
                        'last_update_source': 'last_update'})
        .groupby(['added_year', 'details_historical_year',
                'last_update']).first()
        .reset_index()
        .assign(date_id=lambda x: range(1, len(x) + 1))
        .filter(items=['date_id', 'added_year',
                'details_historical_year', 'last_update'])
)

df.to_csv(output, sep="\t", encoding='utf-8', index=False)
# display(df)
