#!/usr/bin/env python

import argparse
import pandas as pd

"""
Creates Origin dimension table
"""

parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

filename = args.file
output = args.out

df = pd.read_csv(filename, sep="\t")

df = (df[~df['details_origin'].isin(['None', 'NA'])]
        .groupby(['details_origin']).first()
        .reset_index()
        .assign(origin_id=lambda x: range(1, len(x) + 1))
        .filter(items=['origin_id', 'details_origin'])
)

df.to_csv(output, sep="\t", encoding='utf-8', index=False)
# display(df)
