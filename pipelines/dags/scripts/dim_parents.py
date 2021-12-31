#!/usr/bin/env python

import argparse
import pandas as pd

"""
Creates Parents dimension table
"""

parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

filename = args.file
output = args.out

df = pd.read_csv(filename, sep="\t")

df = (df[~df['parent'].isin(['None'])]
    .groupby(['parent']).first()
    .reset_index()
    .assign(parent_id=lambda x: range(1, len(x) + 1))
    .filter(items=['parent_id', 'parent'])
)

df.to_csv(output, sep="\t", encoding='utf-8', index=False)
# display(df)
