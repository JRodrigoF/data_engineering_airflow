#!/usr/bin/env python

import argparse
import pandas as pd

"""
Creates Safeness dimension table
"""

parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

filename = args.file
output = args.out

df = pd.read_csv(filename, sep="\t")

df = (df
    .drop(['key_KYM'], axis=1)
    .groupby(['adult', 'spoof', 'medical', 'violence', 'racy'], dropna=False).first()
    .reset_index()
    .assign(safeness_id=lambda x: range(1, len(x) + 1))
    .filter(items=['safeness_id', 'safeness_category'])
    .replace({'None': 'undefined'})
)

# writes to filename
df.to_csv(output, sep="\t", encoding='utf-8', na_rep='None', index=False)
# display(data)

