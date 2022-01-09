#!/usr/bin/env python

import argparse
import json
import pandas as pd
import gzip
from collections import OrderedDict

from pathlib import Path

"""
Creates GV meme to safeness relationships table
"""

parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

input_file = args.file
output = args.out

# with open(filename, 'r') as f:
#     GV_dict = json.load(f)

# read all entries from file
with gzip.open(input_file, 'r') as f:
    json_bytes = f.read()
    json_str = json_bytes.decode('utf-8')
    GV_dict = json.loads(json_str)

safeness_dict = {}
for d in GV_dict:
    if 'safeSearchAnnotation' in GV_dict[d]:
        ord_d = OrderedDict(GV_dict[d]['safeSearchAnnotation'])
        main_key = d.replace('https://knowyourmeme.com/memes/', '')
        safeness_dict[main_key] = [ord_d[key] for key in ord_d]
    else:
        safeness_dict[d] = [False, False, False, False, False]

df = (pd.DataFrame.from_dict(safeness_dict, orient='index')
    .reset_index()
    .rename(columns={'index': 'key_KYM', 0: 'adult', 1: 'spoof', 2: 'medical', 3: 'violence', 4: 'racy'})
    .replace({'VERY_UNLIKELY': False, 'UNLIKELY': False, 'POSSIBLE': False, 'LIKELY': False, 'VERY_LIKELY': True})
    .groupby(['key_KYM']).first()
    .reset_index()
)

df = df.to_dict('records')

for dict in df:
    type = []
    for key in dict:
        if dict[key] == True and key != 'safeness_id':
            type.append(key)
    if type:
        type = ','.join(type)
    else:
        type = 'None'

    dict['safeness_category'] = type

df = pd.DataFrame(df)

# writes to filename
df.to_csv(output, sep="\t", encoding='utf-8', index=False)
# display(data)
