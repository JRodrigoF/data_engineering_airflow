#!/usr/bin/env python

import argparse
import pandas as pd

"""
Creates Info dimension table
"""

parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

filename = args.file
output = args.out

df = (
    pd.read_csv(filename, sep="\t")
    .assign(info_id=lambda x: range(1, len(x) + 1))
    .rename(columns={'template_image_url': 'url_jpg',
                    'url': 'url_webpage', 'Id': 'key_KYM'})
    .filter(items=['info_id', 'key_KYM', 'title',
                    'meta_description', 'url_jpg', 'url_webpage'])
)

df.to_csv(output, sep="\t", encoding='utf-8', index=False)
# display(df)
