#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import argparse
import gzip

"""
Script to remove duplicate entries from kym.json.
Duplicates are indicated based on meme entry 'url' value.
Script keeps entries with max 'last_update_source' property value.
"""

# parsing command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

input_file = args.file
output_file = args.out

if not input_file:
    sys.stderr.write("Error! No input file specified.\n")

sys.stderr.write(f"Read from {input_file}.\n")

# with open(input_file, 'r') as f:
#     data = json.load(f)

# read all entries from file
with gzip.open(input_file, 'r') as f:
    json_bytes = f.read()
    json_str = json_bytes.decode('utf-8')
    data = json.loads(json_str)

memes_dict = {}
for d in data:
    key = d['url']
    if not key in memes_dict or d['last_update_source'] > memes_dict[key]['last_update_source']:
        memes_dict[key] = d

# write sorted result as json dict to output file
# dict key is 'url'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(memes_dict, f, ensure_ascii=False, sort_keys=True)

sys.stderr.write("Done.\n")
sys.stderr.write(f"File {input_file} has been de-duplicated.\n")
sys.stderr.write(f"Output has been written to {output_file}.\n")
