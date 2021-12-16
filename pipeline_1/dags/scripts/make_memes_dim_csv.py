#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import argparse
import pandas as pd

"""
Put preprocessed data in tsv for ingestion
"""

# parsing command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--outfile", "-o", type=str, required=True)

args = parser.parse_args()

filename = args.file
#output folder name
outputfilename = args.outfile

if not filename:
    sys.stderr.write("Error! No input file specified.\n")
    sys.exit(1)
if not outputfilename:
    sys.stderr.write("Error! No output file specified.\n")
    sys.exit(1)
sys.stderr.write(f"Read from {filename}.\n")

# read all entries from file
with open(filename,'r') as f:
    memes_dict = json.load(f)

memes_dim = {}

columns = ['kym_id', 'name', 'added', 'origin', 'status', 'year', 'last_update', 'description', 'image_url', 'url', 'parent_kym_id', 'tags'  ]
for col in columns:
    memes_dim[col] = []

for d in memes_dict:

    meme = memes_dict[d]
    memeID = meme['Id']
    memes_dim['kym_id'].append(memeID)
    memes_dim['name'].append(meme['title'])
    memes_dim['description'].append(meme['meta_description'])

    if not meme['added']: meme['added'] = 0
    memes_dim['added'].append( meme['added'])
    #if not row['added']: row['added'] = 'null'
    memes_dim['origin'].append( meme['details_origin'])
    memes_dim['status'] .append(meme['details_status'])

    if not meme['details_year']: meme['details_year'] = 0
    memes_dim['year'].append( meme['details_year'])

    if not meme['last_update_source']: meme['last_update_source'] = 0
    memes_dim['last_update'] .append(meme['last_update_source'])

    memes_dim['image_url']  .append( meme['template_image_url'])
    memes_dim['url'].append( meme['template_image_url'])
    memes_dim['parent_kym_id'].append(meme['parent'])
    memes_dim['tags'].append(json.dumps(sorted(meme['_tags']), ensure_ascii=False))

df = pd.DataFrame(memes_dim, columns=columns )
df.fillna('null')
df.astype({'year': 'int32'}).dtypes
df.astype({'added': 'int32'}).dtypes
df.astype({'last_update': 'int32'}).dtypes



df.to_csv(outputfilename, sep='\t', index = False)
sys.exit(0)
