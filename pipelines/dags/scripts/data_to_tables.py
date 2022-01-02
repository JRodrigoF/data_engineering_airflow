#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import argparse
import csv

"""
Put preprocessed data in tsv for ingestion
"""

# parsing command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--outfolder", "-o", type=str, required=True)
parser.add_argument("--prefix", "-p", type=str, required=False)
args = parser.parse_args()

filename = args.file
outputfolder = args.outfolder
prefix = ''
if args.prefix:
    prefix = args.prefix
if not prefix.endswith('-'):
    prefix += '_'

output_memes = f"{outputfolder}/{prefix}memes.tsv"
output_tags = f"{outputfolder}/{prefix}meme_tags.tsv"
output_types = f"{outputfolder}/{prefix}meme_details_types.tsv"
output_examples = f"{outputfolder}/{prefix}meme_content_examples.tsv"
output_children = f"{outputfolder}/{prefix}parent_children_relations.tsv"
output_siblings = f"{outputfolder}/{prefix}siblings_relations.tsv"

if not filename:
    sys.stderr.write("Error! No input file specified.\n")

sys.stderr.write(f"Read from {filename}.\n")

# read all entries from file
with open(filename,'r') as f:
    memes_dict = json.load(f)

#https://stackoverflow.com/questions/60794316/print-get-full-path-of-every-nested-item-in-a-dictionary-python3
memes_rows = []

# single-value keys
memes_sv_keys = []

# multi-dimensional keys
memes_mv_keys = []

meme_tag_rows = []
meme_details_type_rows = []
meme_content_example_rows = []

meme_children_rows = []
meme_siblings_rows = []
meme_siblings_data = {}

for d in memes_dict:
    memes_row = []
    meme = memes_dict[d]
    memeID = meme['Id']

    if not len(memes_sv_keys):
        all_keys_properties = meme.keys()
        for key in sorted(all_keys_properties):
            if not key.startswith('_'):
                memes_sv_keys.append(key) # key -> unique value
            else:
                memes_mv_keys.append(key) # key -> dict / list of values

        # let's keep in separate single value keys and multi value keys
        # memes_rows.append(memes_keys + ['total' + key for key in memes_mv_keys])
        memes_rows.append(memes_sv_keys) # row of columns names

    for key in memes_sv_keys: # grows the row for unique value keys
        memes_row.append(str(meme[key]))

    # for key in memes_mv_keys:
    #     memes_row.append(str(len(meme[key])))
    memes_rows.append(memes_row)

    # tags data
    for value in meme['_tags']:
        if not len(meme_tag_rows):
            meme_tag_rows.append(['key_KYM', 'tag'])
        meme_tag_rows.append([memeID, value])

    # children data
    if not len(meme_children_rows):
        meme_children_rows.append(['meme_Id', 'child'])
    elif meme['parent'] != None:
        meme_children_rows.append([meme['parent'], memeID])

    # siblings data collector
    if meme['parent'] not in meme_siblings_data:
        meme_siblings_data[meme['parent']] = [memeID]
    else:
        meme_siblings_data[meme['parent']].append(memeID)

    #_details_type
    for value in meme['_details_type']:
        if not len(meme_details_type_rows):
            meme_details_type_rows.append(['key_KYM', 'type'])
        meme_details_type_rows.append([memeID, value])

    # #meme_keywords_rows
    # for value in meme['_search_keywords']:
    #     if not len(meme_keywords_rows):
    #         meme_keywords_rows.append(['memeId', 'keyword'])
    #     meme_keywords_rows.append([memeID, value])

    #meme_content_rows
    for c in meme['_content']:
        content = meme['_content'][c]
        # title = content['title']
        if not len(meme_content_example_rows):
            meme_content_example_rows.append(['key_KYM', 'variation'])
            # meme_content_texts_rows.append(['memeId', 'content_title', 'text'])
            # meme_content_links_rows.append(['memeId', 'content_title', 'url_title', 'url'])
            # meme_content_images_rows.append(['memeId', 'content_title', 'image'])

        if c == '_examples':
            for example in content:
                meme_content_example_rows.append([memeID, example])

        # for text in content['_texts']:
        #     meme_content_texts_rows.append([memeID, title, text])

        # for link in content['_links']:
        #     meme_content_links_rows.append([memeID, title, link['title'], link['url']])

        # for img in content['_images']:
        #     meme_content_images_rows.append([memeID, title, img])

# siblings rows
for meme in meme_siblings_data:
    if len(meme_siblings_data[meme]) > 1:
        head_of_siblings = meme_siblings_data[meme].pop()

        for sibling in meme_siblings_data[meme]:
            if not len(meme_siblings_rows):
                meme_siblings_rows.append(['meme_Id', 'sibling'])
            meme_siblings_rows.append([head_of_siblings, sibling])

import os
try:
    os.stat('tsv')
except:
    os.mkdir('tsv')

f = open(output_memes, 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in memes_rows:
    writer.writerow(r)
f.close()

f = open(output_tags, 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_tag_rows:
    writer.writerow(r)
f.close()

# f = open('tsv/meme_search_keywords.tsv', 'w', encoding='UTF8')
# writer = csv.writer(f,  delimiter = "\t")
# for r in meme_keywords_rows:
#     writer.writerow(r)
# f.close()

f = open(output_types, 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_details_type_rows:
    writer.writerow(r)
f.close()

# f = open('tsv/meme_content_texts_rows.tsv', 'w', encoding='UTF8')
# writer = csv.writer(f,  delimiter = "\t")
# for r in meme_content_texts_rows:
#     writer.writerow(r)
# f.close()

# f = open('tsv/meme_content_links_rows.tsv', 'w', encoding='UTF8')
# writer = csv.writer(f,  delimiter = "\t")
# for r in meme_content_links_rows:
#     writer.writerow(r)
# f.close()

# f = open('tsv/meme_content_images_rows.tsv', 'w', encoding='UTF8')
# writer = csv.writer(f,  delimiter = "\t")
# for r in meme_content_images_rows:
#     writer.writerow(r)
# f.close()

f = open(output_examples, 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_content_example_rows:
    writer.writerow(r)
f.close()

f = open(output_children, 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_children_rows:
    writer.writerow(r)
f.close()

f = open(output_siblings, 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_siblings_rows:
    writer.writerow(r)
f.close()
