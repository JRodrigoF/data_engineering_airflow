#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import argparse
import csv

"""
Put preprocessed data in tsv for better overview
"""

# parsing command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
args = parser.parse_args()

filename = args.file
if not filename:
    sys.stderr.write("Error! No input file specified.\n")


sys.stderr.write(f"Read from {filename}.\n")

# read all entries from file
with open(filename,'r') as f:
    memes_dict = json.load(f)
    f.close()

#https://stackoverflow.com/questions/60794316/print-get-full-path-of-every-nested-item-in-a-dictionary-python3
memes_rows = []
memes_keys = []

meme_tag_rows = []
meme_keywords_rows = []
meme_details_type_rows = []

meme_content_links_rows = []
meme_content_texts_rows = []
meme_content_images_rows = []



for d in memes_dict:
    # memes 1-dimensional fields
    memes_row = []
    meme = memes_dict[d]
    memeID = meme['Id']
    if not len(memes_keys):
        all_keys_properties = meme.keys()
        for key in sorted(all_keys_properties):
            if not key.startswith('_'):
                memes_keys.append(key)
            else:
                print (key)
        memes_rows.append(memes_keys)
    for key in memes_keys:
        memes_row.append(str(meme[key]))
    memes_rows.append(memes_row)

    for value in meme['_tags']:
        if not len(meme_tag_rows):
            meme_tag_rows.append(['memeId', 'tag'])

        meme_tag_rows.append([memeID, value])

    #meme_keywords_rows
    for value in meme['_search_keywords']:
        if not len(meme_keywords_rows):
            meme_tag_rows.append(['memeId', 'keyword'])
        meme_keywords_rows.append([memeID, value])

    #_details_type
    for value in meme['_details_type']:
        if not len(meme_details_type_rows):
            meme_tag_rows.append(['memeId', 'type'])
        meme_details_type_rows.append([memeID, value])
    # multidimentional fields

    #meme_content_rows
    for c in meme['_content']:
        content = meme['_content'][c]
        title = content['title']
        if not len(meme_content_texts_rows):
            meme_content_texts_rows.append(['memeId', 'content_title', 'text'])
            meme_content_links_rows.append(['memeId', 'content_title', 'url_title', 'url'])
            meme_content_images_rows.append(['memeId', 'content_title', 'image'])

        for text in content['_texts']:
            meme_content_texts_rows.append([memeID, title, text])

        for link in content['_links']:
            meme_content_links_rows.append([memeID, title, link['title'], link['url']])

        for img in content['_images']:
            meme_content_images_rows.append([memeID, title, img])

import os
try:
    os.stat('tsv')
except:
    os.mkdir('tsv')

f = open('tsv/memes.tsv', 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in memes_rows:
    writer.writerow(r)
f.close()

f = open('tsv/meme_tags.tsv', 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_tag_rows:
    writer.writerow(r)
f.close()

f = open('tsv/meme_search_keywords.tsv', 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_keywords_rows:
    writer.writerow(r)
f.close()

f = open('tsv/meme_details_type.tsv', 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_details_type_rows:
    writer.writerow(r)
f.close()

f = open('tsv/meme_content_texts_rows.tsv', 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_content_texts_rows:
    writer.writerow(r)
f.close()

f = open('tsv/meme_content_links_rows.tsv', 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_content_links_rows:
    writer.writerow(r)
f.close()

f = open('tsv/meme_content_images_rows.tsv', 'w', encoding='UTF8')
writer = csv.writer(f,  delimiter = "\t")
for r in meme_content_images_rows:
    writer.writerow(r)
f.close()
