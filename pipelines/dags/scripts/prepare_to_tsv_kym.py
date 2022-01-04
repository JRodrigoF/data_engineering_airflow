#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import argparse
import urllib.parse

"""
Prepare kym.json data to put it in tsv
"""

# parsing command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

filename = args.file
output_file = args.out

if not filename:
    sys.stderr.write("Error! No input file specified.\n")

sys.stderr.write(f"Read from {filename}.\n")

# read all entries from file
# dict of dicts, keys are the url of the memes
with open(filename,'r') as f:
    memes_dict = json.load(f)
    f.close()

new_memes = {}
for d in memes_dict.keys():
    memeO = memes_dict[d]
    meme = {}
    # Keeping only category 'meme'
    # Keeping also other statuses than ['confirmed', 'submission'] to preserve child-parent chain
    if memeO['category'] != 'Meme' or memeO['details']['status'] not in ['confirmed', 'submission']:
        continue

    meme['category'] = memeO['category']
    #urldecode
    meme['Id'] = urllib.parse.unquote(memeO['url'].replace('https://knowyourmeme.com/memes/',''))
    meme['title'] = memeO['title']
    meme['last_update_source'] = memeO['last_update_source']
    meme['template_image_url'] = memeO['template_image_url']
    meme['url'] = memeO['url']

    meme['added'] = None
    if 'added' in memeO:
        meme['added'] = memeO['added']

    # meme['meta_title'] = None
    meme['meta_description'] = None
    # meme['meta_image'] = None

    if 'meta' in memeO:
        if 'description' in memeO['meta']:
            meme['meta_description'] = memeO['meta']['description']
        elif 'og:description' in memeO['meta']:
            meme['meta_description'] = memeO['meta']['og:description']
        elif 'twitter:description' in memeO['meta']:
            meme['meta_description'] = memeO['meta']['twitter:description']
        # meme_title redundant with main title
        # meme['meta_title'] = memeO['meta']['og:title']
        # meme['meta_image'] = urllib.parse.unquote(memeO['meta']['og:image'])

    meme['details_status'] = memeO['details']['status']
    meme['details_origin'] = memeO['details']['origin']
    meme['details_year'] = memeO['details']['year']
    meme['_details_type'] = []
    if 'type' in memeO['details']:
        for tp in memeO['details']['type']:
            meme['_details_type'].append(tp.replace('https://knowyourmeme.com/types/', ''))

    #sep_table
    meme['_tags'] = []
    if 'tags' in memeO:
        meme['_tags'] = sorted(list(set(memeO['tags'])))

    # -> drop for now
    # sep_table
    # meme['_search_keywords'] = []
    # if 'search_keywords' in memeO:
    #     meme['_search_keywords'] = sorted(list(set(memeO['search_keywords'])))

    # related memes
    meme['parent'] = None
    if 'parent' in memeO:
        meme['parent'] = memeO['parent'].replace('https://knowyourmeme.com/memes/','').replace('https://knowyourmeme.com/','')

    # drop for now, except notable examples
    # keeping loop over sections in case we decide to keep more
    # s -> key
    # sec -> value
    #TODO
    # -> write a version of this step using pattern matching from python 3.10
    meme['_content'] = {'_examples': []}
    if 'content' in memeO:
        for s in memeO['content']:
            # section = {'title': None, '_texts':[], '_links':[], '_images':[]}
            sec = memeO['content'][s]

            # if 'text' in sec:
            #     for it in sec['text']:
            #         section['_texts'].append(it)
            # if 'links' in sec:
            #     for it in sec['links']:
            #         section['_links'].append({'title': it[0], 'url': it[1].replace('https://knowyourmeme.com/','')})
            # if 'images' in sec:
            #     for it in sec['images']:
            #         section['_images'].append(it)

            if s in ['notable examples', 'various examples', 'examples', 'notable images', 'example images']:
                if 'images' in sec:
                    for it in sec['images']:
                        meme['_content']['_examples'].append(it)

            # else:
            #     meme['_content'][s] = section
        meme['_content']['_examples'] = sorted(list(set(meme['_content']['_examples'])))

    if meme['Id'] in new_memes:
        sys.stderr.write('We have a problem with nonunique ID:', meme['id'])
        sys.exit()

    new_memes[meme['Id']] = meme

# sys.stdout.write(json.dumps(new_memes, ensure_ascii=False, sort_keys=True))
# sys.stderr.write("\nDone.\n")
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(new_memes, f, ensure_ascii=False, sort_keys=True)

sys.stderr.write("Done.\n")
sys.stderr.write(f"File {filename} has been filtered and re-formatted.\n")
sys.stderr.write(f"Output has been written to {output_file}.\n")
