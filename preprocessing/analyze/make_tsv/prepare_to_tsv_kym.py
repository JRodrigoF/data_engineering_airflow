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
args = parser.parse_args()

filename = args.file
if not filename:
    sys.stderr.write("Error! No input file specified.\n")

sys.stderr.write(f"Read from {filename}.\n")

# read all entries from file
with open(filename,'r') as f:
    memes_dict = json.load(f)
    f.close()

new_memes = {}
for d in memes_dict:
    memeO = memes_dict[d]
    meme = {}
    #urldecode
    meme['category'] = memeO['category']
    meme['Id'] = urllib.parse.unquote(memeO['url'].replace('https://knowyourmeme.com/memes/',''))
    meme['url'] = memeO['url']
    meme['title'] = memeO['title']

    meme['template_image_url'] = memeO['template_image_url']

    meme['added'] = None

    if 'added' in memeO:
        meme['added'] = memeO['added']
    meme['last_update_source'] = memeO['last_update_source']

    meme['meta_description'] = None
    meme['meta_title'] = None
    meme['meta_image'] = None

    if 'meta' in memeO:
        if 'description' in memeO['meta']:
            meme['meta_description'] = memeO['meta']['description']
        elif 'og:description' in memeO['meta']:
            meme['meta_description'] = memeO['meta']['og:description']
        elif 'twitter:description' in memeO['meta']:
            meme['meta_description'] = memeO['metatwitterog:description']

        meme['meta_title'] = memeO['meta']['og:title']
        meme['meta_image'] = urllib.parse.unquote(memeO['meta']['og:image'])

    meme['details_status'] = memeO['details']['status']
    meme['details_origin'] = memeO['details']['origin']
    meme['details_year'] = memeO['details']['year']
    meme['_details_type'] = []
    if 'type' in memeO['details']:
        for tp in memeO['details']['type']:
            meme['_details_type'].append(tp.replace('https://knowyourmeme.com/types/', ''))
    #

    #sep_table
    meme['_tags'] = []
    if 'tags' in memeO:
        meme['_tags'] = sorted(list(set(memeO['tags'])))
    #sep_table
    meme['_search_keywords'] = []
    if 'search_keywords' in memeO:
        meme['_search_keywords'] = sorted(list(set(memeO['search_keywords'])))

    #related memes
    meme['parent'] = None
    if 'parent' in memeO:
        meme['parent'] = memeO['parent'].replace('https://knowyourmeme.com/','')

    meme['_content'] = {}
    if 'content' in memeO:
        for s in memeO['content']:
            section = {'title': None, '_texts':[], '_links':[], '_images':[]}
            section['title']=s
            sec = memeO['content'][s]
            if 'text' in sec:
                for it in sec['text']:
                    section['_texts'].append(it)
            if 'links' in sec:
                for it in sec['links']:
                    section['_links'].append({'title': it[0], 'url': it[1].replace('https://knowyourmeme.com/','')})
            if 'images' in sec:
                for it in sec['images']:
                    section['_images'].append(it)
            meme['_content'][s] = section

    if meme['Id'] in new_memes:
        sys.stderr.write('We have a problem with nonunique ID:', meme['id'])
        exit()

    new_memes[meme['Id']] = meme


sys.stdout.write(json.dumps(new_memes, ensure_ascii=False, sort_keys=True))
sys.stderr.write("\nDone.\n")
