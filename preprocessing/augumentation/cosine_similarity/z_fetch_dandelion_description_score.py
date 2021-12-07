#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import argparse
import csv
import requests

"""
Fetch memes similarity score based on description
"""
API = 'https://api.dandelion.eu/datatxt/sim/v1/'
TOKEN = 'ce40df67a6824865a6f937fe5586e1be'
LANG = 'en'


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


for (Id, meme) in memes_dict.copy().items():
    # TODO do it earlier at cleansing phase
    # remove non Memes
    if not meme['category'] == 'Meme':
        del  memes_dict[Id]
        continue
    if not meme['details_status'] in ('confirmed', 'submission'):
        del  memes_dict[Id]
        continue

#can load data we already have from file
scores = {}



cnt = 0



for (Id1, meme1) in memes_dict.items():
    for (Id2, meme2) in memes_dict.items():
        if cnt > 100: break
        key = tuple(sorted((Id1, Id2,)))
        if key in scores:
            continue
        if Id1 == Id2:
            continue
        cnt +=1
        params = {}

        params['lang'] = LANG
        params['token'] = TOKEN

        params['text1'] = meme1['meta_description']
        params['text2'] = meme2['meta_description']

        response = requests.get(
            API,
            params=params,
        )

        scores[key] = json.loads(response.text)
        #print (cnt)
        #print (Id1, Id2)


print (scores)

    #retry
#params = {}
#params['lang'] = 'en'
#params['text1'] = """That Post Gave Me Cancer is an image macro series based on images of various characters lying sick in bed or otherwise looking unwell, captioned "That post gave me cancer." The catchphrase, usually used in the comments to various posts, indicates that the content posted was of such poor quality that it infected the reader with "cancer," a slang term that is typically used on 4chan to express one's disgust towards a trend or a thread."""
#params['text2'] = """Tom Green began his career in entertainment when he began hosting The Midnight Caller Show a late-night call-in radio show in the University of Ottawa’s radio station CHUO. Together with his friend Glenn Humplick, the two would take calls from listeners which mostly consisted of pranks."""
#params['token'] =
