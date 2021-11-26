#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import argparse
import operator
"""
Script to find kym_uniq.json properties statistcs
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

tagscount={}
def FindValues(dictionary, my_keys=''):
    global tagscount
    result = ''
    for key, value in dictionary.items():
        current_key = my_keys + '[' + key + ']'
        if type(value) is dict:
            FindValues(value, current_key)
        else:
            if not current_key in tagscount:
                tagscount[current_key] = 0
            tagscount[current_key] += 1


for d in memes_dict:
    tags = FindValues(memes_dict[d])

for (t, c) in sorted(tagscount.items(), key=operator.itemgetter(1), reverse = True):
    sys.stdout.write(f"{c}\t{t}\n")
