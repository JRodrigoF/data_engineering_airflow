#!/usr/bin/env python
# -*- coding: utf-8 -*-

## pip install --force-reinstall pandas
## pip install -U scikit-learn
import sys
import pandas as pd
import numpy as np
import scipy.sparse
import time
import json
import argparse
from sklearn.feature_extraction.text import  CountVectorizer

def common_words(corpus):
    vect = CountVectorizer()
    tfidf = vect.fit_transform(corpus)
    pairwise_similarity = tfidf * tfidf.T

    #set similary of compraring with self to -1
    n, _ = pairwise_similarity.shape
    pairwise_similarity[np.arange(n), np.arange(n)] = -1.0

    return (pairwise_similarity)


"""
 calculates common_tags of memes
"""

start_time = time.time()

# parsing command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)

args = parser.parse_args()

filename = args.file
outputfile = args.out
threshold = 0.0


if not filename:
    sys.stderr.write("Error! No input file specified.\n")
    exit(1)


sys.stderr.write(f"Read from {filename}.\n")

# read all entries from file
with open(filename,'r') as f:
    memes_dict = json.load(f)
    f.close()

outputfilename = f'memes_common_tags.tsv'

cnt = 0
memes_data = []
tags_dict = {}
for (id, meme) in memes_dict.items():
    convertedTags = []
    for tag in meme['_tags']:
        if not tag in tags_dict:
            tags_dict[tag] = len(tags_dict)
        convertedTags.append(str(tags_dict[tag]))
    memes_data.append((meme['Id'], meme['meta_description'],' '.join(convertedTags)))

df = pd.DataFrame(memes_data, columns=['Id', 'desc', 'tag'])

#replace None with ""
df.fillna("",inplace=True)


sys.stderr.write(f"Total memes to compare: {df.shape[0]} \n")


# Convert to TF-IDF vectors
calculations_start_time  = time.time()

pairwise_similarity = common_words(df['tag'])

#min_score = pairwise_similarity.min()
#max_score = pairwise_similarity.max()


if threshold:
    sys.stderr.write(f"Scores threshold: {threshold} \n")

result_table = {'meme1':[], 'meme2':[], 'score':[]}


converting_matrix_start_time  = time.time()
sys.stderr.write("Converting similarity matrix to dictionary... \n")


cx = scipy.sparse.coo_matrix(pairwise_similarity)

for i,j,v in zip(cx.row, cx.col, cx.data):
    if v < threshold: continue
    result_table['meme1'].append(df['Id'][i])
    result_table['meme2'].append(df['Id'][j])
    result_table['score'].append(v)
sys.stderr.write("--- Converting similarity sparse matrix to dictionary: %s seconds --- \n" % (time.time() - converting_matrix_start_time))


writing_csv_start_time  = time.time()
dfScores = pd.DataFrame.from_dict(result_table)
dfScores.to_csv(outputfile, sep='\t', index = False)

sys.stderr.write("--- Writing %s : %s seconds, %i rows --- \n" % (outputfile,  time.time() - writing_csv_start_time, dfScores.shape[0]))

sys.stderr.write("--- Script total execution time:  %s seconds ---\n" % (time.time() - start_time))

exit(0)
