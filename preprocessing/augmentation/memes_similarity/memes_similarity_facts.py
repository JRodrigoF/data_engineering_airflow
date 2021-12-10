#!/usr/bin/env python
# -*- coding: utf-8 -*-

## pip install --force-reinstall pandas
## pip install -U scikit-learn
"""
    Calculates memes similarity
        * number of common tags
        * tags cosine similarity (1.0 if tags are identical)
        * description cosine similarity


    Writes result in tsv file with columns:
        meme1Id, meme2Id, common_tags, tags_similarity, desc_similarity

    Only those meme pairs are included, where desc similarity is above threshold or they do have common tags.

    Similarity scores are rounded to 2 decimals

    Mandatory input parameters:
                -f kym_prepared.json file
                -o output filename

    Other parameters
                -l threshold for scores (float)
    TODO
        Check if there is some faster way to convert sparse matrix into dictionary
            cx = scipy.sparse.coo_matrix(pairwise_similarity)
            for i,j,v in zip(cx.row, cx.col, cx.data):
        Check how difficult is to calculate semantic difference score instead of cosine difference
"""
import sys
import pandas as pd
import numpy as np
import scipy.sparse
import time
import json
import argparse
from sklearn.feature_extraction.text import  CountVectorizer, TfidfVectorizer

def common_words(corpus):
    vect = CountVectorizer()
    tfidf = vect.fit_transform(corpus)
    pairwise_similarity = tfidf * tfidf.T

    #set similary of compraring with self to -1
    n, _ = pairwise_similarity.shape
    pairwise_similarity[np.arange(n), np.arange(n)] = -1.0
    return (pairwise_similarity)

# https://stackoverflow.com/questions/8897593/how-to-compute-the-similarity-between-two-text-documents
def calculate_cosine_similarity(corpus):
    vect = TfidfVectorizer(min_df=1, stop_words="english")
    tfidf = vect.fit_transform(corpus)

    #https://docs.scipy.org/doc/scipy/reference/sparse.html
    pairwise_similarity = tfidf * tfidf.T

    #set similary of compraring with self to -1
    n, _ = pairwise_similarity.shape
    pairwise_similarity[np.arange(n), np.arange(n)] = -1.0

    return (pairwise_similarity)

start_time = time.time()

# parsing command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
parser.add_argument("--threshold", "-l", type=float, required=True)

args = parser.parse_args()

filename = args.file
outputfile = args.out
threshold = args.threshold

if not filename:
    sys.stderr.write("Error! No input file specified.\n")
    exit(1)

if not threshold:
    threshold = 0.1
args = parser.parse_args()

filename = args.file
outputfile = args.out
threshold = 0.1


if not filename:
    sys.stderr.write("Error! No input file specified.\n")
    exit(1)


sys.stderr.write(f"Read from {filename}.\n")

# read all entries from file
with open(filename,'r') as f:
    memes_dict = json.load(f)
    f.close()

outputfilename = f'memes_similarity_facts.tsv'

cnt = 0
memes_data = []
#convert tags to numbers, as they may contain same words
tags_dict = {}
m_dict = {}
for (id, meme) in memes_dict.items():
    convertedTags = []
    for tag in meme['_tags']:
        if not tag in tags_dict:
            tags_dict[tag] = len(tags_dict)
        convertedTags.append(str(tags_dict[tag]))
    if not meme['Id'] in m_dict:
        m_dict[meme['Id']] = len(m_dict)

    memes_data.append((m_dict[meme['Id']], meme['meta_description'],' '.join(convertedTags)))

df = pd.DataFrame(memes_data, columns=['Id', 'desc', 'tag'])

#replace None with ""
df.fillna("",inplace=True)

sys.stderr.write(f"Total memes to compare: {df.shape[0]} \n")

# Convert to TF-IDF vectors
calculations_start_time  = time.time()

pairwise_common_tags = common_words(df['tag'])
pairwise_desc_similarity = calculate_cosine_similarity(df['desc'])
pairwise_desc_similarity.data=np.round(pairwise_desc_similarity.data,2)

pairwise_tags_similarity = calculate_cosine_similarity(df['tag'])
pairwise_tags_similarity.data=np.round(pairwise_tags_similarity.data,2)

if threshold:
    sys.stderr.write(f"Description cosine similarity scores threshold: {threshold} \n")

result_table = {'meme1':[], 'meme2':[], 'common_tags':[], 'tags_similarity':[], 'desc_similarity':[]}


converting_matrix_start_time  = time.time()
sys.stderr.write("Converting similarity matrix to dictionary... \n")

combined_result = pairwise_common_tags + pairwise_desc_similarity

cx = scipy.sparse.coo_matrix(combined_result)
sys.stderr.write("Creating output dataframe... \n")

for i,j,v in zip(cx.row, cx.col, cx.data):
    if v < threshold: continue
    result_table['meme1'].append(df['Id'][i])
    result_table['meme2'].append(df['Id'][j])
    result_table['common_tags'].append(pairwise_common_tags[i,j])
    result_table['tags_similarity'].append(pairwise_tags_similarity[i,j])
    result_table['desc_similarity'].append(pairwise_desc_similarity[i,j])
sys.stderr.write("--- Time: Converting similarity sparse matrix to dictionary: %s seconds --- \n" % (time.time() - converting_matrix_start_time))


writing_csv_start_time  = time.time()
dfScores = pd.DataFrame.from_dict(result_table)
dfScores.to_csv(outputfile, sep='\t', index = False)

sys.stderr.write("--- Time: Writing %s : %s seconds, %i rows --- \n" % (outputfile,  time.time() - writing_csv_start_time, dfScores.shape[0]))
sys.stderr.write("--- Time: Script total execution time:  %s seconds ---\n" % (time.time() - start_time))

exit(0)
