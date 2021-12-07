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
from sklearn.feature_extraction.text import TfidfVectorizer


"""
    Calculates cosine similarity of meme descriptions or tags.
    Writes result in tsv file with columns: index, meme1Id, meme2Id, score

    Mandatory input parameters:
                -f kym_prepared.json file
                -t field that should be compared. Supported values: [tag, desc]
    Other parameters
                -o output filename
                -l threshold for scores (float)

    TODO
        Check if there is some faster way to convert sparse matrix into dictionary
            cx = scipy.sparse.coo_matrix(pairwise_similarity)
            for i,j,v in zip(cx.row, cx.col, cx.data):
        Check how difficult is to calculate semantic difference score instead of cosine difference
"""


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
parser.add_argument("--type", "-t", type=str, required=True)
parser.add_argument("--threshold", "-l", type=float, required=False)
parser.add_argument("--outputfile", "-o", type=str, required=False)
args = parser.parse_args()

filename = args.file
type = args.type
outputfile = args.outputfile
threshold = args.threshold
if not outputfile:
    outputfile = f'memes_{type}_similarity_score.tsv'
if not filename:
    sys.stderr.write("Error! No input file specified.\n")
    exit()
if not threshold:
    threshold = 0.0
if not type in ['tag', 'desc']:
    sys.stderr.write("-t Type should be 'tag' or 'desc'.\n")
    exit()

sys.stderr.write(f"Read from {filename}.\n")

# read all entries from file
with open(filename,'r') as f:
    memes_dict = json.load(f)
    f.close()

outputfilename = f'memes_similarity_score_{type}.tsv'

cnt = 0
memes_data = []
for (id, meme) in memes_dict.items():
    if not meme['category'] in 'Meme': continue
    if not meme['details_status'] in ['submission', 'confirmed']: continue
    memes_data.append((meme['Id'], meme['meta_description'],' '.join(meme['_tags'])))
    #cnt+=1
    #if cnt>100:break


df = pd.DataFrame(memes_data, columns=['Id', 'desc', 'tag'])

#replace None with ""
df.fillna("",inplace=True)

#print (df.head()); exit();

"""
# TODO: delete if not needed
# in case input data would be in tsv format
#load data from json
mandatory_columns = ['Id', ]
for col in mandatory_columns:
    if not col in df.columns:
        sys.stderr.write(f'ERROR: Mandatory column {col} not found in {inputfilename}\n')
        exit()
df['text'] = df['meta_description']
"""


sys.stderr.write(f"Total memes to compare: {df.shape[0]} \n")


# Convert to TF-IDF vectors
calculations_start_time  = time.time()

pairwise_similarity = calculate_cosine_similarity(df[type])

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
dfScores.to_csv(outputfile, sep='\t')

sys.stderr.write("--- Writing %s : %s seconds, %i rows --- \n" % (outputfile,  time.time() - writing_csv_start_time, dfScores.shape[0]))

sys.stderr.write("--- Script total execution time:  %s seconds ---\n" % (time.time() - start_time))
