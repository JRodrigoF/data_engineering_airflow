#!/usr/bin/env python
# -*- coding: utf-8 -*-

## pip install --force-reinstall pandas
## pip install -U scikit-learn

"""
    sources:
    https://www.analyticsvidhya.com/blog/2021/06/part-3-topic-modeling-and-latent-dirichlet-allocation-lda-using-gensim-and-sklearn/
    https://www.kaggle.com/rajmehra03/topic-modelling-using-lda-and-lsa-in-sklearn

    TODO:
        * check what is optimal number of topics
        * check what is the optimal number of keywords
        * extract more topic specific stopwords
"""

from sklearn.feature_extraction.text import TfidfVectorizer

import sys
import pandas as pd
import numpy as np
import scipy.sparse
import time
import json
import argparse

#preprocessing
import nltk
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')
 #stopwords
from nltk.corpus import stopwords
# tokenizing
from nltk import word_tokenize, pos_tag_sents, pos_tag
from nltk.tokenize import sent_tokenize
from nltk.stem import WordNetLemmatizer  # lammatizer from WordNet

# vectorizers for creating the document-term-matrix (DTM)
from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer


TOPICS = 10
KEYWORDS = 10


# Field specific stopwords were extracted from (filtered) memes content texts.
# 200 most frequent words were taken.

custom_stopwords = ['post', 'show', 'user', 'video', 'use', 'feature', 'first', 'image', 'one', 'title', 'like', 'series', 'also', 'day', 'upload', '000', 'meme', '10', 'reddit', 'make', 'youtube', 'gain', 'character', 'create', 'site', 'include', 'year', 'right', '11', 'original', 'know', 'several', 'view', 'begin', 'online', 'may', 'leave', 'become', 'popular', 'page', 'submit', 'internet', 'time', 'comment', 'two', 'many', '12', 'search', 'receive', 'tumblr', 'twitter', 'appear', '2012',     'blog', 'people', '2011', 'game', 'new', 'april', 'within', 'october', 'march', 'july', 'january', 'august', 'november', 'various', 'since', 'june', 'following', 'publish', 'facebook', '13', 'february', 'take', 'thread', 'month', 'september', 'say', '2010', 'parody', 'december', 'often', 'phrase', 'name', 'wikipedia', '2013', 'release', 'well', 'news', 'version', 'subreddit', 'later', 'get', 'youtuber', 'go', 'article', 'redditor', 'see', '14', '2014', 'picture', 'find', 'caption', 'next', 'earliest', 'example', 'macro', '2009', 'com', 'song', 'fan', 'call', 'launch', '2015', 'another', '15', '2008', '4chan', 'come', 'note', 'result', 'million', 'share', 'man', 'similar', 'tweet', 'follow', 'photo', '2016', 'early', 'archive', 'point', 'play', 'start', 'website', 'would', 'spread', 'board', '16', '2007', 'photograph', 'base', 'forum', 'film', 'three', 'refers', 'ask', 'episode', 'urban', 'via', 'scene', 'joke', 'dictionary', 'music', 'daily', 'around', 'reference', 'give', 'week', 'hour', '17', 'tv', 'community', 'popularity', 'reaction', 'clip', 'look', 'vote', 'world', 'due', '2006', '24', 'face', 'word', '20', 'garner', 'buzzfeed', '2017', 'way', 'write', '18', 'best', 'medium', 'upwards', 'term', 'viral', 'single', 'channel', 'instance', 'back', 'star', '1st', 'list', '100', 'inspire', 'among', 'group', 'however', 'american',
'end', 'tag', 'account', 'web', 'response', '6th', 'cover', 'funny', 'available', '4th', '5th', 'machine', 'story', '3rd', 'late', 'comic', '27th', 'member', '2nd', 'even', 'part', '7th', 'refer', 'japanese', 'air', '11th', '20th', '9th', 'number', '10th', '19', 'subject', 'quote', '8th', 'describe', 'art', 'notable', '17th', 'entry', '13th', 'social', 'life', '12th', 'known', 'prior', 'lead', 'different', '14th', '15th', '19th', 'television', '21st', 'woman', 'originally', 'place', 'something', '18th', 'along', 'variation', 'expression', 'second', '23rd', 'style', '28th', '16th', '25th', 'culture', 'humor', 'four', 'photoshopped', 'according', '24th', 'highlight', 'person', 'typically', 'official', '500', 'gather', 'continue', '22nd', 'throughout', 'topic', 'still', 'top', '200', '26th', 'hold', 'retweets', 'wayback', 'anime', 'mock', 'line', '30th', 'report', 'associate', '21', 'others', 'wiki', '29th', 'thing', '2005', 'need', 'movie', 'someone', 'accumulate', 'upvoted', 'work', 'add', 'claim', 'message', 'animated', 'guy', 'pop', 'much', '300', 'text', 'remix', 'link', 'contain', 'quickly', 'catchphrase', 'could', 'depict', '400', 'english', 'google', 'player', 'discussion', 'girl', 'results', 'le', 'deviantart', 'trend', 'form', 'compilation', 'question', 'section', '22', 'origin', 'little', 'soon', 'funnyjunk', 'mean', 'forums', 'friend', 'read', '2004', 'reach', 'real', 'set', 'artist', 'though', '23', 'live', 'edit', '600', 'wear', 'mention', 'reply', 'usually', 'love', 'exploitable', 'five', 'think', 'tell', 'date', 'hashtag', 'hit', 'attempt', 'remixes', 'animal', 'poster', 'memes', 'spawn', 'originate', 'child', 'famous', '25', 'black', 'short', 'footage', '800', 'watch', 'appearance', 'accompany', 'interview', 'try', 'old', 'run', 'front', 'instagram', 'head', 'involve', 'favorite', 'talk', 'ever', 'photoshop', '700', '30', 'explain']

def clean_text_lemmatization(text):
    """
        splits to sentences and tokens, lemmatizes
        removes
            * stopwords [default stopwords + topic specific words]
            * words shorther than 4 chars
        returns cleaned lemmatized text
    """
    global stop_words
    wnl = WordNetLemmatizer()
    tagged_text = pos_tag_sents(map(word_tokenize, [text]))
    tagged_words = []
    for sent in tagged_text:
        for w  in sent:
            tagged_words.append(w)

    tokens =[wnl.lemmatize(word,pos[0].lower()) if pos[0].lower() in ['a','n','v'] else wnl.lemmatize(word) for word,pos in tagged_words]
    tokens = [t for t in tokens if t not in stop_words and len(t)>3]
    return " ".join(tokens)



start_time = time.time()

# parsing command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--outtopics", "-t", type=str, required=True)
parser.add_argument("--outkeywords", "-k", type=str, required=True)
parser.add_argument("--outmemes", "-m", type=str, required=True)
args = parser.parse_args()

filename = args.file
outtopics = args.outtopics
outmemes = args.outmemes
outkeywords = args.outkeywords
if not filename:
    sys.stderr.write("Error! No input file specified.\n")
    exit(1)
if not outtopics:
    sys.stderr.write("Error! No topics output file specified.\n")
    exit(1)
if not outmemes:
    sys.stderr.write("Error! No meme topics output file specified.\n")
    exit(1)
if not outkeywords:
    sys.stderr.write("Error! No topics keywords output file specified.\n")
    exit(1)

sys.stderr.write(f"Read from {filename}.\n")

# read all entries from file
with open(filename,'r') as f:
    memes_dict = json.load(f)
    f.close()

memes_data = []
for (id, meme) in memes_dict.items():
    memes_data.append( ( meme['Id'], meme['meta_description']) )

df = pd.DataFrame(memes_data, columns=['Id', 'desc'])

#replace None with ""
df.fillna("",inplace=True)

#default stopwords + custom_stopwords
stop_words=set(stopwords.words('english'))
[stop_words.add(w) for w in custom_stopwords]

#clean and lemmatize text
df['cleaned_desc']=df['desc'].apply(clean_text_lemmatization)

# EXTRACTING THE FEATURES AND CREATING THE DOCUMENT-TERM-MATRIX ( DTM ) In DTM the values are the TFidf values.

vect = TfidfVectorizer(stop_words=stop_words,max_features=1000)
vect_text = vect.fit_transform(df['cleaned_desc'])

# LDA TOPIC MODELLING

from sklearn.decomposition import LatentDirichletAllocation
lda_model = LatentDirichletAllocation(n_components=TOPICS,learning_method='online',random_state=42,max_iter=20)
lda_top = lda_model.fit_transform(vect_text)
vocab = vect.get_feature_names_out()


#   collectiong data
# add Unknown topic for topics where is hard to decide in which topic it belongs
extracted_topics = [(-1, 'Unknown')]
extracted_topic_keywords = []

for i, comp in enumerate(lda_model.components_):
    vocab_comp = zip(vocab, comp)
    extracted_topics.append( (i+1, f'Topic {i+1}'))
    for w in sorted(vocab_comp, key= lambda x:x[1], reverse=True)[:KEYWORDS]:
        extracted_topic_keywords.append((i+1, w[1], w[0],))

memeTopics = []

for j in range(df['Id'].shape[0]):
    ([topic]) = np.where(lda_top[j] == max(lda_top[j]))
    if len(topic) > 1:
        topicId = '-1'
        topicProbScore = 0.0
    else:
        topic = topic[0]
        topicId = topic + 1
        topicProbScore = lda_top[j][topic]
    memeTopics.append( (df['Id'][j], topicId, topicProbScore ) )


# writing extracted data to to files

writing_csv_start_time  = time.time()
dfTopcsKeywords = pd.DataFrame(extracted_topic_keywords, columns=('topicId', 'keywords', 'keywordScores',))
dfTopcsKeywords.to_csv(outkeywords, sep='\t', index = False)
sys.stderr.write("--- Writing %s : %s seconds, %i rows --- \n" % (outkeywords,  time.time() - writing_csv_start_time, dfTopcsKeywords.shape[0]))


writing_csv_start_time  = time.time()
dfTopcs = pd.DataFrame(extracted_topics, columns=('topicId', 'topicName',))
dfTopcs.to_csv(outtopics, sep='\t', index = False)
sys.stderr.write("--- Writing %s : %s seconds, %i rows --- \n" % (outtopics,  time.time() - writing_csv_start_time, dfTopcs.shape[0]))


writing_csv_start_time  = time.time()
dfMemesTopics = pd.DataFrame(memeTopics, columns=('Id', 'topicId', 'topicProbScore'))
dfMemesTopics.to_csv(outmemes, sep='\t', index = False)
sys.stderr.write("--- Writing %s : %s seconds, %i rows --- \n" % (outmemes,  time.time() - writing_csv_start_time, dfMemesTopics.shape[0]))
sys.stderr.write("--- Script total execution time:  %s seconds ---\n" % (time.time() - start_time))

exit(0)
