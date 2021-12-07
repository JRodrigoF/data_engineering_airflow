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
    TODO:
        * check what is optimal number of topics
        * check what is the optimal number of keywords
        * maybe add more custom stopwords
"""

TOPICS = 20
KEYWORDS = 10

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import style
import seaborn as sns
#configure
# sets matplotlib to inline and displays graphs below the corressponding cell.
%matplotlib inline
style.use('fivethirtyeight')
sns.set(style='whitegrid',color_codes=True)

#import nltk
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize,sent_tokenize

#preprocessing
from nltk.corpus import stopwords  #stopwords
from nltk import word_tokenize,sent_tokenize # tokenizing
from nltk.stem import PorterStemmer,LancasterStemmer  # using the Porter Stemmer and Lancaster Stemmer and others
from nltk.stem.snowball import SnowballStemmer
from nltk.stem import WordNetLemmatizer  # lammatizer from WordNet

# for named entity recognition (NER)
from nltk import ne_chunk

# vectorizers for creating the document-term-matrix (DTM)
from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer

#stop-words
stop_words=set(nltk.corpus.stopwords.words('english'))
