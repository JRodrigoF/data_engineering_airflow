
## lda_topics.py

Scipt generates N (10) topics based on meme descriptions (LDA model).

Result is stored in 3 tsv files file with following columns:
  LDA topics file (lda_topics.tsv)
    topicId
    topicName
  LDA topics keywords (lda_topic_keywords.tsv)
    topicId
    keyword
    keywordScore
  Memes LDA topics  (memes_lda_topics.tsv)
    memeId
    topicId
    topicProbScore


### Usage example

```
%python extract_lda_topics.py -f ../../../pipeline_1/dags/data/kym_unique_filter_1.json --outtopics lda_topics.tsv --outmemes memes_lda_topics.tsv --outkeywords lda_topic_keywords.tsv


Read from ../../../pipeline_1/dags/data/kym_unique_filter_1.json.
--- Writing lda_topic_keywords.tsv : 0.0028600692749023438 seconds, 100 rows ---
--- Writing lda_topics.tsv : 0.0021028518676757812 seconds, 11 rows ---
--- Writing memes_lda_topics.tsv : 0.024268150329589844 seconds, 8174 rows ---
--- Script total execution time:  31.204676151275635 seconds ---
```
