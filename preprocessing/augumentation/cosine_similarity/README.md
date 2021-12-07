
## calculate_cos_similarity.py

Scipt calculates cosine similary score for _tags or meta_description fields.
Result is stored in tsv file with following columns:
  meme1
  meme2
  score


### Usage example

Calculate tags cosine similarity

```
%python calculate_cos_similarity.py -f kym_prepared.json -t tag -l 0.1 -o memes_tag_similarity_score.tsv

Read from kym_prepared.json.
Total memes to compare: 8174

Scores threshold: 0.1

Converting similarity matrix to dictionary...
--- Converting similarity sparse matrix to dictionary: 9.50686502456665 seconds ---
--- Writing memes_tag_similarity_score.tsv : 3.4615559577941895 seconds, 880176 rows ---
--- Script total execution time:  14.21966004371643 seconds ---

```

Calculate descriptions cosine _similarity_score

```
python calculate_cos_similarity.py -f kym_prepared.json -t desc -l 0.1 -o memes_desc_similarity_score.tsv
Read from kym_prepared.json.
Total memes to compare: 8174

Scores threshold: 0.1
Converting similarity matrix to dictionary...
--- Converting similarity sparse matrix to dictionary: 9.16412091255188 seconds ---
--- Writing memes_desc_similarity_score.tsv : 1.1059858798980713 seconds, 275560 rows ---
--- Script total execution time:  12.411976099014282 seconds ---

```

-rw-r--r--  1 rabauti  staff    18M Dec  7 21:19 memes_desc_similarity_score.tsv
-rw-r--r--  1 rabauti  staff    56M Dec  7 21:19 memes_tag_similarity_score.tsv
