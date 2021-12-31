#!/usr/bin/env python

import argparse
import pandas as pd
import numpy as np

"""
Creates Memes Fact table
"""

parser = argparse.ArgumentParser()
parser.add_argument("--folder", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

dims_folder = args.folder
output = args.out

df_memes = pd.read_csv(dims_folder + "memes.tsv", sep="\t")
df_children = pd.read_csv(
    dims_folder + "parent_children_relations.tsv", sep="\t")
df_safeness = pd.read_csv(dims_folder + "meme_safeness_relations.tsv", sep="\t")
df_tags = pd.read_csv(dims_folder + "meme_tags.tsv", sep="\t")
df_examples = pd.read_csv(dims_folder + "meme_content_examples.tsv", sep="\t")

dim_dates = pd.read_csv(dims_folder + "dim_dates.tsv", sep="\t")
dim_status = pd.read_csv(dims_folder + "dim_status.tsv", sep="\t")
dim_parents = pd.read_csv(dims_folder + "dim_parents.tsv", sep="\t")
dim_origins = pd.read_csv(dims_folder + "dim_origins.tsv", sep="\t")
dim_safeness = pd.read_csv(dims_folder + "dim_safeness_gv.tsv", sep="\t")

status_dict = dim_status.set_index('details_status').to_dict()

df_children = (df_children
    .assign(memeId=lambda x: x['memeId']
    .str.replace('memes/', ''))
)

df_memes = (df_memes
    .rename(columns={'Id': 'key_KYM'})
    .drop(['category', 'title', 'meta_description', 'template_image_url',
        'url', 'details_year'], axis=1)

    # date_added_id
    .assign(added=df_memes
        .replace({'None': np.nan})
        .apply(lambda x: x['added'] if pd.notnull(x['added'])
        else x['last_update_source'], axis='columns')
        .astype('int64')
    )
    .merge(dim_dates, left_on=['added'], right_on=['timestamp'], how='left')
    .drop(['added', 'timestamp', 'year', 'month', 'day', 'weekday'], axis=1)

    # date_updated_id
    .merge(dim_dates, left_on=['last_update_source'], right_on=['timestamp'],
        how='left')
    .drop(['last_update_source', 'timestamp', 'year', 'month', 'day',
        'weekday'], axis=1)

    # dim safeness
    .join(df_safeness.set_index('key_KYM'), on='key_KYM')
    .assign(safeness_category=lambda x: x['safeness_category']
        .replace({np.nan: 'None'})
        .replace('None', 'undefined')
    )
    .drop(['adult', 'spoof', 'medical', 'violence', 'racy'], axis=1)
    .join(dim_safeness.set_index('safeness_category'), on='safeness_category')
    .drop(['safeness_category'], axis=1)

    # dim status
    .assign(status_id=lambda x: x['details_status']
        .replace(status_dict['status_id'])
    )
    .drop(['details_status'], axis=1)

    # dim origin
    .join(dim_origins.set_index('details_origin'), on='details_origin')
    .drop(['details_origin'], axis=1)

    # dim parent
    .assign(has_parent=df_memes
        .replace({'None': np.nan})
        .apply(lambda x: 1 if pd.notnull(x['parent']) else 0, axis='columns')
        .astype('Int32')
    )
    .drop(['parent'], axis=1)

    # children
    .assign(children_total=df_memes
        .merge(df_children, left_on=['Id'], right_on=['memeId'], how='left')
        .groupby(['Id'])['child'].count().reset_index()
        .drop(['Id'], axis=1)
    )

    # examples/variations
    .assign(content_variations_total=df_memes
        .merge(df_examples, left_on=['Id'], right_on=['key_KYM'], how='left')
        .groupby(['Id'])['variation'].count().reset_index()
        .drop(['Id'], axis=1)
    )

    # tags
    .assign(tags_total=df_memes
        .merge(df_tags, left_on=['Id'], right_on=['key_KYM'], how='left')
        .groupby(['Id'])['tag'].count().reset_index()
        .drop(['Id'], axis=1)
    )

    .drop(['key_KYM'], axis=1)
    .rename({'date_id_x': 'date_added_id', 'date_id_y': 'date_updated_id'},
            axis=1)
    .sort_values(['date_added_id'])
    .reset_index(drop=True)

    # Id
    .assign(Id=lambda x: range(1, len(x) + 1))
    .filter(items=['Id', 'date_added_id', 'date_updated_id', 'safeness_id',
                'status_id', 'origin_id', 'has_parent', 'children_total',
                'content_variations_total', 'tags_total'])

)

df_memes.to_csv(output, sep="\t", encoding='utf-8', na_rep='None', index=False)
# display(df_memes)
