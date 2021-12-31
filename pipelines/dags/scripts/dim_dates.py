#!/usr/bin/env python

import argparse
import pandas as pd
import numpy  as np

"""
Creates Dates dimension table
"""

parser = argparse.ArgumentParser()
parser.add_argument("--file", "-f", type=str, required=True)
parser.add_argument("--out", "-o", type=str, required=True)
args = parser.parse_args()

filename = args.file
output = args.out

df = pd.read_csv(filename, sep="\t")

def concat_columns(df, col1, col2, colname):
        df = pd.DataFrame(pd.concat([df[col1], df[col2]]), columns=[colname])
        return df

df = (df
        .assign(added=df
                .replace({'None': np.nan})
                .apply(lambda x: x['added'] if pd.notnull(x['added']) else x['last_update_source'],
                        axis='columns'))
        .pipe(concat_columns, col1='added', col2='last_update_source', colname='timestamp')
        .groupby(['timestamp']).first()
        .reset_index()
        .assign(date_id=lambda x: range(1, len(x) + 1))
        .assign(year=lambda x: pd.to_datetime(x['timestamp'], unit='s').dt.year)
        .assign(month=lambda x: pd.to_datetime(x['timestamp'], unit='s').dt.month)
        .assign(day=lambda x: pd.to_datetime(x['timestamp'], unit='s').dt.day)
        .assign(weekday=lambda x: pd.to_datetime(x['timestamp'], unit='s').dt.weekday)
        .filter(items=['date_id', 'timestamp', 'year', 'month', 'day', 'weekday'])
)

df.to_csv(output, sep="\t", encoding='utf-8', index=False)
# display(df)
