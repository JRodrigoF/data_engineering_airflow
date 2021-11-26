import json
import sys
import argparse

"""
Script to remove duplicate entries from kym.json.
Duplicates are indicated based on meme entry 'url' value.
Script keeps entries with max 'last_update_source' property value.
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
    data = json.load(f)
    f.close()

memes_dict = {}
for d in data:
    key = d['url']
    if not key in memes_dict or d['last_update_source'] > memes_dict[key]['last_update_source']:
        memes_dict[key] = d

# write sorted result as json dict to stdout
# dict key is 'url'
sys.stdout.write(json.dumps(memes_dict, ensure_ascii=False, sort_keys=True))

sys.stderr.write("Done.\n")
