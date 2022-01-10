from plotnine import *
import argparse
import pandas as pd

"""
Creates plots from tsv files made from sql queries
"""

parser = argparse.ArgumentParser()
parser.add_argument("--folder", "-f", type=str, required=True)
parser.add_argument("--prefix", "-p", type=str, required=False)
args = parser.parse_args()

prefix = ''
if args.prefix:
    prefix = args.prefix
if not prefix.endswith('-'):
    prefix += '_'

data_folder = args.folder + prefix
# data_folder = args.folder

my_palette = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3',
                '#ff7f00', '#ffff33', '#a65628', '#f781bf']

# query 01 plot
df = pd.read_csv(data_folder + 'query_01.tsv', sep="\t")

p = ggplot(data=df)
p = p + geom_point(aes('year', 'total', color='details_status'),
                    size=0.4, alpha=0.9)
p = p + geom_line(aes('year', 'total', color='details_status'),
                    size=0.4, alpha=0.9)
p = p + scale_x_continuous(breaks=list(df.year))
p = p + scale_color_manual(my_palette)
p = p + labs(x='Year', y='Total', title="Memes per year")
p = p + theme_classic()
p = p + theme(axis_text_x=element_text(rotation=45, hjust=1))
# display(p)
output_file = data_folder + 'query_01.png'
p.save(output_file, dpi=300)

# query 02 plot
df = pd.read_csv(data_folder + 'query_02.tsv', sep="\t")

df = (df
    .replace({0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'})
)

p = ggplot(data=df)
p = p + geom_bar(aes(x='weekday', y='total'),
                fill=my_palette[3], stat="identity")
p = p + labs(x='', y='Total', title="Memes per day of week")
p = p + theme_classic()
# display(p)
output_file = data_folder + 'query_02.png'
p.save(output_file, dpi=300)

# query 03 plot
df = pd.read_csv(data_folder + 'query_03.tsv', sep="\t")

p = ggplot(data=df, mapping=aes(x='safeness_category',
                                y='total', fill='safeness_category'))
p = p + scale_y_log10()
p = p + geom_col()
p = p + coord_flip()
p = p + scale_fill_brewer(type='div', palette="Spectral")
p = p + theme_classic() + ggtitle('Safeness categories')
p = p + labs(x='', y='(log) Total', title="Google Vision enrichment")
# display(p)
output_file = data_folder + 'query_03.png'
p.save(output_file, dpi=300)
