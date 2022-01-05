import datetime
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


DAGS_FOLDER = '/opt/airflow/dags/'
SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'

TASK1_FOLDER = DAGS_FOLDER + 'output/task_1/'
OUTPUT_FOLDER = DAGS_FOLDER + 'output/task_4/'

USE_PRECALCULATED_MEMES_SIMILARITY_DATA = 1

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2021, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}


pipeline = DAG(
    dag_id='task_4',
    default_args=default_args_dict,
    catchup=False,
)

create_dag_folder = BashOperator(
    task_id='create_dag_folder',
    dag=pipeline,
    bash_command=("mkdir -p  {OUTPUT_FOLDER}").format(OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='none_failed'
)

#bash_command= ("tar xvzf {DATA_FOLDER}precalculated_memes_similarity_score.tsv.gz {OUTPUT_FOLDER} "
#            " && mv {OUTPUT_FOLDER}precalculated_memes_similarity_score.tsv {OUTPUT_FOLDER}{epoch}_memes_similarity_score.tsv"
#        ).format(DATA_FOLDER=DATA_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER, epoch="{{ execution_date.int_timestamp }}"),

# lets use precalculated tsv, as this script runs for ca 7 minutes
if USE_PRECALCULATED_MEMES_SIMILARITY_DATA:

    make_meme_similarity_facts_csv = BashOperator(
        task_id='make_meme_similarity_facts_csv',
        dag=pipeline,
        bash_command= ("gunzip -c {DATA_FOLDER}precalculated_memes_similarity_score.tsv.gz > {OUTPUT_FOLDER}{epoch}_memes_similarity_score.tsv"
            ).format(DATA_FOLDER=DATA_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER, epoch="{{ execution_date.int_timestamp }}"),
        trigger_rule='all_success',
        depends_on_past=False,
    )
else:
    make_meme_similarity_facts_csv = BashOperator(
        task_id='make_meme_similarity_facts_csv',
        dag=pipeline,
        bash_command= ("python "
            + " {SCRIPTS_FOLDER}memes_similarity_facts.py "
            + " --file {TASK1_FOLDER}kym_unique_filter_1.json "
            + " --threshold=0.5 "
            + " --out {OUTPUT_FOLDER}{epoch}_memes_similarity_score.tsv"
            ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER
                , TASK1_FOLDER=TASK1_FOLDER
                , OUTPUT_FOLDER=OUTPUT_FOLDER
                , epoch="{{ execution_date.int_timestamp }}"),
        trigger_rule='all_success',
        depends_on_past=False,
    )

extract_lda_topics = BashOperator(
    task_id='extract_lda_topics',
    dag=pipeline,
    bash_command = ("python "
        + " {SCRIPTS_FOLDER}lda_topics.py "
        + " --file {TASK1_FOLDER}kym_unique_filter_1.json "
        + " --outtopics {OUTPUT_FOLDER}{epoch}_lda_topics.tsv "
        + " --outmemes {OUTPUT_FOLDER}{epoch}_memes_lda_topics.tsv "
        + " --outkeywords {OUTPUT_FOLDER}{epoch}_lda_topic_keywords.tsv"
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER
            , TASK1_FOLDER=TASK1_FOLDER
            , OUTPUT_FOLDER=OUTPUT_FOLDER
            , epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)


files = ['memes_similarity_score.tsv'
    , 'lda_topics.tsv'
    , 'lda_topic_keywords.tsv'
    , 'memes_lda_topics.tsv'
    ]
commands = []
for f in files:
    commands.append('cp {OUTPUT_FOLDER}{epoch}_%s {OUTPUT_FOLDER}%s '% (f, f))
command = " && ".join(commands)
rename_output_files = BashOperator(
    task_id='rename_output_files',
    dag=pipeline,
    bash_command=(command).format(
            OUTPUT_FOLDER=OUTPUT_FOLDER
            , epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False
)

sink = DummyOperator(
    task_id='sink',
    dag=pipeline,
    trigger_rule='none_failed'
)

create_dag_folder >> [make_meme_similarity_facts_csv, extract_lda_topics] >> rename_output_files >> sink
