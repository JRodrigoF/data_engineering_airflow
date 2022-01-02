import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


import pandas as pd
import os


DAGS_FOLDER = '/opt/airflow/dags/'
OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipeline_3/'
INPUT_FOLDER = DAGS_FOLDER + 'output/pipeline_2/'

SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'
TEMPLATE_FOLDER = DAGS_FOLDER + 'data/'

TIMESTAMP = '1640976918'


default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2021, 1, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

pipeline = DAG(
    dag_id='pipeline_3_lda_similarity',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath = TEMPLATE_FOLDER,
)


start = DummyOperator(
    task_id='start',
    dag=pipeline,
    trigger_rule='none_failed'
)




make_meme_similarity_facts_csv = BashOperator(
    task_id='make_meme_similarity_facts_csv',
    dag=pipeline,
    bash_command= ("python "
        + " {SCRIPTS_FOLDER}memes_similarity_facts.py "
        + " --file {INPUT_FOLDER}{epoch}_kym_unique_filter_1.json "
        + " --threshold=0.5 "
        + " --out {OUTPUT_FOLDER}memes_similarity_score.tsv"
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, INPUT_FOLDER=INPUT_FOLDER, epoch=TIMESTAMP, OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)


extract_lda_topics = BashOperator(
    task_id='extract_lda_topics',
    dag=pipeline,
    bash_command = ("python "
        + " {SCRIPTS_FOLDER}lda_topics.py "
        + " --file {INPUT_FOLDER}{epoch}_kym_unique_filter_1.json "
        + " --outtopics {OUTPUT_FOLDER}lda_topics.tsv "
        + " --outmemes {OUTPUT_FOLDER}memes_lda_topics.tsv "
        + " --outkeywords {OUTPUT_FOLDER}lda_topic_keywords.tsv"
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, INPUT_FOLDER=INPUT_FOLDER, epoch=TIMESTAMP, OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)



end = DummyOperator(
    task_id='end',
    dag=pipeline,
    trigger_rule='none_failed'
)


start >> [make_meme_similarity_facts_csv, extract_lda_topics]>> end
