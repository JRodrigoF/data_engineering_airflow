import datetime
import pandas as pd
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

DAGS_FOLDER = '/opt/airflow/dags/'
SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'
OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipeline_4/'

POSTGRES_CONN_ID = "postgres_default"

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2021, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

pipeline_4 = DAG(
    dag_id='pipeline_4',
    default_args=default_args_dict,
    catchup=False,
)

source = DummyOperator(
    task_id='source',
    dag=pipeline_4,
    trigger_rule='none_failed'
)

KYM_data = DummyOperator(
    task_id='KYM_data',
    dag=pipeline_4,
    trigger_rule='none_failed'
)

Google_Vision_data = DummyOperator(
    task_id='Google_Vision_data',
    dag=pipeline_4,
    trigger_rule='none_failed'
)

GV_data_to_tsv = BashOperator(
    task_id='GV_data_to_tsv',
    dag=pipeline_4,
    bash_command=("python "
    + "{SCRIPTS_FOLDER}GV_data_to_table.py "
    + "--file {DATA_FOLDER}kym_vision.json.gz "
    + "--out {OUTPUT_FOLDER}{epoch}_meme_safeness_relations.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, DATA_FOLDER=DATA_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

safeness_dim_tsv = BashOperator(
    task_id='safeness_dim_tsv',
    dag=pipeline_4,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_safeness_gv.py "
                + "--file {OUTPUT_FOLDER}{epoch}_meme_safeness_relations.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_safeness_gv.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

KYM_fact_table_tsv = BashOperator(
    task_id='KYM_fact_table_tsv',
    dag=pipeline_4,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}fact_table_maker_augmented.py "
                + "--folder {OUTPUT_FOLDER} "
                + "--out {OUTPUT_FOLDER}{epoch}_fact_table_memes.tsv "
                + "--prefix {epoch} ")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

sink = DummyOperator(
    task_id='sink',
    dag=pipeline_4,
    trigger_rule='none_failed'
)

source >> KYM_data >> Google_Vision_data >> GV_data_to_tsv >> safeness_dim_tsv
safeness_dim_tsv >> KYM_fact_table_tsv >> sink

# source >> KYM_data >> remove_duplicates >> filtering >> KYM_data_to_tsv >> \
#     [date_dim_tsv, status_dim_tsv, origin_dim_tsv, memes_dim_tsv] >> \
#     KYM_fact_table_tsv >> db_init >> postgres_db >> populate_db >> sink

# source >> Google_Vision_data >> GV_data_to_tsv >> safeness_dim_tsv >> \
#     KYM_fact_table_tsv >> db_init >> postgres_db >> populate_db >> sink
