import datetime
import pandas as pd
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

DAGS_FOLDER = '/opt/airflow/dags/'
SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'
OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipeline_2/'

POSTGRES_CONN_ID = "postgres_default"

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2020, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

pipeline_2 = DAG(
    dag_id='pipeline_2',
    default_args=default_args_dict,
    catchup=False,
)

source = DummyOperator(
    task_id='source',
    dag=pipeline_2,
    trigger_rule='none_failed'
)

KYM_data = DummyOperator(
    task_id='KYM_data',
    dag=pipeline_2,
    trigger_rule='none_failed'
)

remove_duplicates = BashOperator(
    task_id='remove_duplicates',
    dag=pipeline_2,
    bash_command=("mkdir -p {OUTPUT_FOLDER}; python "
                + " {SCRIPTS_FOLDER}remove_duplicates_kym.py "
                + "--file {DATA_FOLDER}kym.json.gz "
                + "--out {OUTPUT_FOLDER}{epoch}_kym_unique.json")
                    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                        DATA_FOLDER=DATA_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER,
                        epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

filtering = BashOperator(
    task_id='filtering',
    dag=pipeline_2,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}prepare_to_tsv_kym.py "
                + "--file {OUTPUT_FOLDER}{epoch}_kym_unique.json "
                + "--out {OUTPUT_FOLDER}{epoch}_kym_unique_filter_1.json")
                    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                        OUTPUT_FOLDER=OUTPUT_FOLDER,
                        epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

KYM_data_to_tsv = BashOperator(
    task_id='KYM_data_to_tsv',
    dag=pipeline_2,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}data_to_tables.py "
                + "--file {OUTPUT_FOLDER}{epoch}_kym_unique_filter_1.json "
                + "--outfolder {OUTPUT_FOLDER} "
                + "--prefix {epoch} ")
                    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                        OUTPUT_FOLDER=OUTPUT_FOLDER,
                        epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

memes_dim_tsv = BashOperator(
    task_id='memes_dim_tsv',
    dag=pipeline_2,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}make_memes_dim_tsv.py "
                + "--file {OUTPUT_FOLDER}{epoch}_kym_unique_filter_1.json "
                + "--outfile {OUTPUT_FOLDER}{epoch}_memes_dim.tsv ")
                    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                        OUTPUT_FOLDER=OUTPUT_FOLDER,
                        epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

status_dim_tsv = BashOperator(
    task_id='status_dim_tsv',
    dag=pipeline_2,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}dim_status.py "
                + "--file {OUTPUT_FOLDER}{epoch}_memes.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_status.tsv")
                    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                        OUTPUT_FOLDER=OUTPUT_FOLDER,
                        epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

date_dim_tsv = BashOperator(
    task_id='date_dim_tsv',
    dag=pipeline_2,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_dates.py "
                + "--file {OUTPUT_FOLDER}{epoch}_memes.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_dates.tsv")
                    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                        OUTPUT_FOLDER=OUTPUT_FOLDER,
                        epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

origin_dim_tsv = BashOperator(
    task_id='origin_dim_tsv',
    dag=pipeline_2,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_origin.py "
                + "--file {OUTPUT_FOLDER}{epoch}_memes.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_origins.tsv")
                    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                        OUTPUT_FOLDER=OUTPUT_FOLDER,
                        epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

KYM_fact_table_tsv = BashOperator(
    task_id='KYM_fact_table_tsv',
    dag=pipeline_2,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}fact_table_maker.py "
                + "--folder {OUTPUT_FOLDER} "
                + "--out {OUTPUT_FOLDER}{epoch}_fact_table_memes.tsv "
                + "--prefix {epoch} ")
                    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                        OUTPUT_FOLDER=OUTPUT_FOLDER,
                        epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

db_init = PostgresOperator(
    task_id='db_init',
    dag=pipeline_2,
    postgres_conn_id='postgres_default',
    sql=[
        'DROP DATABASE IF EXISTS memes',
        'CREATE DATABASE memes',
    ],
    trigger_rule='all_success',
    autocommit=True,
)

postgres_db = PostgresOperator(
    task_id='postgres_db',
    dag=pipeline_2,
    postgres_conn_id='postgres_default',
    # sql='{}create_tables.sql'.format(SCRIPTS_FOLDER), # does not find the file
    sql='sql/create_tables.sql',
    trigger_rule='all_success',
    autocommit=True,
)

def _postgres_populate_db(output_folder: str, postgres_conn_id: str, epoch: str):
    hook = PostgresHook.get_hook(postgres_conn_id)
    # tables = ['dim_dates', 'dim_status', 'dim_origins', 'dim_safeness_gv',
    #           'fact_table_memes']:
    tables = ['dim_dates', 'dim_status', 'dim_origins', 'fact_table_memes']
    for table_name in tables:
        tmp_table = f'{output_folder}/temp_{epoch}_{table_name}.tsv'
        df_temp = pd.read_csv(
            f'{output_folder}/{epoch}_{table_name}.tsv', sep="\t")
        df_temp.to_csv(tmp_table, sep="\t", encoding='utf-8', na_rep='None',
                        header=False, index=False)
        # hook.run(f"COPY {table_name} FROM '{tmp_table}.tsv' DELIMITER '\t' CSV HEADER")
        hook.bulk_load(
            table_name, tmp_table)
        os.remove(tmp_table)

populate_db = PythonOperator(
    task_id='populate_db',
    dag=pipeline_2,
    python_callable=_postgres_populate_db,
    op_kwargs={
        'output_folder': OUTPUT_FOLDER,
        'postgres_conn_id': POSTGRES_CONN_ID,
        'epoch': "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
)

sink = DummyOperator(
    task_id='sink',
    dag=pipeline_2,
    trigger_rule='none_failed'
)

# source >> KYM_data >> remove_duplicates >> filtering >> KYM_data_to_tsv >> sink

source >> KYM_data >> remove_duplicates >> filtering >> KYM_data_to_tsv >> \
    [date_dim_tsv, status_dim_tsv, origin_dim_tsv, memes_dim_tsv] >> \
    KYM_fact_table_tsv >> db_init >> postgres_db >> populate_db  >> sink

# source >> GV_data >> GV_tables >> safeness_dim >> Memes_Fact_Table_tsv >> sink

# , GV_data] >> deduplicate >> filter_1 >> core_tables  # >> end
# KYM_data >> GV_tables

# KYM_data >> deduplicate

# [deduplicate >> filter_1 >> core_tables >> [
#     date_dim, status_dim, origin_dim]]
