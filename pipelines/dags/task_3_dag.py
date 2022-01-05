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

TASK1_FOLDER = DAGS_FOLDER + 'output/task_1/'
TASK2_FOLDER = DAGS_FOLDER + 'output/task_2/'

OUTPUT_FOLDER = DAGS_FOLDER + 'output/task_3/'

POSTGRES_CONN_ID = "postgres_default"

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2021, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

pipeline = DAG(
    dag_id='task_3',
    default_args=default_args_dict,
    catchup=False,
)

source = DummyOperator(
    task_id='source',
    dag=pipeline,
    trigger_rule='none_failed'
)

create_dag_folder = BashOperator(
    task_id='create_dag_folder',
    dag=pipeline,
    bash_command=("mkdir -p  {OUTPUT_FOLDER}").format(OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='none_failed'
)

memes_dim_tsv = BashOperator(
    task_id='memes_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}make_memes_dim_tsv.py "
                + "--file {TASK1_FOLDER}kym_unique_filter_1.json "
                + "--outfile {OUTPUT_FOLDER}{epoch}_memes_dim.tsv ")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            TASK1_FOLDER=TASK1_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

status_dim_tsv = BashOperator(
    task_id='status_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}dim_status.py "
                + "--file {TASK1_FOLDER}memes.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_status.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            TASK1_FOLDER=TASK1_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

date_dim_tsv = BashOperator(
    task_id='date_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_dates.py "
                + "--file {TASK1_FOLDER}memes.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_dates.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            TASK1_FOLDER=TASK1_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

origin_dim_tsv = BashOperator(
    task_id='origin_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_origin.py "
                + "--file {TASK1_FOLDER}memes.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_origins.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            TASK1_FOLDER=TASK1_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)



KYM_fact_table_tsv = BashOperator(
    task_id='KYM_fact_table_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}fact_table_maker_augmented.py "
                + "--folder1 {TASK1_FOLDER} "
                + "--folder2 {TASK2_FOLDER} "
                + "--folder3 {OUTPUT_FOLDER} "
                + "--prefix {epoch} "
                + "--out {OUTPUT_FOLDER}{epoch}_fact_table_memes.tsv ")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            TASK1_FOLDER=TASK1_FOLDER,
            TASK2_FOLDER=TASK2_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

db_init = PostgresOperator(
    task_id='db_init',
    dag=pipeline,
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
    dag=pipeline,
    postgres_conn_id='postgres_default',
    # sql='{}create_tables.sql'.format(SCRIPTS_FOLDER), # does not find the file
    sql='sql/create_tables_augmented.sql',
    trigger_rule='all_success',
    autocommit=True,
)

def _postgres_populate_db(output_folder: str, task2_folder: str, postgres_conn_id: str, epoch: str):
    hook = PostgresHook.get_hook(postgres_conn_id)

    for table_name in ['dim_safeness_gv']:
        tmp_table = f'{output_folder}/temp_{epoch}_{table_name}.tsv'
        df_temp = pd.read_csv(
            f'{task2_folder}{table_name}.tsv', sep="\t")
        df_temp.to_csv(tmp_table, sep="\t", encoding='utf-8', na_rep='None',
                        header=False, index=False)
        # hook.run(f"COPY {table_name} FROM '{tmp_table}.tsv' DELIMITER '\t' CSV HEADER")
        hook.bulk_load(table_name, tmp_table)
        os.remove(tmp_table)

    tables = ['dim_dates', 'dim_status', 'dim_origins', 'fact_table_memes']
    for table_name in tables:
        tmp_table = f'{output_folder}/temp_{epoch}_{table_name}.tsv'
        df_temp = pd.read_csv(
            f'{output_folder}{epoch}_{table_name}.tsv', sep="\t")
        df_temp.to_csv(tmp_table, sep="\t", encoding='utf-8', na_rep='None',
                        header=False, index=False)
        # hook.run(f"COPY {table_name} FROM '{tmp_table}.tsv' DELIMITER '\t' CSV HEADER")
        hook.bulk_load(table_name, tmp_table)
        os.remove(tmp_table)

populate_db = PythonOperator(
    task_id='populate_db',
    dag=pipeline,
    python_callable=_postgres_populate_db,
    op_kwargs={
        'output_folder': OUTPUT_FOLDER,
        'task2_folder': TASK2_FOLDER,
        'postgres_conn_id': POSTGRES_CONN_ID,
        'epoch': "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
)


sink = DummyOperator(
    task_id='sink',
    dag=pipeline,
    trigger_rule='none_failed'
)

source >> create_dag_folder >> [date_dim_tsv, status_dim_tsv, origin_dim_tsv, memes_dim_tsv] >> KYM_fact_table_tsv >> db_init >> postgres_db >> populate_db >> sink
