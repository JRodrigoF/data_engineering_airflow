import datetime
import pandas as pd
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

DAGS_FOLDER = '/opt/airflow/dags/'
SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'
TEMPLATE_FOLDER = DAGS_FOLDER + 'data/'
SQL_FOLDER = DAGS_FOLDER + 'sql/'

POSTGRES_CONN_ID = "postgres_default"

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2020, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

pipeline_1 = DAG(
    dag_id='pipeline_1',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=DAGS_FOLDER,
)

source = DummyOperator(
    task_id='source',
    dag=pipeline_1,
    trigger_rule='none_failed'
)

KYM_data = DummyOperator(
    task_id='KYM_data',
    dag=pipeline_1,
    trigger_rule='none_failed'
)

GV_data = DummyOperator(
    task_id='Google_Vision_data',
    dag=pipeline_1,
    trigger_rule='none_failed'
)

#TODO
deduplicate = BashOperator(
    task_id='deduplicate',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/remove_duplicates_kym.py --file /opt/airflow/dags/data/kym.json.gz --out /opt/airflow/dags/data/kym_unique.json",
    # bash_command="python {{ DAGS_FOLDER }}scripts/remove_duplicates_kym.py --file data/kym.json --out data/kym_unique.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

filter_1 = BashOperator(
    task_id='filter_1',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/prepare_to_tsv_kym.py --file /opt/airflow/dags/data/kym_unique.json --out /opt/airflow/dags/data/kym_unique_filter_1.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

core_tables = BashOperator(
    task_id='KYM_core_tables',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/data_to_tables.py --file /opt/airflow/dags/data/kym_unique_filter_1.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

date_dim = BashOperator(
    task_id='date_dim_tsv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/dim_dates.py --file /opt/airflow/dags/data/memes.tsv --out /opt/airflow/dags/data/dim_dates.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

status_dim = BashOperator(
    task_id='status_dim_tsv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/dim_status.py --file /opt/airflow/dags/data/memes.tsv --out /opt/airflow/dags/data/dim_status.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

origin_dim = BashOperator(
    task_id='origin_dim_tsv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/dim_origin.py --file /opt/airflow/dags/data/memes.tsv --out /opt/airflow/dags/data/dim_origins.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

GV_tables = BashOperator(
    task_id='GV_core_tables',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/GV_data_to_table.py --file /opt/airflow/dags/data/kym_vision.json.gz --out /opt/airflow/dags/data/meme_safeness_relations.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

safeness_dim = BashOperator(
    task_id='safeness_dim_tsv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/dim_safeness_gv.py --file /opt/airflow/dags/data/meme_safeness_relations.tsv --out /opt/airflow/dags/data/dim_safeness_gv.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

Memes_Fact_Table_tsv = BashOperator(
    task_id='Memes_Fact_Table_tsv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/memes_fact_table_maker.py --folder /opt/airflow/dags/data/ --out /opt/airflow/dags/data/fact_table_memes.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

db_init = PostgresOperator(
    task_id='db_init',
    dag=pipeline_1,
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
    dag=pipeline_1,
    postgres_conn_id='postgres_default',
    sql='sql/create_tables.sql',
    trigger_rule='all_success',
    autocommit=True,
)

def _postgres_populate_db(data_folder: str, postgres_conn_id: str):
    hook = PostgresHook.get_hook(postgres_conn_id)
    for table_name in ['dim_dates', 'dim_status', 'dim_origins', 'dim_safeness_gv', 'fact_table_memes']:
        df_temp = pd.read_csv(f'{data_folder}/{table_name}.tsv', sep="\t")
        df_temp.to_csv(f'{data_folder}/temp_{table_name}.tsv', sep="\t",
                        encoding='utf-8', na_rep='None', header=False, index=False)
        # hook.run(f"COPY {table_name} FROM '{data_folder}/temp_{table_name}.tsv' DELIMITER '\t' CSV HEADER")
        hook.bulk_load(table_name, f'{data_folder}/temp_{table_name}.tsv')
        os.remove(f'{data_folder}/temp_{table_name}.tsv')

populate_db = PythonOperator(
    task_id='populate_db',
    dag=pipeline_1,
    python_callable=_postgres_populate_db,
    op_kwargs={
        'data_folder': DATA_FOLDER,
        'postgres_conn_id': POSTGRES_CONN_ID
    },
    trigger_rule='all_success',
)

sink = DummyOperator(
    task_id='sink',
    dag=pipeline_1,
    trigger_rule='none_failed'
)

source >> KYM_data >> deduplicate >> filter_1 >> core_tables >> [
    date_dim, status_dim, origin_dim] >> Memes_Fact_Table_tsv >> db_init >> postgres_db >> populate_db >> sink

source >> GV_data >> GV_tables >> safeness_dim >> Memes_Fact_Table_tsv >> db_init >> postgres_db >> populate_db >> sink

# source >> db_init >> postgres_db >> populate_db >> sink

# , GV_data] >> deduplicate >> filter_1 >> core_tables  # >> end
# KYM_data >> GV_tables

# KYM_data >> deduplicate

# [deduplicate >> filter_1 >> core_tables >> [
#     date_dim, status_dim, origin_dim]]
