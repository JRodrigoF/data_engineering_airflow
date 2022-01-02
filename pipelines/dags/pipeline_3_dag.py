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
OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipeline_3/'

POSTGRES_CONN_ID = "postgres_default"

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2021, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

pipeline_3 = DAG(
    dag_id='pipeline_3',
    default_args=default_args_dict,
    catchup=False,
)

source = DummyOperator(
    task_id='source',
    dag=pipeline_3,
    trigger_rule='none_failed'
)

KYM_data = DummyOperator(
    task_id='KYM_data',
    dag=pipeline_3,
    trigger_rule='none_failed'
)

remove_duplicates = BashOperator(
    task_id='remove_duplicates',
    dag=pipeline_3,
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
    dag=pipeline_3,
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
    dag=pipeline_3,
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
    dag=pipeline_3,
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
    dag=pipeline_3,
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
    dag=pipeline_3,
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
    dag=pipeline_3,
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
    dag=pipeline_3,
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
    dag=pipeline_3,
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
    dag=pipeline_3,
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
        hook.bulk_load(table_name, tmp_table)
        os.remove(tmp_table)

populate_db = PythonOperator(
    task_id='populate_db',
    dag=pipeline_3,
    python_callable=_postgres_populate_db,
    op_kwargs={
        'output_folder': OUTPUT_FOLDER,
        'postgres_conn_id': POSTGRES_CONN_ID,
        'epoch': "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
)

# SQL_queries = PostgresOperator(
#     task_id='SQL_queries',
#     dag=pipeline_3,
#     postgres_conn_id='postgres_default',
#     sql='sql/sql_queries.sql',
#     trigger_rule='all_success',
#     autocommit=True,
# )

# def _query_tables_to_tsv(output_folder: str, postgres_conn_id: str, tables: list):
#     hook = PostgresHook.get_hook(postgres_conn_id)
#     # output_file = output_folder +
#     # hook.run(f"COPY {table_name} FROM '{tmp_table}.tsv' DELIMITER '\t' CSV HEADER")
#     # sql_query = f"COPY (SELECT * FROM fact_table_memes) TO '{tmp_table}.tsv' DELIMITER '\t' CSV HEADER"
#     # sql_query = "COPY (SELECT * FROM fact_table_memes) TO '/opt/airflow/dags/output/pipeline_3/' DELIMITER ',' CSV HEADER"
#     for table in tables:
#         file = output_folder + table + '_from_query.tsv'
#         hook.bulk_dump(table, file)

# query_tables_to_tsv = PythonOperator(
#     task_id='query_tables_to_tsv',
#     dag=pipeline_3,
#     python_callable=_query_tables_to_tsv,
#     op_kwargs={
#         'output_folder': OUTPUT_FOLDER,
#         'postgres_conn_id': POSTGRES_CONN_ID,
#         'tables': ['test_table']
#     },
#     trigger_rule='all_success',
# )


def _queries_to_tables(dags_folder: str, output_folder: str,
    postgres_conn_id: str, query_names: list):

    # hook = PostgresHook.get_hook(postgres_conn_id)
    conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()

    sql_folder = '{}sql'.format(dags_folder)
    sql_files = sorted([f for f in os.listdir(sql_folder) if
        f.startswith('query') and f.endswith('.sql')])

    for sql_file in sql_files:
        file_path = '{}/{}'.format(sql_folder, sql_file)
        file_preffix = sql_file.replace('.sql', '')
        with open(file_path, 'r') as f:
            sql_query = f.read().replace('\'', '')

        df = pd.read_sql_query(sql_query, conn)
        out_file = output_folder + file_preffix + '.tsv'
        df.to_csv(out_file, sep="\t", encoding='utf-8', na_rep='None')

query_tables_to_tsv = PythonOperator(
    task_id='query_tables_to_tsv',
    dag=pipeline_3,
    python_callable=_queries_to_tables,
    op_kwargs={
        'dags_folder': DAGS_FOLDER,
        'output_folder': OUTPUT_FOLDER,
        'postgres_conn_id': POSTGRES_CONN_ID,
        'query_names': []},
    trigger_rule='all_success',
)

plots = DummyOperator(
    task_id='plots',
    dag=pipeline_3,
    trigger_rule='none_failed'
)

sink = DummyOperator(
    task_id='sink',
    dag=pipeline_3,
    trigger_rule='none_failed'
)

# source >> query_tables_to_tsv >> sink

source >> KYM_data >> remove_duplicates >> filtering >> KYM_data_to_tsv >> \
    [date_dim_tsv, status_dim_tsv, origin_dim_tsv, memes_dim_tsv] >> \
    KYM_fact_table_tsv >> db_init >> postgres_db >> populate_db >> \
    query_tables_to_tsv >> plots >> sink
