import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.postgres_operator import PostgresOperator

DAGS_FOLDER = '/opt/airflow/dags/'

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
    bash_command="python /opt/airflow/dags/scripts/dim_safeness.py --file /opt/airflow/dags/data/meme_safeness_relations.tsv --out /opt/airflow/dags/data/dim_safeness.tsv",
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

# start_schema = PostgresOperator(
#     task_id='start_schema',
#     dag=pipeline_1,
#     postgres_conn_id='postgres_default',
#     database = 'airflow',
#     # sql='/opt/airflow/dags/scripts/pipeline_1_inserts.sql',
#     sql='pipeline_1_inserts.sql',
#     trigger_rule='all_success',
#     autocommit=True,
# )

start_schema = PostgresOperator(
    task_id='start_schema',
    dag=pipeline_1,
    postgres_conn_id='postgres_default',
    # database = 'airflow',
    # sql='/opt/airflow/dags/scripts/pipeline_1_inserts.sql',
    # sql='sql/test.sql',
    sql=[
        'DROP DATABASE IF EXISTS memes3',
        'CREATE DATABASE memes3',
    ],
    # sql='pipeline_1_inserts.sql',
    trigger_rule='all_success',
    autocommit=True,
)
# DROP DATABASE IF EXISTS memes3

# CREATE DATABASE memes3

# tenth_node = PostgresOperator(
#     task_id='insert_inserts',
#     dag=assignment_dag,
#     postgres_conn_id='postgres_default',
#     sql='inserts.sql',
#     trigger_rule='all_success',
#     autocommit=True
# )

sink = DummyOperator(
    task_id='sink',
    dag=pipeline_1,
    trigger_rule='none_failed'
)

# source >> KYM_data >> deduplicate >> filter_1 >> core_tables >> [
#     date_dim, status_dim, origin_dim] >> Memes_Fact_Table_tsv >> start_schema >> sink

# source >> GV_data >> GV_tables >> safeness_dim >> Memes_Fact_Table_tsv >> start_schema >> sink

source >> start_schema >> sink

# , GV_data] >> deduplicate >> filter_1 >> core_tables  # >> end
# KYM_data >> GV_tables

# KYM_data >> deduplicate

# [deduplicate >> filter_1 >> core_tables >> [
#     date_dim, status_dim, origin_dim]]
